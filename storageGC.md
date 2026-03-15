# Pixels Storage GC 设计与实现文档

---

## 1. 背景与目标

Pixels 采用 **不可变列式文件 + Visibility 链** 的 MVCC 架构：写入追加、删除仅在内存中的 `RGVisibility` 打标记（`DeleteIndexBlock` 链表 + `baseBitmap`）。随着删除积累，文件中的"幽灵行"占比上升，既浪费存储，也拖累扫描性能。

Memory GC 负责回收内存中的过期 Visibility Block，但物理文件本身不缩减。**Storage GC** 的目标是：**重写高删除率的物理文件，回收存储空间，同时保证正在运行的查询不受影响**。

---

## 2. 核心概念

### 2.1 Visibility 数据结构

#### MVCC 时间轴与水位线

Retina 采用轻量化 MVCC 算法实现存算分离架构下的高效隔离，核心是两条水位线：

```
ts:  0 ───────── [lwm] ───────────────── [hwm] ──────────────▶
     |_reclaimable_| |____active queries____|  |___replaying___|
       all queries      snap_ts in [lwm,hwm)    not yet visible
          done
```

- **hwm（High Water Mark）**：最新已完成回放的事务 ts + 1。hwm 之前的事务均已回放完成，数据对查询可见；hwm 及之后是仍在回放中的事务，数据尚不可见。随事务回放不断向右推进。新的分析型查询到来时，系统分配 `hwm - 1` 作为其**快照时间戳（snap_ts）**。
- **lwm（Low Water Mark）**：当前所有活跃查询快照 ts 的最小值。lwm 之前的查询均已结束，对应的数据版本不再被任何查询引用，可以进行垃圾回收。由 `TransService.getSafeGcTimestamp()` 向 trans server 查询得到。随旧查询结束向右推进。
- **活跃查询窗口**：所有活跃查询的 snap_ts 均落在 `[lwm, hwm)` 区间内。

每个 tuple version 以**创建它的事务 ts** 为 create_ts，以**删除它的事务 ts** 为 delete_ts。一行数据对快照 snap_ts 可见，当且仅当 `create_ts ≤ snap_ts` 且（`delete_ts > snap_ts` 或无删除记录）。

#### TileVisibility 数据结构

为避免追踪海量历史版本的可见性消耗大量内存，系统将每个列存行组（RG）逻辑上划分为多个 **Tile**，每个 Tile 在内存中维护一个极度轻量级的可见性结构 `TileVisibility`：

```
TileVisibility<CAPACITY>
├── currentVersion (atomic ptr) → VersionedData   // COW 版本，GC 时原子替换
│     ├── baseBitmap[NUM_WORDS]   // 已 compact 的基准删除位图（bit=1 表示已删除）
│     ├── baseTimestamp           // baseBitmap 已 compact 到的最新删除记录 ts（≤ lwm）
│     └── head → DeleteIndexBlock → ... → null   // Deletion Chain（未 compact 的增量删除）
├── tail (atomic ptr)             // 链表尾部，用于无锁追加
└── tailUsed (atomic size_t)      // 尾 Block 已用槽数
```

**`baseTimestamp` 的精确语义**：不等于 lwm，而是 Deletion Chain 中被 Memory GC 清除的最后一个 block 的最后一条 item 的 delete_ts（即实际 compact 进 baseBitmap 的最新删除记录的 ts）。由于 Memory GC 以整 block 为单位 compact（只 compact block 内最后一个 item 的 ts ≤ lwm 的 block），`baseTimestamp` ≤ lwm，但不一定等于 lwm。

**Deletion Chain 的时序不变式**：同一 Retina Node 写入的文件，Commit Timestamp 单调递增，因此 Deletion Chain 中各 item 的 delete_ts 严格按追加顺序递增（从 head 到 tail 单调不减）。这一有序性是 Memory GC 按 block 批量 compact 和查询提前退出优化的前提。

**item 编码格式**（`uint64_t`，`TileVisibility.h`）：

- C++ 层内部存储：高 16 位 = Tile 内 `localRowId`（`0..CAPACITY-1`），低 48 位 = delete_ts
- `exportDeletionBlocks` 导出时须将 `localRowId` **还原为 RG 内全局行偏移**（`tileId * CAPACITY + localRowId`），以便 Java 层直接以全局行偏移索引 `mapping[]` 数组

item 编码**固定使用高 16 位存储 `localRowId`**，这一设计决定了 **`CAPACITY` 必须 ≤ 65535**（`uint16_t` 最大值），否则 `localRowId` 会溢出。`CAPACITY` 通过 `pixels.properties` 中的 `retina.tile.visibility.capacity` 配置，在 Maven 编译时以 `-DRETINA_CAPACITY` cmake 参数传入，作为 C++ 模板参数在编译期固定（见 §7.6）。运行时不可动态调整；修改配置后须重新编译整个项目方能生效。

编码/解码函数（`TileVisibility.h`，用于 C++ 内部）：

```cpp
inline uint64_t makeDeleteIndex(uint16_t rowId, uint64_t ts) {
    return (static_cast<uint64_t>(rowId) << 48 | (ts & 0x0000FFFFFFFFFFFFULL));
}
inline uint16_t extractRowId(uint64_t raw)    { return static_cast<uint16_t>(raw >> 48); }
inline uint64_t extractTimestamp(uint64_t raw) { return raw & 0x0000FFFFFFFFFFFFULL; }
```

#### Memory GC（`collectTileGarbage`）

Memory GC 由后台 `gcExecutor` 线程定期触发，以当前 lwm（`getSafeGcTimestamp()` 返回值）为参数，对所有 `TileVisibility` 执行 `collectTileGarbage(lwm)`：

1. 从 Deletion Chain 的 `head` 向 `tail` 遍历，找到满足条件的最后一个 block（`lastFullBlk`）：该 block 内最后一个 item 的 delete_ts ≤ lwm（利用时序不变式，只需检查每个 block 的最后一项）。
2. 以 COW 方式创建新的 `VersionedData`：新 `baseBitmap` = 旧 `baseBitmap` OR 上从 head 到 `lastFullBlk` 所有 item 中 delete_ts ≤ lwm 的 bit；新 `baseTimestamp` = `lastFullBlk` 最后一个已用 slot 的 delete_ts；新 `head` = `lastFullBlk->next`（剩余 Deletion Chain）。注意：实际代码中仍对每个 item 进行 `extractTimestamp(item) <= ts` 的二次过滤（见 `TileVisibility.cpp`），而非依赖 block 级约束直接 OR 全部 item——这在时序不变式成立时结果等价，但代码写法更为安全严谨。
3. 原子 CAS 将 `currentVersion` 替换为新版本，旧版本及其 block 链通过 **Epoch-based reclamation**（`EpochManager`）延迟安全释放，保证并发读者不会访问已释放内存。
4. 如此循环，随着 lwm 推进，Deletion Chain 头部的 block 不断被 compact 进 `baseBitmap` 并释放，内存持续收缩。

**Memory GC 触发时机**（`runGC()` 中的顺序）：
```
1. createCheckpoint(lwm, GC)   // 异步落盘：启动 checkpoint 写入，不等待完成即继续
2. collectRGGarbage(lwm)       // 清内存：compact Deletion Chain → baseBitmap
```
`createCheckpoint()` 返回 `CompletableFuture<Void>`，`runGC()` **不调用 `.get()` 等待其完成**，因此 checkpoint 磁盘写入与内存 GC（`garbageCollect`）实际是并发执行的。并发安全性由以下机制保证：

- `createCheckpoint` 通过 `getVisibilityBitmap(timestamp)` 对每个 RG 的可见性状态做快照（原子读取 `currentVersion`），快照时刻已确定；
- `garbageCollect` 通过 COW 原子 CAS 替换 `currentVersion`，不修改快照所引用的旧 `VersionedData`，两者读写的对象在内存层面不冲突；
- `latestGcTimestamp` 在 checkpoint **写盘成功后**才更新（异步回调中赋值），因此下次 `runGC()` 入口的 `timestamp <= latestGcTimestamp` 检查能阻止在同一 ts 下重复执行 GC，但不保证上一 checkpoint 完全落盘后才开始下一次 GC。

实际的"先落盘、再清内存"语义保证体现在：宕机重启时，`latestGcTimestamp` 从已成功落盘的最新 GC checkpoint 恢复，未成功落盘的 checkpoint 对应的内存 GC 结果不会被采信，因此 checkpoint 与内存状态始终保持一致性。

#### 查询可见性判断（`getTileVisibilityBitmap`）

分析型查询以 snap_ts = hwm - 1 调用 `getTileVisibilityBitmap(snap_ts)`，返回该 Tile 内所有**对 snap_ts 不可见（已被删除）**的行的位图：

1. **进入 Epoch 保护**（`EpochGuard`），防止并发 GC 释放正在读取的内存。
2. 原子读取 `currentVersion`，若 `snap_ts < baseTimestamp`，说明该查询的快照比 `baseBitmap` 还旧，需从磁盘 checkpoint 读取（抛出异常，由上层 offload 机制处理）。
3. memcpy `baseBitmap` 作为结果的初始值（baseTimestamp 之前的所有删除已在此中）。
4. 从 `head` 遍历 Deletion Chain，对每条 item：
   - 若 `delete_ts ≤ snap_ts`：将该 rowId 对应 bit 置 1（此行对 snap_ts 已删除，不可见）。
   - 若 `delete_ts > snap_ts`：**立即 return**（利用时序不变式，后续所有 item 的 delete_ts 更大，无需继续扫描）。
5. 在 SIMD 编译模式下（`RETINA_SIMD`），步骤 4 使用 AVX2 指令每次处理 4 个 item（`_mm256_cmpgt_epi64`），向量化加速过滤。

整个过程通过原子操作与 Epoch 保护完成，与前台的删除写入和后台 Memory GC 无并发冲突。

#### 长查询 Offload 机制

当某个查询运行时间过长时，若不处理则 lwm 无法推进，Deletion Chain 持续积累导致内存 OOM。系统通过以下机制强制推进 lwm：

1. 检测到长查询时，调用 `registerOffload(snap_ts)`：以该查询的 snap_ts 为时间点，将当前所有 RG 的完整可见性状态（`getVisibilityBitmap(snap_ts)` 的结果）异步序列化落盘（Offload Checkpoint 文件）。
2. checkpoint 落盘完成后，调用 `TransService.markTransOffloaded(transId)`：通知 trans server 将该事务从 lwm 计算中排除，lwm 可以继续向右推进，后台 Memory GC 得以正常回收内存。
3. 长查询后续调用 `getTileVisibilityBitmap` 时，若 `snap_ts < baseTimestamp`（说明其快照已被 compact 掉），从 Offload Checkpoint 文件读取对应时间点的可见性数据。
4. 长查询结束后调用 `unregisterOffload(snap_ts)`，当引用计数归零时删除 Offload Checkpoint 文件。

**关键语义**：
- `baseBitmap` 中 bit=1 的行：delete_ts ≤ baseTimestamp（≤ lwm），所有活跃查询的 snap_ts ≥ lwm > baseTimestamp，因此这些行对所有当前及未来查询均永久不可见，**可物理跳过**。
- Deletion Chain 中的行：delete_ts > baseTimestamp，可能 ≤ 或 > 某个活跃查询的 snap_ts，**必须保留**，查询时按 snap_ts 动态过滤。
- `DeleteIndexBlock` 每块固定 8 个 slot（`BLOCK_CAPACITY = 8`），除 tail block 外，所有中间 block 必须是满的（不变式，`collectTileGarbage` 依赖此假设）。tail block 本身**也可以被合并**：`collectTileGarbage` 遍历时对 tail block 取 `tailUsed`（实际已用 slot 数）而非 `BLOCK_CAPACITY` 作为有效 count，只要 tail block 最后一个已用 slot 的 ts ≤ lwm，tail block 同样会被合并进 `baseBitmap`。`prependDeletionBlocks` 的"末尾不满则 padding"约束，是针对**新插入的 block 成为中间 block 后不变式不被破坏**的要求，与 tail block 能否被 GC 合并无关。

### 2.2 RGVisibility 分层结构

```
RGVisibility (Java JNI wrapper)
  └── C++: RGVisibility<CAPACITY>
        └── TileVisibility<CAPACITY>[]   // 按 Tile 分片，每 Tile 管理 CAPACITY 行
              ├── VersionedData { baseBitmap[NUM_WORDS], baseTimestamp, head→DeleteIndexBlock }
              └── DeleteIndexBlock { items[8], next (atomic ptr) }
```

`rgVisibilityMap` 的 key 格式：`"<fileId>_<rgId>"`（`RetinaResourceManager` 中的 `ConcurrentHashMap<String, RGVisibility>`）。

### 2.3 文件命名与合并约束

#### 文件名格式

`FileWriterManager` 与 `IndexedPixelsConsumer` 统一采用如下文件名格式：

```
<Host>_<Timestamp>_<VirtualNodeId>.pxl
```

```java
// FileWriterManager.java
String targetFileName = hostName + "_" + DateUtil.getCurTime() + "_" + virtualNodeId + ".pxl";

// IndexedPixelsConsumer.java（已有实现，格式相同）
String targetFileName = targetNode.getAddress() + "_" + DateUtil.getCurTime() + "_" + bucketId + ".pxl";
```

其中 `Host` 即 `retinaHostName`，可能本身含下划线；`VirtualNodeId` 为纯数字，无歧义。解析时从右向左连取两个 `_` 分隔段即可精确还原三个字段：最右段为 `virtualNodeId`，中间段为 `timestamp`，剩余前缀为 `hostName`。`RetinaUtils` 中的解析工具方法须实现为：

```java
public static String extractRetinaHostNameFromPath(String path) {
    String baseName = path.contains("/") ? path.substring(path.lastIndexOf('/') + 1) : path;
    String withoutExt = baseName.endsWith(".pxl") ? baseName.substring(0, baseName.length() - 4) : baseName;
    // 最右段是 virtualNodeId，去掉后再取一次 lastIndexOf 得到 timestamp 与 hostName 的分隔点
    int vnodeSep = withoutExt.lastIndexOf('_');
    if (vnodeSep <= 0) return null;
    String withoutVnode = withoutExt.substring(0, vnodeSep);
    int timestampSep = withoutVnode.lastIndexOf('_');
    if (timestampSep <= 0) return null;
    return withoutVnode.substring(0, timestampSep);
}

public static int extractVirtualNodeIdFromPath(String path) {
    String baseName = path.contains("/") ? path.substring(path.lastIndexOf('/') + 1) : path;
    String withoutExt = baseName.endsWith(".pxl") ? baseName.substring(0, baseName.length() - 4) : baseName;
    int vnodeSep = withoutExt.lastIndexOf('_');
    if (vnodeSep < 0) return -1;
    return Integer.parseInt(withoutExt.substring(vnodeSep + 1));
}
```

将 `virtualNodeId` 编码进文件名还解决了一个潜在的正确性问题：同一 Retina Node 上若两个不同 `virtualNodeId` 的 `PixelsWriteBuffer` 在同一毫秒触发 `switchMemTable()`，旧格式 `<hostName>_<timestamp>.pxl` 会产生文件名冲突，新格式因末尾 `_<virtualNodeId>` 不同而天然区分。

#### 合并约束：同一 Write Buffer 写出的文件才能合并

Storage GC 合并文件时，必须确保合并后的文件内各行的 Commit Timestamp 仍然**单调递增**，否则会破坏 Deletion Chain 的时序不变式，导致 Memory GC 的 batch compact 逻辑和查询的提前退出优化均失效（见 §2.1）。

**时间戳单调性的保证来源于 `PixelsWriteBuffer` 的单流写入结构：**

`RetinaResourceManager` 为每个 `(table, virtualNodeId)` 创建一个独立的 `PixelsWriteBuffer` 实例。在该实例内部：

- `addRow()` 通过 `rowLock` 串行写入 `activeMemTable`，CDC 事务的 timestamp 按提交顺序依次追加；
- 任意时刻只有一条活跃写流 `currentFileWriterManager`，MemTable 满后触发 `switchMemTable()`，将当前 `FileWriterManager` 入队并新建下一个，两个 FileWriter 之间不存在时间交叉；
- 同一 `FileWriterManager` 在 `finish()` 中将分配给它的连续若干个 MemTable **按 blockId 顺序**从对象存储读回并写入同一个 `.pxl` 文件。

由此形成完整的有序传递链：

```
CDC 事务 (timestamp↑)
  → rowLock 串行写入 activeMemTable
    → MemTable 满 → FileWriterManager（连续 MemTable 按 blockId 顺序写入同一 .pxl）
      → switchMemTable() → 下一个 FileWriterManager（下一个 .pxl，起始 ts > 上一文件结尾 ts）
```

因此，**同一 `PixelsWriteBuffer` 写出的所有 `.pxl` 文件，跨文件的行 Commit Timestamp 严格单调递增**。

不同 `PixelsWriteBuffer`（即不同 `virtualNodeId`，或来自不同 Retina Node）的文件之间，timestamp 序列无全序关系，若混合合并则 Deletion Chain 中的 `delete_ts` 不再单调。

**结论**：Storage GC 的文件分组（S1 扫描与分组）以 **`(retinaHostName, virtualNodeId)`**（即一个 `PixelsWriteBuffer` 的唯一标识）为单位，只有同一 write buffer 负责写出的文件才可合并。由于两个字段均已编码在文件名中，S1 扫描阶段直接从文件名解析即可完成精确分组，无需借助 catalog 或文件内嵌元信息，对未来同一 Node 上多个并发 `PixelsWriteBuffer` 的场景同样天然兼容。

### 2.4 File 元信息说明

Pixels 对文件的描述分布在**两个独立层次**，需结合使用：

#### Catalog 层：`metadata.proto` File message

由 MetadataService 持久化管理，字段如下：

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | uint64 | catalog 全局唯一文件 ID，GC 以此标记 `processingFiles` 防重入 |
| `name` | string | 文件名（含 `.pxl` 后缀，**不含路径**），与 `Path.uri` 拼接得完整路径 |
| `type` | enum | `TEMPORARY`（写入/GC 过程中）/ `REGULAR`（正式可用）；Storage GC S2 注册 `TEMPORARY`，S6 切换为 `REGULAR` |
| `numRowGroup` | uint32 | RG 数量，与文件内 `Footer.rowGroupInfos.size()` 保持一致。注意：`FileWriterManager` 注册文件时硬编码 `numRowGroup=1`，当前每个 Retina 写入文件固定只有一个 RG；Storage GC 重写后的新文件可能含多个 RG，`insertFile` 时须正确传入实际 RG 数量 |
| `minRowId` | uint64 | 该文件第一行的全局 RowId |
| `maxRowId` | uint64 | 该文件最后一行的全局 RowId，`[minRowId, maxRowId]` 是 Retina MVCC 的行版本区间锚点 |
| `pathId` | uint64 | 关联的 `Path` ID，间接连接到 Layout → Table |

**Catalog 层不含以下字段**，实现时须在扫描阶段从外部获取：
- `numRows`：从文件内部 `PostScript.numberOfRows` 读取，即 `PixelsReader.getNumberOfRows()`
- `tableId`：通过 `pathId` → `MetadataService.getPath(pathId)` → 关联 Layout 反查
- `retinaNodeId`（即 Hostname）：从 `File.getName()` 通过 `extractRetinaHostNameFromPath()` 解析
- `fileSize`：不在 catalog 中持久化，无需使用

#### 文件内部层：`pixels.proto` 自描述结构

每个 `.pxl` 文件是自描述的列式格式，文件尾部存储完整的内部元信息，物理布局如下：

```
┌──────────────────────────────────────┐
│  Data Region                         │
│  [RG0 Column Chunks][RG1 Col Chunks] │  ← PostScript.contentLength 涵盖此部分
├──────────────────────────────────────┤
│  RowGroupFooter[0]  (Index+Encoding) │
│  RowGroupFooter[1]  ...              │  ← 各 RG 自带列级索引与编码信息
├──────────────────────────────────────┤
│  Footer                              │  ← 文件级 schema + 统计 + RG 汇总
├──────────────────────────────────────┤
│  PostScript                          │  ← 文件级配置参数（含行数、版本等）
├──────────────────────────────────────┤
│  FileTail（8 字节偏移量）             │  ← 读取入口，固定在文件末尾
└──────────────────────────────────────┘
```

**PostScript**（文件级配置，`pixels.proto`）：

| 字段 | 说明 |
|---|---|
| `version` | Pixels 文件格式版本 |
| `contentLength` | 数据区字节数（不含 FileTail） |
| `numberOfRows` | 文件总行数，即 `PixelsReader.getNumberOfRows()` 的数据来源 |
| `pixelStride` | 每个 pixel 包含的行数（如 10000） |
| `partitioned` | 是否为 hash 分区文件，每个 RG 对应一个分区 |
| `columnChunkAlignment` | 列 chunk 起始偏移的对齐字节数 |
| `hasHiddenColumn` | 是否含隐藏时间戳列（Retina MVCC 写入的 `create_ts`/`delete_ts`），Storage GC 重写时必须同步处理 |
| `magic` | 恒为 `"PIXELS"`，用于格式校验 |

**Footer**（文件级统计，`pixels.proto`）：

| 字段 | 说明 |
|---|---|
| `types` | 列 schema（`Type` 列表，含类型、名称、精度等） |
| `columnStats` | 文件级列统计（min/max/count/nullFraction 等） |
| `rowGroupInfos` | 各 RG 的物理偏移与行数（`RowGroupInformation` 列表） |
| `rowGroupStats` | 各 RG 的列统计 |
| `hiddenType` / `hiddenColumnStats` | 隐藏时间戳列的类型与统计，仅在 `hasHiddenColumn=true` 时有效 |

**RowGroupInformation**（RG 物理布局，`pixels.proto`）：

| 字段 | 说明 |
|---|---|
| `footerOffset` | 该 RG 的 `RowGroupFooter` 起始偏移 |
| `dataLength` | 该 RG 数据区字节数 |
| `footerLength` | `RowGroupFooter` 序列化字节数 |
| `numberOfRows` | 该 RG 行数，GC 重写时用于构建 `rgMapping[]` |
| `partitionInfo` | hash 分区信息（可选），含分区键列 ID 和 hash 值 |

**RowGroupFooter → ColumnChunkIndex**（列级物理索引，`pixels.proto`）：

| 字段 | 说明 |
|---|---|
| `chunkOffset` | 该列 chunk 在文件中的绝对偏移 |
| `chunkLength` | 列 chunk 字节数（含 isNull bitmap） |
| `isNullOffset` | isNull bitmap 在 chunk 内的偏移 |
| `pixelPositions` | 各 pixel 在 chunk 内的起始偏移数组 |
| `pixelStatistics` | pixel 级列统计 |
| `littleEndian` / `nullsPadding` | 编码字节序与 null 填充标记 |

#### 两层元信息在 Storage GC 中的协作

```
S1 扫描阶段
  ├── Catalog 层（MetadataService.getFiles）
  │     注意：getFiles(pathId) 只接受 pathId 参数，且底层自动过滤 TEMPORARY 文件（
  │     RdbFileDao 中 FILE_TYPE <> 0），因此只返回 REGULAR 文件。
  │     枚举某张表的所有文件需三层查询链：
  │       getLatestLayout(tableId)
  │         → 遍历 Layout 下所有 ordered/compact Path
  │           → getFiles(path.getId())
  │     → id / name / numRowGroup / minRowId / maxRowId / pathId
  │     → numRows、tableId、retinaNodeId 需间接获取
  └── 文件内部层（PixelsReader.open）
        → PostScript.numberOfRows          ← numRows 来源
        → PostScript.hasHiddenColumn       ← 判断是否需处理隐藏列
        → Footer.rowGroupInfos[i].numberOfRows  ← 各 RG 行数，用于 rgMapping 构建

S2 数据重写
  └── 文件内部层
        → RowGroupFooter → ColumnChunkIndex  ← 精确定位各列 chunk 进行读写

S6 原子切换
  └── Catalog 层（MetadataService.UpdateFile / AddFiles / DeleteFiles）
        → 新文件 TEMPORARY → REGULAR
        → 更新 numRowGroup / minRowId / maxRowId
        → 删除旧 File 记录
```

### 2.5 `RETINA_CAPACITY` 编译期默认值与 baseBitmap 大小

`RETINA_CAPACITY` 在 `cpp/pixels-retina/include/RetinaBase.h` 中的默认值为 **256**（可通过 `pixels.properties` 中 `retina.tile.visibility.capacity` 配置，编译时以 `-DRETINA_CAPACITY` 注入 cmake）。

`CAPACITY` 在 Maven 编译时固定，运行时不可动态调整；修改配置后须重新编译整个项目方能生效。

### 2.6 `addVisibility()` 的幂等性

`RetinaResourceManager.addVisibility(long fileId, int rgId, int recordNum)` 是幂等的：若 `rgVisibilityMap` 中 key `"<fileId>_<rgId>"` 已存在，则直接返回，不覆盖已有对象。

```java
// RetinaResourceManager.java
public void addVisibility(long fileId, int rgId, int recordNum) {
    String rgKey = fileId + "_" + rgId;
    if (rgVisibilityMap.containsKey(rgKey)) {
        return;
    }
    rgVisibilityMap.put(rgKey, new RGVisibility(recordNum));
}
```

这一幂等性对 Storage GC 的 WAL 恢复重试至关重要：若进程在 `TEMPORARY_CREATED` 状态后崩溃重启，恢复时重新调用 `addVisibility` 不会导致双重注册或对象泄漏。

同理，`addVisibility(String filePath)` 重载（按文件路径批量注册所有 RG）也因内部调用同一幂等方法而保持幂等。

### 2.7 TEMPORARY 文件对查询不可见的保证机制

Storage GC 在 S2 阶段将新文件以 `TEMPORARY` 类型写入 catalog，这保证了新文件在 S6 切换前对所有分析查询不可见。这一不可见性**不依赖任何查询层过滤逻辑**，而是由 `MetadataService.getFiles()` 底层实现决定的：

`RdbFileDao.getAllByPathId()` 在 SQL 查询中包含条件 `FILE_TYPE <> 0`（`TEMPORARY = 0`，`REGULAR = 1`），即**数据库层直接过滤掉所有 TEMPORARY 文件**，查询引擎通过 `getFiles()` 枚举候选文件时天然看不到 TEMPORARY 文件。

这一行为与 `FileWriterManager` 的写入流程一致（正常写入文件也先标 `TEMPORARY`，`finish()` 后才改为 `REGULAR`），Storage GC 复用了同一语义。实现 `atomicSwapFiles` 时，将新文件 `TYPE` 改为 `REGULAR` 的那一刻即是新文件对查询可见的边界。

---

## 3. 整体流程

### 3.1 六步重写流程

```
步骤顺序：S1 → S2 → S3 → S4 → S5 → S6

S1. 扫描与分组
S2. 数据重写 + 注册 TEMPORARY + 初始化新文件 Visibility
S3. 注册双写（双写窗口开启）
S4. Visibility 同步（在双写保护下迁移旧文件 Deletion Chain）
S5. 更新主索引（可选，在双写保护下进行）
S6. 原子切换元信息 + 注销双写 + 清理旧文件
```

各步骤与论文原文的对应关系：

| 本文步骤 | 论文步骤 | 说明 |
|---|---|---|
| S1 扫描与分组 | — | 论文未单独列出 |
| S2 数据重写 + 注册 TEMPORARY | S1 Rewrite Tuples + S2 Build Location Map | mapping 在重写时同步生成 |
| S3 注册双写 | S3 Enable Dual-Delete | 双写开启后，所有新 delete 自动同步到新文件 |
| S4 Visibility 同步 | S4 Synchronize Visibility | 在双写保护下 export，export 快照之后的 delete 由双写覆盖，无需补偿同步 |
| S5 更新主索引（可选） | S5 Update Index | |
| S6 原子切换与清理 | S6 Complete and Clean | |

### 3.2 触发时机

Storage GC 在 `RetinaResourceManager.runGC()` 中，**Memory GC 完成后**作为第三阶段运行：

```
runGC()
  ├── 1. createCheckpoint(timestamp, GC)         // 持久化快照（等待异步写入完成后继续）
  ├── 2. Memory GC：遍历 rgVisibilityMap，
  │        执行 rgVisibility.garbageCollect(timestamp)
  └── 3. StorageGarbageCollector.runStorageGC()  // 仅在 storage.gc.enabled=true 时执行
```

Memory GC 必须先于 Storage GC 完成，以确保 `baseBitmap` 已 compact 到最新的 Safe GC Timestamp，Storage GC 据此过滤的"已删除行"才准确。

`latestGcTimestamp` 更新时机：必须在整个 `runGC()`（含 Storage GC）成功完成后才更新；若 Storage GC 失败回滚，则不推进，确保下次 GC 能重试。

---

## 4. 详细步骤

### S1. 扫描与分组（Scan & Group）

**无效率计算**：

```
fileInvalidRatio = Σ(rgVisibility.getInvalidCount()) / totalRows
```

- 加权计算（总无效行数 / 总行数），比各 RG 算术平均更精确。
- 仅基于 `baseBitmap`（Memory GC 已 compact 部分），**不包含** Deletion Chain 中的行——Deletion Chain 中的行尚未确认全局不可见，不能计入无效。
- `getInvalidCount()` 须新增为 `RGVisibility` 的 JNI 方法，统计 `baseBitmap` 中 bit=1 的数量（实现见 §7.1）。

**分组约束**：
- `fileInvalidRatio > threshold`（默认 0.5）才标记为候选，**严格大于**，等于阈值不触发。
- 仅允许**同一 Table + 同一 Retina Node** 的文件合并，通过文件名解析 Hostname 确定 Node。
- 贪心合并：按无效率降序排序，累积有效行数（= 总行数 × (1 - invalidRatio)）直到达到目标文件大小（`storage.gc.target.file.size`，默认 128MB），形成一个 `FileGroup`。
- 若单个文件的有效数据量已超过目标大小，该文件独立形成一个 `FileGroup`，不与其他文件合并。

**防重入**：使用 `ConcurrentHashMap<Long, Boolean> processingFiles` 记录处理中的文件 ID，`scanAndGroupFiles()` 使用 `putIfAbsent` 原子标记，GC 完成或失败后移除。

### S2. 数据重写（Data Rewrite）

**读取过滤原则**：

```
for each row in oldFile:
    baseBitmap bit == 1  →  物理跳过（已 compact 的删除，永久不可见）
    baseBitmap bit == 0  →  写入新文件（包含 Deletion Chain 中尚未 compact 的行）
```

不可使用 `queryVisibility(timestamp)` 进行过滤：该方法会将 Deletion Chain 中的行也标记为"不可见"并跳过，导致这些行在新文件中丢失，破坏 MVCC 语义。

**`baseBitmap` 的获取方式**：须新增 JNI 方法 `getBaseBitmap()` 直接导出 `currentVersion->baseBitmap` 的副本（实现见 §7.1）。调用 `getVisibilityBitmap(Long.MAX_VALUE)` 是错误的，会将 Deletion Chain 中所有删除也合并进来。

**RowId 映射生成**（与数据重写同步完成，RG 粒度）：

```java
// rgMapping[oldRgRowOffset] = newRgRowOffset，-1 表示已跳过
int[] rgMapping = new int[rgRecordNum];
int newRowCounter = 0;
for (int oldOffset = 0; oldOffset < rgRecordNum; oldOffset++) {
    if (isBaseBitmapBitSet(baseBitmap, oldOffset)) {
        rgMapping[oldOffset] = -1;
    } else {
        rgMapping[oldOffset] = newRowCounter++;
    }
}

private static boolean isBaseBitmapBitSet(long[] baseBitmap, int rowOffset) {
    return (baseBitmap[rowOffset / 64] & (1L << (rowOffset % 64))) != 0;
}
```

多文件合并时，每个旧文件每个 RG 独立维护 `rgMapping[]`，新 RowId 全文件全局连续递增（按文件顺序）。

同时生成 `forwardGlobalMapping`：

```
fwdMapping[oldRgRowOffset] = newGlobalRowOffset（= accumulatedNewRows + newRgRowOffset）
其中 accumulatedNewRows 是在此 RG 之前已输出的全局行数
```

**写入参数**：
- `pixelStride`：从旧文件 `PostScript.getPixelStride()` 读取。
- `rowGroupSize`（字节）：优先读配置 `row.group.size`；其次取旧文件 Footer 中各 RG `dataLength` 的最大值；默认 268435456（256MB）。
- `encodingLevel`：优先读配置 `encoding.level`；默认 EL2。
- `compressionKind`：与旧文件一致（`PostScript.getCompression()`）。
- `path`：`generateNewFilePath(FileGroup)` 从 metadata Path URI 动态生成，不硬编码路径。

**新文件路径生成**（`generateNewFilePath(FileGroup)`）：
1. 取 `FileGroup` 中第一个 `FileCandidate` 的 `path.getUri()`（目录 URI）。
2. 文件名格式：`<retinaNodeId>_<currentTimeMillis>_<gcCounter>.pxl`，`gcCounter` 为本次 `runStorageGC()` 调用内的递增计数器。
3. 若目录 URI 不以 `/` 结尾，追加 `/`。

**注册 TEMPORARY 并初始化 Visibility**：

数据写入物理文件后，立即将新文件以 `File.Type.TEMPORARY` 插入 catalog：

```java
long newFileId = metadataService.insertFile(newFile);  // 返回 catalog 分配的真实 fileId
```

新文件对查询不可见（查询只扫描 `REGULAR` 文件），但 fileId 已确定。立即基于该 fileId 为每个新 RG 创建 `RGVisibility` 并注册到 `rgVisibilityMap`：

```java
for (int rgId = 0; rgId < newFileRgCount; rgId++) {
    resourceManager.addVisibility(newFileId, rgId, newFileRgActualRecordNums[rgId]);
    // rgVisibilityMap["<newFileId>_<rgId>"] = new RGVisibility(recordNum)
}
```

此后所有针对新文件的操作均直接使用 `newFileId`，无需临时 ID 或后续 ID 替换。

### S3. 注册双写（Enable Dual-Write）

在 Visibility 同步之前**先注册双写**，使双写窗口覆盖 Visibility 同步和索引更新的全过程。export 期间旧文件上的任何新增 delete 会通过双写自动同步到新文件，无需任何补偿同步。

**注册内容**（在 `RetinaResourceManager` 中维护，需新增 §7.3 所述字段）：

```java
// forwardMap：旧文件上的删除请求 → forward 到新文件
Map<Long, ForwardInfo> forwardMap;   // key = oldFileId，读写均须持有 redirectionLock

// backwardMap：新文件上的删除请求 → backward 到所有旧文件（索引更新期间使用）
Map<Long, List<BackwardInfo>> backwardMap;  // key = newFileId

record ForwardInfo(
    long newFileId,
    Map<Integer, int[]> forwardRgMappings,  // oldRgId → int[] fwdMapping
    // fwdMapping[oldRgRowOffset] = newGlobalRowOffset
    int[] newFileRgRowStart                 // 新文件各 RG 的起始全局行号（length=rgCount+1，末尾为哨兵）
) {}

record BackwardInfo(
    long oldFileId,
    Map<Integer, int[]> backwardRgMappings, // newRgId → int[] bwdMapping
    // bwdMapping[newRgRowOffset] = oldGlobalRowOffset
    int[] oldFileRgRowStart                 // 旧文件各 RG 的起始全局行号
) {}
```

多文件合并时，`backwardMap.get(newFileId)` 返回包含所有旧文件 `BackwardInfo` 的列表。

**双写执行逻辑**（扩展 `RetinaResourceManager.deleteRecord()`）：

```java
public void deleteRecord(long fileId, int rgId, int rgRowOffset, long timestamp)
        throws RetinaException {
    // 1. 写入当前文件
    checkRGVisibility(fileId, rgId).deleteRecord(rgRowOffset, timestamp);

    redirectionLock.readLock().lock();
    try {
        // 2. Forward：旧文件删除 → 同步到新文件
        ForwardInfo fwd = forwardMap.get(fileId);
        if (fwd != null) {
            int[] fwdMapping = fwd.forwardRgMappings().get(rgId);
            if (fwdMapping != null && rgRowOffset < fwdMapping.length && fwdMapping[rgRowOffset] >= 0) {
                int newGlobalOffset = fwdMapping[rgRowOffset];
                int newRgId = rgIdForGlobalRowOffset(newGlobalOffset, fwd.newFileRgRowStart());
                int newRgRowOffset = newGlobalOffset - fwd.newFileRgRowStart()[newRgId];
                checkRGVisibility(fwd.newFileId(), newRgId).deleteRecord(newRgRowOffset, timestamp);
            }
        }

        // 3. Backward：新文件删除 → 同步到所有旧文件（索引切换期间）
        List<BackwardInfo> bwdList = backwardMap.get(fileId);
        if (bwdList != null) {
            for (BackwardInfo bwd : bwdList) {
                int[] bwdMapping = bwd.backwardRgMappings().get(rgId);
                if (bwdMapping != null && rgRowOffset < bwdMapping.length && bwdMapping[rgRowOffset] >= 0) {
                    int oldGlobalOffset = bwdMapping[rgRowOffset];
                    int oldRgId = rgIdForGlobalRowOffset(oldGlobalOffset, bwd.oldFileRgRowStart());
                    int oldRgOff = oldGlobalOffset - bwd.oldFileRgRowStart()[oldRgId];
                    checkRGVisibility(bwd.oldFileId(), oldRgId).deleteRecord(oldRgOff, timestamp);
                }
            }
        }
    } finally {
        redirectionLock.readLock().unlock();
    }
}

// 二分查找全局行偏移落在哪个 RG
// rgRowStart[i] = 第 i 个 RG 的起始行号，rgRowStart[rgCount] = totalRows（哨兵）
private static int rgIdForGlobalRowOffset(int globalOffset, int[] rgRowStart) {
    int lo = 0, hi = rgRowStart.length - 2;
    while (lo < hi) {
        int mid = (lo + hi + 1) >>> 1;
        if (rgRowStart[mid] <= globalOffset) lo = mid; else hi = mid - 1;
    }
    return lo;
}
```

注册/注销双写时须持有写锁：

```java
private final ReadWriteLock redirectionLock = new ReentrantReadWriteLock();

public void registerRedirection(long oldFileId, ForwardInfo fwd,
                                 long newFileId, List<BackwardInfo> bwds) {
    redirectionLock.writeLock().lock();
    try {
        forwardMap.put(oldFileId, fwd);
        backwardMap.computeIfAbsent(newFileId, k -> new ArrayList<>()).addAll(bwds);
    } finally {
        redirectionLock.writeLock().unlock();
    }
}

public void unregisterRedirection(long oldFileId, long newFileId) {
    redirectionLock.writeLock().lock();
    try {
        forwardMap.remove(oldFileId);
        backwardMap.remove(newFileId);
    } finally {
        redirectionLock.writeLock().unlock();
    }
}
```

### S4. Visibility 同步（Visibility Synchronization）

注册双写后，将旧文件各 RG 的 Deletion Chain 迁移到新文件对应 RG 中。**此时双写窗口已开启**，export 期间旧文件上的任何新增 delete 均会通过 forward 双写自动同步到新文件，不存在漏记风险。

**新增 JNI 方法**（实现见 §7.1）：

```java
// RGVisibility.java 新增
public long[] exportDeletionBlocks();
// 导出该 RG 所有 Tile 的 Deletion Chain items（从 head 到 tail 的原子快照）
// 返回 flat array，每个 item 编码：高16位=rgGlobalRowOffset，低48位=timestamp

public void prependDeletionBlocks(long[] items);
// 将 items 批量插入到该 RG Deletion Chain 的头部（head 之前）
// C++ 实现须保证插入后所有中间 block 满 8 个 slot（Padding 规则）
```

**同步流程**（对每个旧文件的每个 RG）：

```java
for (FileCandidate oldFile : group.files()) {
    for (int oldRgId = 0; oldRgId < oldFile.file().getNumRowGroup(); oldRgId++) {
        long[] items = resourceManager.exportDeletionBlocks(oldFile.fileId(), oldRgId);
        // items[i] 高16位已为 rgGlobalRowOffset（C++ export 时已还原）

        int[] rgMapping = result.perFileRgMappings().get(oldFile.fileId()).get(oldRgId);
        long[] converted = convertDeletionBlocks(items, rgMapping);

        // 通过 forwardGlobalMappings 确定该旧 RG 的行对应新文件的哪个 RG
        int newRgId = determineTargetNewRgId(oldFile.fileId(), oldRgId, result);
        resourceManager.prependDeletionBlocks(result.newFileId(), newRgId, converted);
    }
}
```

**`convertDeletionBlocks` 实现**：

```java
private long[] convertDeletionBlocks(long[] items, int[] rgMapping) {
    // rgMapping[oldRgRowOffset] = newRgRowOffset，-1 表示跳过
    List<Long> result = new ArrayList<>();
    for (long item : items) {
        int oldOffset = (int) (item >>> 48);
        long ts = item & 0x0000_FFFF_FFFF_FFFFL;
        if (oldOffset < rgMapping.length && rgMapping[oldOffset] >= 0) {
            result.add(((long) rgMapping[oldOffset] << 48) | ts);
        }
    }
    return result.stream().mapToLong(Long::longValue).toArray();
}
```

**Padding 规则**：

`collectTileGarbage()` 依赖不变式：除 tail block 外，所有中间 block 必须满 8 个 slot。`prependDeletionBlocks()` 将导入的 items 组装成若干 block 后插入到 head 之前，这些新 block 成为中间 block，因此：
- 若 items 数量不是 8 的倍数，最后一个不满的 block 用**最后一个有效 item 重复填充**至 8 个 slot。
- 重复填充同一删除记录，Bitmap OR 操作幂等，不引入语义错误。

**时序保证**：Memory GC 完成后，`exportDeletionBlocks()` 导出的所有记录 timestamp ≤ 旧文件 `baseTimestamp`（Safe GC Timestamp）。双写注册后新到的 delete 的 timestamp > `baseTimestamp`，通过 forward 追加到新文件 Deletion Chain 的 tail，时序有序，头部插入不破坏链表时序。

**正确性保证**：export 快照之前的历史 delete 通过 `prependDeletionBlocks` 迁移到新文件；export 快照之后的新 delete 通过 forward 双写写入新文件的 tail。两段记录以 S3 注册时刻为界，不重叠、不遗漏，共同构成新文件完整的 Deletion Chain。

**多文件合并时**：各旧文件的 Deletion Chain 分别转换后，按其行在新文件中的 RG 归属，依次 `prependDeletionBlocks` 到新文件对应 RG 的 Visibility 中。

### S5. 更新主索引（Update Index，可选）

若系统未启用 Primary Index，跳过本步骤。

若启用 Primary Index（Range/Point Index）：

1. S3 注册的 `backwardMap` 已就绪，针对新文件的删除请求会 backward 到旧文件，旧文件索引仍有效，可正确处理 delete。
2. 扫描新文件 tuple，批量插入 Primary Index（使用 GC 快照 timestamp）。索引条目不是原子批量插入的，插入期间若有查询命中新 fileId，backward 双写保证旧文件索引的正确性。
3. 索引更新完成后，旧文件的索引条目随旧文件删除（S6）一并清除（stale entries）。

### S6. 原子切换与清理（Atomic Switch & Cleanup）

**Metadata 原子切换**（须新增 `MetadataService.atomicSwapFiles`，底层见 §7.2）：

```java
metadataService.atomicSwapFiles(newFileId, List<Long> oldFileIds);
// 底层（RdbFileDao）：在同一 JDBC 事务内执行：
//   UPDATE FILES SET TYPE=REGULAR WHERE FILE_ID=newFileId
//   DELETE FROM FILES WHERE FILE_ID IN (oldFileIds)
```

切换成功后，新文件对查询可见，旧文件对查询不可见。按以下**严格顺序**执行清理：

1. **注销旧双写映射**（双写窗口结束）：`unregisterRedirection(oldFileId, newFileId)`，持写锁移除 `forwardMap` 和 `backwardMap` 中相关条目。
2. **注销旧 Visibility key**：`removeFileVisibility(oldFileId, rgCount)` 从 `rgVisibilityMap` 中移除 `"<oldFileId>_<rgId>"` 条目，返回被移除的 `RGVisibility` 对象列表。新文件 Visibility（`"<newFileId>_<rgId>"`）在 S2 完成时已注册，此处无需插入。
3. **延迟释放旧 RGVisibility**：若仍有长尾查询持有旧 `RGVisibility` 对象的引用，等引用计数归零后调用 `rgVisibility.close()` 释放 C++ native 内存。
4. **物理删除旧文件**：异步后台任务，等引用计数归零后删除，避免阻塞 GC 主流程。

**为什么切换时不存在并发窗口**：新文件在 S2 完成后以 `TEMPORARY` 类型注册到 catalog，获得真实 fileId，`rgVisibilityMap` 中的新 key 在那时就已就绪。Metadata 切换只是将文件类型从 `TEMPORARY` 改为 `REGULAR`，查询从切换后才能看到新文件，此时 Visibility 早已准备好。

**回滚时**（S2 之后任意阶段失败，S6 之前）：
1. 注销双写映射（若已注册）。
2. 从 `rgVisibilityMap` 移除新文件的 key（`removeFileVisibility(newFileId, rgCount)`，并 close）。
3. 从 catalog 删除 TEMPORARY 文件记录（`metadataService.deleteFile(newFileId)`）。
4. 从物理存储删除新文件。

---

## 5. 并发安全分析

### 5.1 双写窗口

双写窗口从"注册双写映射"（S3）开始，到"注销双写映射"（S6 步骤 1）结束。窗口期覆盖 S4 Visibility 同步和 S5 索引更新的全过程。窗口期内：

| 删除请求目标 | 处理方式 |
|---|---|
| 针对旧文件的删除 | 旧文件直接记录；同时 forward 到新文件（通过 `forwardRgMappings` 转换行号） |
| 针对新文件的删除 | 新文件直接记录；同时 backward 到所有旧文件（通过 `backwardRgMappings` 转换行号） |

**长尾查询保证**：S6 切换后，新查询只能看到新文件；切换前已开始且仍在读旧文件的查询，依赖旧文件的 Visibility（含 backward 同步过来的删除记录），能正确过滤。

### 5.2 `exportDeletionBlocks` 在双写保护下的正确性

`exportDeletionBlocks()` 的 C++ 实现须满足：
1. **原子读取 `tail` 指针**：export 开始时用 `memory_order_acquire` 读取一次 `tail` 快照，仅导出该快照之前的所有 block，`tail` 快照之后新追加的 items 不在导出范围内。
2. **不阻塞写入**：export 期间旧文件仍可接受 `deleteRecord`（写入到 tail 之后），这些新 delete 已被双写同步到新文件（forward），无需额外处理。
3. **内存屏障**：遍历 block 链表时每个 `next` 指针读取使用 `memory_order_acquire`，每个 item 读取使用 `memory_order_relaxed`（items 在写入后不再修改）。

export 快照之前的历史 delete 通过 `prependDeletionBlocks` 迁移到新文件；export 快照之后的新 delete 通过 forward 双写写入新文件的 tail。两段记录以 S3 注册时刻严格分界：双写在 S3 注册后才生效，export 导出的是注册前的历史，不存在同一条 delete 既被 export 又被双写的情况，**不重叠、不遗漏**。

### 5.3 `rgVisibilityMap` key 的安全性

新文件在 S2 完成后以 `TEMPORARY` 类型注册，获得真实 fileId，并立即向 `rgVisibilityMap` 插入 `"<newFileId>_<rgId>"` 条目。S6 Metadata 切换后，新查询用新 fileId 查 Visibility，此时 map 中已有对应条目，不存在窗口。切换后仅需移除旧 key，无需任何预插入机制。

### 5.4 并发安全汇总

| 并发场景 | 保证机制 |
|---|---|
| Memory GC 与 Storage GC 同时运行 | 两者均由单线程 `gcExecutor` 串行调度，不存在并发 |
| `deleteRecord` 与 `exportDeletionBlocks` 并发 | export 原子读取 `tail` 快照；export 快照之后的 delete 由双写（forward）保护 |
| 多个 FileGroup 并行处理 | `runStorageGC()` 串行处理各 FileGroup；`processingFiles` 防止同一文件重入 |
| `deleteRecord` 与 `registerRedirection`/`unregisterRedirection` 并发 | `redirectionLock` 读写锁：`deleteRecord` 持读锁，注册/注销持写锁 |
| Metadata 切换后新查询访问新文件 Visibility | 新 key 在 S2 时已注册，切换后直接可用 |

---

## 6. 失败处理与幂等性

### 6.1 失败场景分类

| 阶段 | 失败行为 | 恢复方式 |
|---|---|---|
| S1 扫描分组 | 无副作用 | 下次 GC 重试 |
| S2 数据重写（物理文件写入失败） | 新物理文件部分写入 | 删除新物理文件；下次 GC 重试 |
| S2 注册 TEMPORARY（catalog 写入失败） | 新文件未进 catalog | 删除新物理文件；下次 GC 重试 |
| S3 注册双写失败 | 双写未生效，新文件未暴露给任何查询 | 移除新 Visibility key 并 close；删除 TEMPORARY 记录和物理文件；下次 GC 重试 |
| S4 Visibility 同步失败 | 新文件 Deletion Chain 不完整 | 注销双写；移除新 Visibility key 并 close；删除 TEMPORARY 记录和物理文件；下次 GC 重试 |
| S5 索引更新失败 | 索引部分更新，旧文件索引仍有效 | 注销双写；移除新 Visibility key 并 close；删除 TEMPORARY 记录和物理文件；下次 GC 重试 |
| S6 `atomicSwapFiles` 失败 | catalog 未变更，新文件仍为 TEMPORARY | 注销双写；移除新 Visibility key 并 close；删除 TEMPORARY 记录和物理文件；下次 GC 重试 |
| S6 切换成功，后续清理失败 | 旧文件 Visibility key / 物理文件泄漏 | GcWal 记录 `COMMITTED` 状态；进程重启后异步清理 |

### 6.2 WAL（GcWal）

`GcWal` 记录每次 GC 任务的状态机，持久化到磁盘（复用 Retina checkpoint 目录，以独立文件存储）：

```
PENDING
  → REWRITING          // S2 物理文件写入中
  → TEMPORARY_CREATED  // S2 完成：TEMPORARY 已注册（持久化 newFileId），Visibility 已初始化
  → DUAL_WRITE_ENABLED // S3 完成，双写窗口开启
  → VISIBILITY_SYNCED  // S4 完成，Deletion Chain 已迁移
  → INDEX_UPDATING     // S5 索引更新中（可选，无索引时跳过）
  → COMMITTING         // S6 atomicSwapFiles 执行中
  → COMMITTED          // S6 切换成功
  → CLEANING           // 旧 key 注销、物理文件异步删除中
  → DONE               // 全部完成
```

进程崩溃重启后，从 WAL 恢复：

| 恢复时状态 | 恢复操作 |
|---|---|
| `PENDING` / `REWRITING` | 删除新物理文件（若存在）；重置 WAL |
| `TEMPORARY_CREATED` / `DUAL_WRITE_ENABLED` / `VISIBILITY_SYNCED` / `INDEX_UPDATING` | 注销双写（若已注册）；移除新 Visibility key 并 close；删除 catalog TEMPORARY 记录；删除物理文件；重置 WAL |
| `COMMITTING` | 检查 catalog 中新文件是否已为 `REGULAR`：若是，按 `COMMITTED` 处理；若否，按上行处理 |
| `COMMITTED` / `CLEANING` | 继续执行清理（注销旧 key、删除旧物理文件）；标记 `DONE` |

### 6.3 幂等性保证

- **`atomicSwapFiles`**：底层 `UPDATE + DELETE` 在事务中执行，若新文件已为 `REGULAR` 则 UPDATE 为空操作，若旧文件已删除则 DELETE 为空操作，整体幂等。
- **`prependDeletionBlocks` + Padding**：重复插入相同删除记录，Bitmap OR 操作幂等，不引入错误。
- **`convertDeletionBlocks`**：纯函数，幂等。

---

## 7. 新增代码实现说明

### 7.1 RGVisibility 新增 JNI 方法

**Java 层**（`pixels-retina/src/main/java/io/pixelsdb/pixels/retina/RGVisibility.java`）：

```java
public long getInvalidCount() {
    long handle = nativeHandle.get();
    if (handle == 0) throw new IllegalStateException("RGVisibility is closed");
    return getInvalidCount(handle);
}
private native long getInvalidCount(long nativeHandle);

public long[] getBaseBitmap() {
    long handle = nativeHandle.get();
    if (handle == 0) throw new IllegalStateException("RGVisibility is closed");
    return getBaseBitmap(handle);
}
private native long[] getBaseBitmap(long nativeHandle);

public long[] exportDeletionBlocks() {
    long handle = nativeHandle.get();
    if (handle == 0) throw new IllegalStateException("RGVisibility is closed");
    return exportDeletionBlocks(handle);
}
private native long[] exportDeletionBlocks(long nativeHandle);

public void prependDeletionBlocks(long[] items) {
    long handle = nativeHandle.get();
    if (handle == 0) throw new IllegalStateException("RGVisibility is closed");
    prependDeletionBlocks(items, handle);
}
private native void prependDeletionBlocks(long[] items, long nativeHandle);
```

**C++ 层**（`cpp/pixels-retina/include/RGVisibility.h` 声明，`cpp/pixels-retina/lib/RGVisibility.cpp` 实现）：

```cpp
// getInvalidCount：统计所有 TileVisibility baseBitmap 中 bit=1 的数量
template<size_t CAPACITY>
uint64_t RGVisibility<CAPACITY>::getInvalidCount() const {
    uint64_t count = 0;
    for (uint64_t i = 0; i < tileCount; i++) {
        count += tileVisibilities[i].getBaseBitmapSetBitCount();
    }
    return count;
}

// getBaseBitmap：返回当前版本 baseBitmap 的副本
template<size_t CAPACITY>
std::vector<uint64_t> RGVisibility<CAPACITY>::getBaseBitmap() const {
    VersionedData<CAPACITY>* ver = currentVersion.load(std::memory_order_acquire);
    return std::vector<uint64_t>(ver->baseBitmap,
                                  ver->baseBitmap + VersionedData<CAPACITY>::NUM_WORDS);
}

// exportDeletionBlocks：导出所有 Tile 的 Deletion Chain，localRowId 转换为 RG 内全局行偏移
template<size_t CAPACITY>
std::vector<uint64_t> RGVisibility<CAPACITY>::exportDeletionBlocks() const {
    std::vector<uint64_t> result;
    for (uint64_t tileId = 0; tileId < tileCount; tileId++) {
        auto items = tileVisibilities[tileId].exportItems();
        for (uint64_t item : items) {
            uint16_t localRowId = extractRowId(item);
            uint64_t ts = extractTimestamp(item);
            uint32_t rgGlobalOffset = static_cast<uint32_t>(tileId) * CAPACITY + localRowId;
            result.push_back(makeDeleteIndex(static_cast<uint16_t>(rgGlobalOffset), ts));
        }
    }
    return result;
}

// prependDeletionBlocks：将 items 批量头插，末尾不满的 block 用最后一个 item 重复填充
template<size_t CAPACITY>
void RGVisibility<CAPACITY>::prependDeletionBlocks(const uint64_t* items, size_t count) {
    // 按 BLOCK_CAPACITY(=8) 分组，最后一组不足时用最后一个 item 填充至 8 个 slot
    // 新 block 链通过 COW 更新 currentVersion，原子 CAS 替换，链接到原 head 之前
}
```

`TileVisibility` 须新增 `exportItems()` 和 `getBaseBitmapSetBitCount()` 方法（`cpp/pixels-retina/include/TileVisibility.h` 和 `cpp/pixels-retina/lib/TileVisibility.cpp`）：

```cpp
// exportItems：原子读取 tail 快照，导出 head 到 tail 快照之间的所有 items
std::vector<uint64_t> TileVisibility<CAPACITY>::exportItems() const {
    // 1. tail 快照：tailSnap = tail.load(memory_order_acquire)
    // 2. 从 currentVersion->head 遍历，每个 next 用 memory_order_acquire 读取
    // 3. 到达 tailSnap 后停止，读取其已使用的 slot（用 tailUsed 当前值）
    // 4. 返回收集到的所有 items
}

// getBaseBitmapSetBitCount：统计 baseBitmap 中 bit=1 的数量
uint64_t TileVisibility<CAPACITY>::getBaseBitmapSetBitCount() const {
    VersionedData<CAPACITY>* ver = currentVersion.load(std::memory_order_acquire);
    uint64_t count = 0;
    for (size_t i = 0; i < NUM_WORDS; i++) {
        count += __builtin_popcountll(ver->baseBitmap[i]);
    }
    return count;
}
```

**JNI 层**（`cpp/pixels-retina/lib/RGVisibilityJni.cpp`）：按现有 `deleteRecord`、`garbageCollect` 的模式新增 4 个 JNI 函数：`Java_io_pixelsdb_pixels_retina_RGVisibility_getInvalidCount`、`getBaseBitmap`、`exportDeletionBlocks`、`prependDeletionBlocks`。

### 7.2 `atomicSwapFiles` 实现

**MetadataService 新增接口**（`pixels-common/src/main/java/io/pixelsdb/pixels/common/metadata/MetadataService.java`）：

```java
/** 在同一事务内：将 newFileId 对应文件改为 REGULAR，并删除 oldFileIds 对应文件 */
boolean atomicSwapFiles(long newFileId, List<Long> oldFileIds);
```

**RdbFileDao 实现**（`pixels-daemon/src/main/java/io/pixelsdb/pixels/daemon/metadata/dao/impl/RdbFileDao.java`）：

```java
public boolean atomicSwapFiles(long newFileId, List<Long> oldFileIds) {
    Connection conn = db.getConnection();
    try {
        conn.setAutoCommit(false);
        try (PreparedStatement pst = conn.prepareStatement(
                "UPDATE FILES SET TYPE=? WHERE FILE_ID=?")) {
            pst.setInt(1, File.Type.REGULAR.ordinal());
            pst.setLong(2, newFileId);
            pst.executeUpdate();
        }
        String inClause = oldFileIds.stream().map(id -> "?").collect(Collectors.joining(","));
        try (PreparedStatement pst = conn.prepareStatement(
                "DELETE FROM FILES WHERE FILE_ID IN (" + inClause + ")")) {
            for (int i = 0; i < oldFileIds.size(); i++) pst.setLong(i + 1, oldFileIds.get(i));
            pst.executeUpdate();
        }
        conn.commit();
        return true;
    } catch (SQLException e) {
        try { conn.rollback(); } catch (SQLException ignored) {}
        log.error("atomicSwapFiles failed", e);
        return false;
    } finally {
        try { conn.setAutoCommit(true); } catch (SQLException ignored) {}
    }
}
```

### 7.3 RetinaResourceManager 新增字段与接口

在 `pixels-retina/src/main/java/io/pixelsdb/pixels/retina/RetinaResourceManager.java` 中新增：

```java
// 双写映射（读多写少，使用读写锁）
private final Map<Long, ForwardInfo> forwardMap = new HashMap<>();
private final Map<Long, List<BackwardInfo>> backwardMap = new HashMap<>();
private final ReadWriteLock redirectionLock = new ReentrantReadWriteLock();

// 新增方法：
public long[] getBaseBitmap(long fileId, int rgId) throws RetinaException;
public long getInvalidCount(long fileId, int rgId) throws RetinaException;
public long[] exportDeletionBlocks(long fileId, int rgId) throws RetinaException;
public void prependDeletionBlocks(long fileId, int rgId, long[] items) throws RetinaException;
public void registerRedirection(long oldFileId, ForwardInfo fwd,
                                 long newFileId, List<BackwardInfo> bwds);
public void unregisterRedirection(long oldFileId, long newFileId);
public List<RGVisibility> removeFileVisibility(long fileId, int rgCount);
```

以上方法均通过 `checkRGVisibility(fileId, rgId)` 获取 `RGVisibility` 对象后调用对应 JNI 方法。`deleteRecord` 修改见 S3 节。

### 7.4 StorageGarbageCollector 核心接口

新建文件：`pixels-retina/src/main/java/io/pixelsdb/pixels/retina/StorageGarbageCollector.java`

```java
public class StorageGarbageCollector {
    private final RetinaResourceManager resourceManager;
    private final MetadataService metadataService;
    private final GcWal gcWal;
    private final ConcurrentHashMap<Long, Boolean> processingFiles = new ConcurrentHashMap<>();
    private final AtomicInteger gcCounter = new AtomicInteger(0);

    public StorageGarbageCollector(RetinaResourceManager resourceManager,
                                   MetadataService metadataService, GcWal gcWal) { ... }

    public boolean runStorageGC();

    private List<FileGroup> scanAndGroupFiles() throws Exception;
    private RewriteResult rewriteFileGroup(FileGroup group) throws Exception;
    private String generateNewFilePath(FileGroup group);
    private void registerDualWrite(RewriteResult result);
    private void syncVisibility(RewriteResult result) throws Exception;
    private long[] convertDeletionBlocks(long[] items, int[] rgMapping);
    private void updateIndex(RewriteResult result) throws Exception;
    private void commitFileGroup(RewriteResult result) throws Exception;
    private void rollback(RewriteResult result);
    private static int rgIdForGlobalRowOffset(int globalOffset, int[] rgRowStart);

    record FileCandidate(
        io.pixelsdb.pixels.common.metadata.domain.File file,
        io.pixelsdb.pixels.common.metadata.domain.Path path,
        String filePath, long numRows, long fileId,
        String retinaNodeId, double invalidRatio
    ) {}

    record FileGroup(long tableId, String retinaNodeId, List<FileCandidate> files) {}

    record RewriteResult(
        FileGroup group,
        String newFilePath,
        long newFileId,
        int newFileRgCount,
        int[] newFileRgActualRecordNums,
        int[] newFileRgRowStart,                            // length = newFileRgCount+1，末尾哨兵
        Map<Long, int[]> oldFileRgRowStarts,               // fileId → int[] rgRowStart
        Map<Long, Map<Integer, int[]>> perFileRgMappings,  // fileId → rgId → rgMapping
        Map<Long, Map<Integer, int[]>> forwardGlobalMappings,  // fileId → rgId → fwdMapping
        Map<Integer, int[]> backwardGlobalMappings,            // newRgId → bwdMapping
        boolean success
    ) {}
}
```

### 7.5 GcWal 接口

新建文件：`pixels-retina/src/main/java/io/pixelsdb/pixels/retina/GcWal.java`

```java
public class GcWal {
    public enum State {
        PENDING, REWRITING, TEMPORARY_CREATED, DUAL_WRITE_ENABLED,
        VISIBILITY_SYNCED, INDEX_UPDATING, COMMITTING, COMMITTED, CLEANING, DONE
    }

    public long beginTask(List<Long> oldFileIds, String newFilePath);
    public void updateState(long taskId, State state);
    public void updateState(long taskId, State state, long newFileId);  // TEMPORARY_CREATED 时使用
    public void markDone(long taskId);
    public List<GcTask> recoverPendingTasks();

    record GcTask(long taskId, State state, List<Long> oldFileIds,
                  String newFilePath, long newFileId) {}
}
```

持久化方案：在 Retina checkpoint 目录下，每个 GC 任务写一个 `gc-wal-<taskId>.json` 文件，包含 `taskId`、`state`、`oldFileIds`、`newFilePath`、`newFileId`（`TEMPORARY_CREATED` 后写入）。

### 7.6 配置参数

在 `pixels.properties` 或等效配置文件中增加：

```properties
storage.gc.enabled=true
storage.gc.threshold=0.5                  # 严格大于此阈值才触发 GC
storage.gc.target.file.size=134217728     # 目标新文件大小（字节），默认 128MB
storage.gc.max.file.groups.per.run=10     # 单次 GC 最多处理的 FileGroup 数
row.group.size=268435456                  # 新文件单个 RG 大小上限（字节），默认 256MB
encoding.level=EL2                        # 新文件编码级别，默认 EL2
```

以下参数已在 `pixels-retina` 配置块中存在，与 Storage GC 密切相关，此处说明其约束：

```properties
# 每个 Tile Visibility 管理的行数，必须是 64 的倍数（baseBitmap 按 64 位 word 存储）
# item 编码固定使用高 16 位存储 localRowId，因此 CAPACITY 必须 ≤ 65535（uint16_t 最大值）
# 该参数在 Maven 编译时通过 -DRETINA_CAPACITY 传给 cmake，作为 C++ 模板参数编译期固定
# 修改后须重新编译整个项目方能生效，运行时不可动态调整
retina.tile.visibility.capacity=10240
```

---

## 8. 完整时序图

```
GC Thread (gcExecutor)   RetinaResourceManager        MetadataService        Storage
    |                           |                           |                   |
    |== Memory GC ===========>>|                           |                   |
    |  garbageCollect(safeTs) on all RGVisibility          |                   |
    |<< Memory GC Done ========|                           |                   |
    |                           |                           |                   |
    |-- S1: scanAndGroupFiles ->|                           |                   |
    |   getBaseBitmap + getInvalidCount for each RG        |                   |
    |   group by (tableId, retinaNodeId)                   |                   |
    |   processingFiles.putIfAbsent for each candidate     |                   |
    |                           |                           |                   |
    |-- S2: rewriteFileGroup ------------------------------------------>|      |
    |   for each oldFile: read rows, filter by baseBitmap              |      |
    |   write retained rows to newFile                                 |      |
    |   build perFileRgMappings + forwardGlobalMappings                |      |
    |      + backwardGlobalMappings + newFileRgRowStart                |      |
    |<-- newFilePath, rgMappings ----------------------------------------      |
    |                           |                           |                   |
    |-- insertFile(TEMPORARY) ------------------------------>|                 |
    |<-- newFileId ------------------------------------------|                 |
    |                           |                           |                   |
    |-- addVisibility(newFileId, rgId, recordNum) -------->|                   |
    |   (for each RG of new file)                          |                   |
    |   rgVisibilityMap["newFileId_rgId"] = new RGVis     |                   |
    |-- GcWal: TEMPORARY_CREATED ---|                       |                   |
    |                           |                           |                   |
    |-- S3: registerDualWrite -->|                          |                   |
    |   forwardMap[oldFileId] = ForwardInfo                |                   |
    |   backwardMap[newFileId] += BackwardInfo             |                   |
    |-- GcWal: DUAL_WRITE_ENABLED --|                       |                   |
    |                 [双写窗口开始]  |                       |                   |
    |                           |                           |                   |
    |-- S4: syncVisibility ----->|                           |                   |
    |   exportDeletionBlocks(oldFileId, oldRgId)           |                   |
    |   (export 快照之后的新 delete 已被 forward 双写保护)   |                   |
    |   convertDeletionBlocks(items, rgMapping)            |                   |
    |   prependDeletionBlocks(newFileId, newRgId, items)   |                   |
    |   (repeat for each old RG)                           |                   |
    |-- GcWal: VISIBILITY_SYNCED ---|                       |                   |
    |                           |                           |                   |
    |-- S5: updateIndex (optional) ----------------------->|                   |
    |-- GcWal: INDEX_UPDATING / skip ---|                   |                   |
    |                           |                           |                   |
    |-- GcWal: COMMITTING -------|                           |                   |
    |-- S6: atomicSwapFiles ------------------------------>|                   |
    |   UPDATE newFile: TEMPORARY→REGULAR                  |                   |
    |   DELETE oldFiles  (single JDBC transaction)         |                   |
    |<-- Swap OK ------------------------------------------|                   |
    |-- GcWal: COMMITTED --------|                           |                   |
    |                           |                           |                   |
    |-- unregisterRedirection(oldFileId, newFileId) --->|                   |
    |                 [双写窗口结束]  |                       |                   |
    |-- removeFileVisibility(oldFileIds, rgCount) ----->|                   |
    |   rgVisibilityMap.remove("oldFileId_rgId")           |                   |
    |   (RGVisibility.close() after refCount==0)           |                   |
    |-- processingFiles.remove for each old fileId         |                   |
    |-- GcWal: CLEANING ---------|                           |                   |
    |                           |                           |                   |
    |-- async: wait refCount==0 → physicalDelete(oldFiles) ---------->|        |
    |-- GcWal: DONE -------------|                           |                   |
```

---

## 9. 代码实现步骤

### Phase 1：C++ 层新增方法（前置依赖）

**涉及文件**：
- `cpp/pixels-retina/include/TileVisibility.h`
- `cpp/pixels-retina/lib/TileVisibility.cpp`
- `cpp/pixels-retina/include/RGVisibility.h`
- `cpp/pixels-retina/lib/RGVisibility.cpp`
- `cpp/pixels-retina/lib/RGVisibilityJni.cpp`

**实现内容**：

1. `TileVisibility::getBaseBitmapSetBitCount()`：统计 `currentVersion->baseBitmap` 中 bit=1 的数量，使用 `__builtin_popcountll` 逐 word 累加。
2. `TileVisibility::exportItems()`：原子读取 `tail` 快照（`memory_order_acquire`），从 `currentVersion->head` 遍历链表直到 `tail` 快照，收集所有 items，返回 `std::vector<uint64_t>`。遍历中每个 `next` 指针用 `memory_order_acquire` 读取，item 读取用 `memory_order_relaxed`。
3. `RGVisibility::getInvalidCount()`：遍历所有 `TileVisibility`，累加 `getBaseBitmapSetBitCount()`。
4. `RGVisibility::getBaseBitmap()`：原子读取 `currentVersion`，返回 `baseBitmap` 的 `std::vector<uint64_t>` 副本。
5. `RGVisibility::exportDeletionBlocks()`：遍历所有 Tile，调用 `exportItems()`，将每个 item 中的 `localRowId` 转换为 `rgGlobalOffset`（`tileId * CAPACITY + localRowId`），重新编码后放入结果数组。
6. `RGVisibility::prependDeletionBlocks(const uint64_t* items, size_t count)`：将 items 分组为 `DeleteIndexBlock`（每组 8 个），末尾不足 8 个时用最后一个 item 填充；新建 block 链以 COW 方式更新 `currentVersion`，原子 CAS 将新 block 链接到 `currentVersion->head` 之前。
7. 在 `RGVisibilityJni.cpp` 中新增 4 个 JNI 函数（`getInvalidCount`、`getBaseBitmap`、`exportDeletionBlocks`、`prependDeletionBlocks`），模仿现有 `deleteRecord` 和 `garbageCollect` 的 JNI 模式（获取 native pointer、强转、调用方法、Java long[] 与 `jlongArray` 互转）。

### Phase 2：Java JNI Binding 层与 Visibility 工具接口

**涉及文件**：
- `pixels-retina/src/main/java/io/pixelsdb/pixels/retina/RGVisibility.java`
- `pixels-retina/src/main/java/io/pixelsdb/pixels/retina/RetinaResourceManager.java`
- `pixels-common/src/main/java/io/pixelsdb/pixels/common/utils/RetinaUtils.java`

**实现内容**：

1. `RGVisibility.java`：新增 `getInvalidCount()`、`getBaseBitmap()`、`exportDeletionBlocks()`、`prependDeletionBlocks(long[])` 四个 public 方法及对应 `private native` 声明（见 §7.1）。
2. `RetinaResourceManager.java`：
   - 新增 `forwardMap`、`backwardMap`、`redirectionLock` 字段（见 §7.3）。
   - 新增 `getBaseBitmap`、`getInvalidCount`、`exportDeletionBlocks`、`prependDeletionBlocks` 委托方法。
   - 新增 `registerRedirection`、`unregisterRedirection`（持写锁操作 map）。
   - 新增 `removeFileVisibility`（遍历 rgId，`rgVisibilityMap.remove(key)`，返回被移除对象列表）。
   - 修改 `deleteRecord`：在写入当前文件后，持读锁检查 `forwardMap`/`backwardMap`，执行双写逻辑（含 `rgIdForGlobalRowOffset` 工具方法，见 S3 节）。
3. `RetinaUtils.java`：修改 `extractRetinaHostNameFromPath()` 为从右向左解析（见 §2.3）。

### Phase 3：Metadata 层支持

**涉及文件**：
- `pixels-common/src/main/java/io/pixelsdb/pixels/common/metadata/MetadataService.java`
- `pixels-daemon/src/main/java/io/pixelsdb/pixels/daemon/metadata/dao/impl/RdbFileDao.java`
- 若 MetadataService 通过 gRPC 暴露：更新 proto 定义及 server stub

**实现内容**：

1. `RdbFileDao.java`：新增 `atomicSwapFiles(long newFileId, List<Long> oldFileIds)` 方法（见 §7.2），在同一 JDBC 事务中执行 `UPDATE TYPE=REGULAR` + `DELETE oldFiles`。
2. `MetadataService.java`：新增 `atomicSwapFiles` 接口，调用 `RdbFileDao.atomicSwapFiles()`（若为 gRPC 服务，同步更新 proto 和 server stub）。

### Phase 4：GcWal 实现

**涉及文件**：
- `pixels-retina/src/main/java/io/pixelsdb/pixels/retina/GcWal.java`（新建）

**实现内容**：

实现 §7.5 定义的 `GcWal` 接口：

1. `beginTask`：生成 taskId（`System.currentTimeMillis()` + 自增计数器），写 PENDING 状态文件到 checkpoint 目录，文件名 `gc-wal-<taskId>.json`，内容包含 `taskId`、`state`、`oldFileIds`、`newFilePath`。
2. `updateState`：读取现有 WAL 文件，更新 state 字段后覆写（fsync 保证持久化）。`TEMPORARY_CREATED` 时同时写入 `newFileId`。
3. `markDone`：更新 state 为 `DONE` 后删除 WAL 文件。
4. `recoverPendingTasks`：扫描 checkpoint 目录所有 `gc-wal-*.json`，反序列化，返回 state 不为 `DONE` 的任务列表。

### Phase 5：StorageGarbageCollector 主体实现

**涉及文件**：
- `pixels-retina/src/main/java/io/pixelsdb/pixels/retina/StorageGarbageCollector.java`（新建）

**实现内容**：

按 §7.4 接口实现各方法：

1. `runStorageGC()`：读取配置（`storage.gc.enabled`、`threshold`、`max.file.groups.per.run`）；调用 `scanAndGroupFiles()`；对每个 `FileGroup` 按 S2→S3→S4→S5→S6 顺序执行，任意步骤失败则调用 `rollback()`；完成后从 `processingFiles` 移除相应 fileId。
2. `scanAndGroupFiles()`：
   - 通过 `MetadataService` 枚举所有表的 `REGULAR` 文件；
   - 为每个文件用 `PixelsReaderBuilder` 读取 Footer，获取 `numRows`、RG 行数分布；
   - 逐 RG 调用 `resourceManager.getBaseBitmap(fileId, rgId)` 和 `getInvalidCount(fileId, rgId)` 计算 `invalidRatio`；
   - 按 `(tableId, retinaNodeId)` 分组，贪心合并，返回 `List<FileGroup>`；
   - 对每个候选文件 `processingFiles.putIfAbsent` 标记（失败则跳过）。
3. `rewriteFileGroup()`：
   - 用 `PixelsReaderBuilder` 依次读各旧文件各 RG 数据列；
   - 逐行检查 baseBitmap，保留 bit=0 的行，写入 `PixelsWriterBuilder` 构建的新文件；
   - 同步生成 `perFileRgMappings`、`forwardGlobalMappings`、`backwardGlobalMappings`、`newFileRgRowStart`；
   - 写完后：`metadataService.insertFile(TEMPORARY)` 获取 `newFileId`，`resourceManager.addVisibility()` 初始化 Visibility；
   - `gcWal.updateState(taskId, TEMPORARY_CREATED, newFileId)`。
4. `registerDualWrite()`：构造 `ForwardInfo`（含 `forwardRgMappings`、`newFileRgRowStart`）和每个旧文件的 `BackwardInfo`（含 `backwardRgMappings`、`oldFileRgRowStart`），调用 `resourceManager.registerRedirection()`；`gcWal.updateState(DUAL_WRITE_ENABLED)`。
5. `syncVisibility()`：对每个旧文件每个 RG：`exportDeletionBlocks` → `convertDeletionBlocks` → `prependDeletionBlocks`（将 oldRgRowOffset 转为 newRgRowOffset）；`gcWal.updateState(VISIBILITY_SYNCED)`。
6. `updateIndex()`：仅在系统启用 Primary Index 时执行；扫描新文件 tuple，批量插入索引条目。
7. `commitFileGroup()`：`gcWal.updateState(COMMITTING)`；`metadataService.atomicSwapFiles(newFileId, oldFileIds)`；成功后 `gcWal.updateState(COMMITTED)`；`unregisterRedirection`；`removeFileVisibility(oldFileIds, ...)`；提交异步物理删除任务；`gcWal.updateState(CLEANING)`；物理删除完成后 `gcWal.markDone()`。
8. `rollback()`：`unregisterRedirection`（若已注册）；`removeFileVisibility(newFileId, rgCount)` 并 close；`metadataService.deleteFile(newFileId)`；物理删除新文件；从 `processingFiles` 移除旧文件 ID。

### Phase 6：集成与接入

**涉及文件**：
- `pixels-retina/src/main/java/io/pixelsdb/pixels/retina/RetinaResourceManager.java`

**实现内容**：

1. 在 `RetinaResourceManager` 构造函数中实例化 `GcWal` 和 `StorageGarbageCollector`（依赖注入 `this`、`metadataService`、`gcWal`）。
2. 修改 `runGC()` 方法，在 Memory GC 完成后调用 `storageGarbageCollector.runStorageGC()`（仅当配置 `storage.gc.enabled=true`）。
3. 在 `RetinaResourceManager` 启动时调用 `gcWal.recoverPendingTasks()`，对每个未完成任务按其 WAL state 执行对应恢复操作（见 §6.2 恢复表格）。
