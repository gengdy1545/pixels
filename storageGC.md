# Pixels Storage Garbage Collection (Storage GC) 设计文档

## 1. 背景与目标 (Background & Objective)

Pixels 文件组织采用 Row Group (RG) -> Tile 的层级结构。删除信息存储在 Visibility 组件中（RGVisibility -> TileVisibility），以 Block 链表形式记录删除操作的时间戳（Timestamp）。

目前已实现 **Memory GC**（回收内存中过期的 Visibility Block）。本项目的目标是实现 **Storage GC**，回收高删除率的物理文件，通过重写数据、同步 Visibility 和更新索引，释放存储空间并优化读取性能。

### 1.1 现有基础设施 (Existing Infrastructure)

经设计与实现确认以下组件已就绪：

- **RGVisibility/TileVisibility**: 已实现 `getInvalidCount()` 方法（C++ 和 Java/JNI），可获取每个 RG 的精确无效行数用于加权计算
- **Storage GC 核心方法（C++/JNI/Java）**: `exportDeletionBlocks()` / `prependDeletionBlocks()` / `getBaseBitmap()` 已在 TileVisibility、RGVisibility、RGVisibilityJni 和 RGVisibility.java 各层实现
- **Memory GC**: `collectRGGarbage()` 已实现，在 `RetinaResourceManager.runGC()` 中定期执行，Storage GC 在 Memory GC 完成后作为第三阶段运行
- **RetinaResourceManager 双写逻辑**: `forwardMap`、`backwardMap`、`RedirectInfo`、`registerRedirection()`、`unregisterRedirection()`、双写版 `deleteRecord()` 已实现；另提供 `getRGVisibility()` 和 `createRGVisibility()` 供 Storage GC 使用
- **DeleteIndexBlock**: 链表结构，`BLOCK_CAPACITY = 8`，每个 Block 存储 8 个删除记录（rowId + timestamp）
- **VersionedData**: 包含 `baseBitmap`, `baseTimestamp`, `baseInvalidCount`, `head` 指针
- **MetadataService.AtomicSwapFiles**: `proto/metadata.proto`、`RdbFileDao`、`MetadataServiceImpl`、`MetadataService` 客户端已全链路实现
- **StorageGarbageCollector**: `StorageGarbageCollector.java` 在 `pixels-retina` 模块中实现并集成到 `runGC()`
- **RetinaUtils.extractRetinaNodeIdFromPath()**: 从右向左解析文件名获取 Retina Node ID 的工具方法已实现

---

## 2. 核心机制 (Core Mechanisms)

### 2.1 触发策略 (Trigger Strategy)

- **指标计算**：在 Memory GC 遍历 Visibility 链表时，统计**无效行数 (Invalid Row Count)**。
  - **实现位置**: `RGVisibility::getInvalidCount()` (C++) 及对应的 `RGVisibility.getInvalidCount()` (Java JNI)
  - **计算方式**：每个 RG 通过 `getInvalidCount()` 直接返回**无效行数**（baseBitmap 中 1 位的数量之和）。文件级无效率在 `StorageGarbageCollector` 中计算：`fileInvalidRatio = Σ(RG无效行数) / file.getNumRows()`
  - **注意**：统计时仅计算 `baseBitmap` (Safe Timestamp 对应的基准位图) 中的无效位，**不包含** Visibility 链表中尚未 Compact 的动态删除记录。
  - **触发时机**: 在 `RetinaResourceManager.runGC()` 执行 Memory GC 完成后，`StorageGarbageCollector.scanAndGroupFiles()` 调用 `calculateFileInvalidRatio()` 获取每个文件的精确无效率
- **阈值判定**：
  - **单文件判定**：若 `Σ(RG无效行数) / Σ(RG总行容量) > 阈值`（如 0.5），标记为待回收。（加权计算，比各 RG 无效率的算术平均更精确）
  - **合并判定**：将多个待回收文件组合，目标是重写后的新文件大小达到配置的**目标文件大小**。
  - **合并约束**：仅允许合并**同一张表 (Table)** 且 **同一 Retina 节点 (Retina Node)** 写入的文件。
    - **原因**：只有同一 Retina 节点的数据才能保证 Commit Timestamp 的严格单调递增性，跨节点合并可能破坏时序逻辑。
    - **识别**：通过解析文件名 `<Host>_<Timestamp>_<Counter>.pxl` 识别 Retina Node ID，采用**从右向左解析**以兼容 Hostname 中含下划线的情况（先取 Counter，再取 Timestamp，剩余为 Host）。

### 2.2 重写流程 (Rewrite Process)

Storage GC 执行分为三阶段：

1. **数据重写 (Data Rewrite)**：
   - **读取**: 使用 `PixelsReaderImpl` + `PhysicalReader` 读取旧文件
   - **过滤 [CRITICAL]**: **仅基于 Base Bitmap 过滤**，而非完整 Visibility Bitmap
     - **Base Bitmap**: Memory GC 已 compact 的删除记录，这些行可物理删除
     - **Deletion Chain**: Memory GC 尚未处理的删除记录，必须保留并迁移到新文件
     - **实现**: 调用 `RGVisibility.getBaseBitmap()` 获取 Base Bitmap，**跳过 bitmap 中为 1 的行**（0 表示有效、1 表示已删除可跳过）
     - **错误做法**: 调用 `queryVisibility(timestamp)` 会包含 Deletion Chain，导致删除信息丢失
   - **写入**: 使用 `PixelsWriterImpl` 创建新文件。参数来源：
     - `pixelStride`: 从旧文件 PostScript 读取
     - `rowGroupSize`: 由 `getRowGroupSizeFromConfig()` 从配置或旧文件读取（替换硬编码 128MB）
     - `encodingLevel`: 由 `getEncodingLevelFromConfig()` 从配置读取（替换硬编码 EL2）
     - `compressionKind`: 与旧文件一致
   - **新文件路径**: 由 `generateNewFilePath()` 从 metadata Path URI 动态获取，替换硬编码 `/tmp/pixels/`
   - **新 RG recordNum**: 由 `calculateActualRecordNumPerRG(newFilePath)` 从重写后的新文件 Footer 读取每 RG 实际行数，创建 RGVisibility 时传入，替换硬编码
   - **生成映射**: 重写过程中生成 **RowId Mapping** `int[] mapping`，`mapping[OldRowId] = NewRowId`；Base Bitmap 中已删除行 `mapping[OldRowId] = -1`，保留行 `mapping[OldRowId] = newRowCounter++`

2. **Visibility 同步 (Visibility Sync)**：将旧文件未被物理删除、但尚未在 Memory GC 中 Compact 的 Deletion Chain 迁移到新文件（见 3.3 节）。

3. **索引重建与切换 (Index Rebuild & Switch)**：更新主索引，在 Metadata 层完成原子切换；切换成功后由 `getRealFileIdFromMetadata()` 从 metadata 获取真实文件 ID，并调用 `updateFileIdInRGVisibility()`、`updateFileIdInRedirectInfo()` 更新 RGVisibility 与 RedirectInfo。

---

## 3. 关键算法与策略 (Key Algorithms)

### 3.1 双写策略 (Dual-Write Strategy)

为解决重写期间“漏读”或“追赶不上”的问题，采用双向同步：

1. **注册映射**: 数据重写及 Visibility 同步完成后，在 `RetinaResourceManager` 中注册 `(OldFileId, Mapping) <-> (NewFileId)`。多文件合并时，`backwardMap` 支持多个旧文件映射到同一新文件（多对一）。
2. **双写逻辑**（`RetinaResourceManager.deleteRecord`）：
   - **Old -> New (Forward)**: 针对 OldFile 的删除请求 → 查 Map 得 NewFileId 和 NewRowId → 若 NewRowId 有效则调用 `NewFile.deleteRecord`
   - **New -> Old (Backward)**: 针对 NewFile 的删除请求 → 查 Map 得 OldFileId 和 OldRowId → 调用 `OldFile.deleteRecord`，保证仍读旧文件的长尾查询能看到最新删除

### 3.2 Base Bitmap vs Deletion Chain [CRITICAL CONCEPT]

**Visibility 结构**：

```
Visibility = Base Bitmap + Deletion Chain
            ↓                    ↓
      Memory GC 已处理      Memory GC 未处理
      (可以物理删除)        (必须保留)
```

**VersionedData 结构**（来自 `TileVisibility.h`）：

```cpp
template<size_t CAPACITY>
struct VersionedData {
    uint64_t baseBitmap[NUM_WORDS]; // Memory GC compact 后的基准位图
    uint64_t baseTimestamp;         // baseBitmap 对应的 Safe GC Timestamp
    uint64_t baseInvalidCount;      // baseBitmap 中的无效行数（1 的个数）
    DeleteIndexBlock* head;         // Deletion Chain 头指针（未 compact 的删除）
};
```

**数据重写时**：仅用 `rgVisibility.getBaseBitmap()`，跳过 bitmap 中为 1 的行（已 compact 的删除）；保留 bitmap 为 0 的行（含 Deletion Chain 中的行）。不可使用 `queryVisibility(timestamp)`。

**Visibility 同步时**：`oldRGVisibility.exportDeletionBlocks()` 导出 Deletion Chain → Java 层用 RowId Mapping 转换 RowId → `newRGVisibility.prependDeletionBlocks()` 插入新文件头部。

**为何必须保留 Deletion Chain**：MVCC 语义（按 queryTimestamp 判断可见性）；Memory GC 只回收对所有活跃查询不可见的删除；双写期间新文件需有对应行才能执行 `deleteRecord(newRowId)`。

### 3.3 Visibility 同步 (Visibility Synchronization)

- **头部插入**: 旧数据 Timestamp 小于双写产生的新数据，流程为 `RGVisibility.exportDeletionBlocks` → Java 层 RowId (Old→New) 转换 → `NewRGVisibility.prependDeletionBlocks`（插入头部）。
- **Padding [CRITICAL]**: Memory GC (`collectTileGarbage`) 假设链表中除 Tail Block 外所有中间 Block 均为满的 (`BLOCK_CAPACITY`)。导出的最后一个 Block 可能未满，若直接链接到新文件 Head 会破坏该约束。在 `prependDeletionBlocks` 中，若最后一 Block 未满，必须用该 Block 最后一个元素重复填充至满；Bitwise OR 幂等，重复删除记录不会导致错误。

### 3.4 提交与原子切换 (Commit & Atomic Switch)

1. **预备阶段**：新文件生成、Visibility 同步、Index 更新完成。
2. **Metadata 原子切换**：
   - **接口**: `MetadataService.AtomicSwapFiles(filesToAdd, filesToDelete)`
   - **实现**: `RdbFileDao.atomicSwapFiles()`，JDBC 事务内先 `deleteByIds(filesToDelete)` 再 `insertBatch(filesToAdd)`，失败则回滚
3. **物理清理**：切换成功后注销 `RetinaResourceManager` 中的映射；旧文件可进入 Soft Delete，由后台异步清理。

---

## 4. 代码实现方案 (Implementation Plan)

### 4.1 模块：pixels-daemon (Metadata Service)

- **`proto/metadata.proto`**: 定义 `rpc AtomicSwapFiles`、`AtomicSwapFilesRequest`、`AtomicSwapFilesResponse`
- **`FileDao.java` / `RdbFileDao.java`**: `atomicSwapFiles(List<MetadataProto.File>, List<Long>)`，JDBC 事务保证原子性，复用 `insertBatch`、`deleteByIds`
- **`MetadataServiceImpl.java`**: 对应 gRPC 服务实现
- **`MetadataService.java`（客户端）**: `atomicSwapFiles(Collection<File> filesToAdd, List<Long> fileIdsToDelete)`

### 4.2 模块：pixels-retina (C++ Native Layer)

- **`TileVisibility.h/cpp`**: `exportDeletionBlocks()`（带 EpochGuard）、`prependDeletionBlocks(const uint64_t* items, size_t count)`（含 Padding + CAS）、`getBaseBitmap()`（返回 `currentVersion->baseBitmap`，带 EpochGuard）
- **`RGVisibility.h/cpp`**: `exportDeletionBlocks()`（local→global rowId）、`prependDeletionBlocks()`（global→local 分发）、`getBaseBitmap()`（拼接各 Tile baseBitmap）、`getInvalidCount()`（各 Tile 无效行数之和）
- **`RGVisibilityJni.h/cpp`**: 上述方法的 JNI 绑定（含 `getInvalidCount`）

### 4.3 模块：pixels-retina (Java/JNI)

- **`RGVisibility.java`**: JNI 为 `private native`，通过公开方法暴露：`exportDeletionBlocks()`、`prependDeletionBlocks(long[] items)`、`getBaseBitmap()`、`getInvalidCount()`
- **`RetinaResourceManager.java`**: `forwardMap`、`backwardMap`、`RedirectInfo`、双写版 `deleteRecord()`、`registerRedirection()`（含 reverseMapping 计算）、`unregisterRedirection()`；Storage GC 用：`getRGVisibility()`、`createRGVisibility()`

### 4.4 组件：StorageGarbageCollector (Java)

- **位置**: `pixels-retina/src/main/java/io/pixelsdb/pixels/retina/StorageGarbageCollector.java`
- **集成**: 在 `RetinaResourceManager.runGC()` 末尾调用（Memory GC 之后）
- **核心流程**: Scan → Rewrite → Sync → Commit
- **关键方法**:
  - `generateNewFilePath()`: 从 metadata Path URI 获取新文件存储路径
  - `getRowGroupSizeFromConfig()` / `getEncodingLevelFromConfig()`: 从配置读取 RG 大小与编码级别
  - `calculateActualRecordNumPerRG(newFilePath)`: 从重写后的新文件 Footer 读取每 RG 实际行数
  - `getRealFileIdFromMetadata()` / `updateFileIdInRGVisibility()` / `updateFileIdInRedirectInfo()`: 原子切换后同步真实文件 ID
  - `calculateFileInvalidRatio()`: 文件无效率 = Σ(RG invalid count) / Σ(RG total row count)（加权）
  - `rewriteFileGroup()`: 重写与 backwardMap 注册（支持多对一）

### 4.5 配置参数 (Configuration Parameters)

```properties
# Storage GC 配置
storage.gc.enabled=true
storage.gc.threshold=0.5
storage.gc.target.file.size=134217728  # 128MB
storage.gc.interval=3600000  # 1 hour in ms
storage.gc.max.files.per.run=10
```

---

## 5. 实现细节与注意事项 (Implementation Details & Caveats)

### 5.1 Visibility 导出与插入

- **时序**: 导出的删除记录 timestamp 小于双写期间新删除，头部插入不破坏时序
- **Padding**: `collectTileGarbage()` 假设除 tail 外 Block 满（见 `TileVisibility.cpp` 相关逻辑），未满 Block 必须填充
- **幂等性**: 重复填充同一删除记录用 Bitmap OR，结果正确

### 5.2 双写与并发

- **注册时机**: 新文件写入完成且 Visibility 同步完成后才注册映射
- **查询隔离**: 双写期间旧查询读旧文件、新查询可读新文件，均能见到最新删除
- **切换原子性**: Metadata 原子切换保证只看到旧文件集或新文件集，无中间状态

### 5.3 失败与回滚

- **Rewrite 失败**: 删除新文件，保留旧文件
- **Sync 失败**: 删除新文件与映射，保留旧文件
- **Commit 失败**: 删除新文件与映射，保留旧文件，可下次 GC 重试
- **部分成功**: 需记录状态，避免重复处理

### 5.4 性能与监控

- **性能**: 批量处理、异步执行、优先高无效率文件、I/O 限流
- **监控**: 记录每次 GC 的文件数、回收空间、耗时；失败时详细日志与告警；追踪文件状态（待处理/处理中/已完成/失败）

### 5.5 风险与限制

- 双写期间每次删除需更新两个文件的 Visibility，有性能开销
- 大文件 RowId Mapping 内存占用
- 若启用 Range/Point Index，需同步更新索引

---

## 6. 代码验证参考 (Code Verification Reference)

- **TileVisibility.cpp**: Memory GC 仅回收满 Block（`count < BLOCK_CAPACITY` 则 break），Padding 必须实现
- **RetinaResourceManager.runGC()**: 顺序为 createCheckpoint → Memory GC（遍历 rgVisibilityMap 执行 garbageCollect）→ `new StorageGarbageCollector(...).runStorageGC()`
- **RdbFileDao**: `insertBatch`、`deleteByIds`、`atomicSwapFiles`（事务内先删后插）已实现
- **PixelsWriterImpl.Builder**: 支持 schema、pixelStride、rowGroupSize、storage、path、compressionKind、encodingLevel
- **RGVisibility.getBaseBitmap()**: C++/JNI/Java 全链路可用，数据重写仅依赖 Base Bitmap
- **metadata.proto**: `AddFiles`/`DeleteFiles`/`AtomicSwapFiles` 已定义并实现
