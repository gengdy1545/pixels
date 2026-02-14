# Pixels Storage Garbage Collection (Storage GC) 设计文档

## 1. 背景与目标 (Background & Objective)
Pixels 文件组织采用 Row Group (RG) -> Tile 的层级结构。删除信息存储在 Visibility 组件中（RGVisibility -> TileVisibility），以 Block 链表形式记录删除操作的时间戳（Timestamp）。

目前已实现 **Memory GC**（回收内存中过期的 Visibility Block）。本项目的目标是实现 **Storage GC**，回收高删除率的物理文件，通过重写数据、同步 Visibility 和更新索引，释放存储空间并优化读取性能。

### 1.1 现有基础设施验证 (Existing Infrastructure Verification)
经代码分析确认以下组件已就绪：
- **RGVisibility/TileVisibility**: 已实现 `getInvalidRatio()` 方法，可统计 baseBitmap 中的无效行数
- **Memory GC**: `collectRGGarbage()` 已实现，在 `RetinaResourceManager.runGC()` 中定期执行
- **DeleteIndexBlock**: 链表结构，`BLOCK_CAPACITY = 8`，每个 Block 存储 8 个删除记录（rowId + timestamp）
- **VersionedData**: 包含 `baseBitmap`, `baseTimestamp`, `baseInvalidCount`, `head` 指针

## 2. 核心机制 (Core Mechanisms)

### 2.1 触发策略 (Trigger Strategy)
*   **指标计算**：在 Memory GC 遍历 Visibility 链表时，统计**无效行数 (Invalid Row Count)**。
    *   **实现位置**: `RGVisibility::getInvalidRatio()` (C++) 和 `RGVisibility.getInvalidRatio()` (Java JNI)
    *   **计算方式**: 遍历所有 TileVisibility，累加 `baseInvalidCount`（通过 `__builtin_popcountll` 统计 baseBitmap 中的 1 位）
    *   **注意**：统计时仅计算 `baseBitmap` (Safe Timestamp 对应的基准位图) 中的无效位。**不包含** Visibility 链表中尚未 Compact 的动态删除记录。
    *   **触发时机**: 在 `RetinaResourceManager.runGC()` 执行 Memory GC 后，调用 `getInvalidRatio()` 获取每个文件的无效率
*   **阈值判定**：
    *   **单文件判定**：若 `(文件无效行数 / 文件总行数) > 阈值` (如 0.5)，标记为待回收。
    *   **合并判定**：将多个待回收文件组合，目标是重写后的新文件大小达到配置的**目标文件大小**。
    *   **合并约束 (关键)**：仅允许合并**属于同一张表 (Table)** 且 **由同一 Retina 节点 (Retina Node) 写入** 的文件。
        *   **原因**：只有同一 Retina 节点的数据才能保证 Commit Timestamp 的严格单调递增性，跨节点合并可能破坏时序逻辑。
        *   **识别**：通过解析文件名 `<Host>_<Timestamp>_<Counter>.pxl` 识别 Retina Node ID。采用**从右向左解析**策略以兼容 Hostname 中包含下划线的情况（先提取 Counter，再提取 Timestamp，剩余为 Host）。
        *   **验证**: 从代码中找到的文件名示例：`20190102094644_0.compact_copy_20190103025917_0.pxl`，确认命名规则存在

### 2.2 重写流程 (Rewrite Process)
Storage GC 的执行分为三个主要阶段：
1.  **数据重写 (Data Rewrite)**：
    *   **读取**: 使用 `PixelsReaderImpl` + `PhysicalReader` 读取旧文件
    *   **过滤 [CRITICAL]**: **仅基于 Base Bitmap 过滤**，而非完整的 Visibility Bitmap
        - **Base Bitmap**: Memory GC 已经 compact 过的删除记录，这些行可以物理删除
        - **Deletion Chain**: Memory GC 尚未处理的删除记录，这些删除信息必须保留并迁移到新文件
        - **实现**: 调用 `RGVisibility.getBaseBitmap()` 获取 Base Bitmap，跳过 bitmap 中为 0 的行
        - **错误做法**: ❌ 调用 `queryVisibility(timestamp)` 会包含 Deletion Chain，导致删除信息丢失
    *   **写入**: 使用 `PixelsWriterImpl` 创建新文件，配置参数：
        - `pixelStride`: 从旧文件的 PostScript 中读取
        - `rowGroupSize`: 从配置或旧文件中读取
        - `encodingLevel`: 保持与旧文件一致
        - `compressionKind`: 保持与旧文件一致
    *   **生成映射**: 在重写过程中，生成 **RowId Mapping**: `int[] mapping`，其中 `mapping[OldRowId] = NewRowId`。
        - 对于 Base Bitmap 中被删除的行（bitmap[i] = 0），`mapping[OldRowId] = -1`
        - 对于 Base Bitmap 中保留的行（bitmap[i] = 1），`mapping[OldRowId] = newRowCounter++`
2.  **Visibility 同步 (Visibility Sync)**：处理重写期间及历史删除数据的同步。
3.  **索引重建与切换 (Index Rebuild & Switch)**：更新主索引，并最终在 Metadata 层完成原子切换。

---

## 3. 关键算法与策略 (Key Algorithms)

### 3.1 双写策略 (Dual-Write Strategy)
为解决重写期间“漏读”或“追赶不上”的问题，采用双向同步策略：
1.  **注册映射**: 数据重写完成后，在 `RetinaResourceManager` 中注册 `(OldFileId, Mapping) <-> (NewFileId)` 的双向关系。
2.  **开启双写**: `RetinaResourceManager.deleteRecord` 逻辑变更：
    *   **Old -> New** (Forward): 收到针对 OldFile 的删除请求 -> 查 Map 得到 NewFileId 和 NewRowId -> 若 NewRowId 有效（未被物理删除），则调用 `NewFile.deleteRecord`。
    *   **New -> Old** (Backward): 收到针对 NewFile 的删除请求 -> 查 Map 得到 OldFileId 和 OldRowId -> 调用 `OldFile.deleteRecord`。这确保了在切换期间，仍持有 OldFile 引用的长尾查询能看到最新的删除。

### 3.2 Base Bitmap vs Deletion Chain [CRITICAL CONCEPT]

**这是 Storage GC 最容易混淆的核心概念**，必须深刻理解：

#### 3.2.1 Visibility 数据结构
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
    uint64_t baseBitmap[NUM_WORDS]; // Memory GC compact 后的基准位图（数组，不是指针）
    uint64_t baseTimestamp;         // baseBitmap 对应的 Safe GC Timestamp
    uint64_t baseInvalidCount;      // baseBitmap 中的无效行数（1 位的数量）
    DeleteIndexBlock* head;         // Deletion Chain 的头指针（未 compact 的删除）
};
```

#### 3.2.2 具体示例

假设某个 RowGroup 有 10 行数据，经历以下操作：

**时间线**：
1. **T0**: 初始状态，所有行有效
   ```
   Base Bitmap:    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]  (0 = valid)
   Deletion Chain: []
   ```

2. **T1**: 删除 Row 2, Row 5
   ```
   Base Bitmap:    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]  (未变)
   Deletion Chain: [(2, ts=T1), (5, ts=T1)]
   ```

2. **T2**: Memory GC 执行（Safe Timestamp = T1.5）
   ```
   Base Bitmap:    [0, 0, 1, 0, 0, 1, 0, 0, 0, 0]  (compact 了 T1 的删除)
                          ↑           ↑
                       Row 2        Row 5
                    (1 = deleted)
   Deletion Chain: []  (已清空)
   ```
4. **T3**: 删除 Row 7, Row 9
   ```
   Base Bitmap:    [0, 0, 1, 0, 0, 1, 0, 0, 0, 0]  (未变)
   Deletion Chain: [(7, ts=T3), (9, ts=T3)]
   ```

5. **T4**: Storage GC 执行
   - **错误做法** ❌: 调用 `queryVisibility(T4)` 获取完整的 Visibility Bitmap
     ```
     Visibility Bitmap: [0, 0, 1, 0, 0, 1, 1, 0, 1, 0]
                                          ↑      ↑
                                       Row 7  Row 9 (来自 Deletion Chain)
     ```
     **问题**: 如果跳过 Row 7 和 Row 9，这两条删除记录会丢失！
   
   - **正确做法** ✅: 仅使用 `getBaseBitmap()` 获取 Base Bitmap
     ```
     Base Bitmap: [0, 0, 1, 0, 0, 1, 0, 0, 0, 0]
     ```
     **结果**: 
     - 跳过 Row 2, Row 5（可以物理删除，bitmap = 1）
     - **保留** Row 7, Row 9（虽然在 Deletion Chain 中被标记删除，但 bitmap = 0，必须写入新文件）
     - 然后将 Deletion Chain `[(7, ts=T3), (9, ts=T3)]` 迁移到新文件

#### 3.2.3 为什么必须保留 Deletion Chain？

**原因 1: MVCC 语义**
- Deletion Chain 中的删除记录有 timestamp
- 查询时需要根据 `queryTimestamp` 判断可见性
- 如果物理删除这些行，早于 T3 的查询会看到错误的结果

**原因 2: Memory GC 的职责边界**
- Memory GC 只负责回收**已经对所有活跃查询不可见**的删除记录
- Safe GC Timestamp 之后的删除记录仍然可能被某些查询看到
- Storage GC 不能越权删除这些记录

**原因 3: 双写期间的一致性**
- 如果物理删除了 Deletion Chain 中的行，双写期间的删除操作无法正确同步
- 新文件中必须有对应的行才能执行 `deleteRecord(newRowId)`

#### 3.2.4 实现要点

**数据重写时**：
- ✅ 正确：调用 `rgVisibility.getBaseBitmap()` 获取 Base Bitmap（需要新增此方法）
- ✅ 仅跳过 Base Bitmap 中为 1 的行（Memory GC 已 compact 的删除，1 = deleted）
- ✅ 保留 Base Bitmap 中为 0 的行（包括 Deletion Chain 中的行，0 = valid）
- ❌ 错误：调用 `queryVisibility(timestamp)` 会包含 Deletion Chain，导致删除信息丢失

**Visibility 同步时**：
- 调用 `oldRGVisibility.exportDeletionBlocks()` 导出 Deletion Chain（需要新增此方法）
- 使用 RowId Mapping 转换 RowId
- 调用 `newRGVisibility.prependDeletionBlocks()` 插入到新文件头部（需要新增此方法）

---

### 3.3 Visibility 同步 (Visibility Synchronization)
新文件在创建时是“干净”的，我们需要将旧文件的 Visibility 历史数据（那些未被物理删除、但在 Memory GC 中尚未 Compact 的动态 Block）迁移过来。

*   **头部插入 (Head Insertion)**：
    *   旧数据的 Timestamp 必然 **小于** 双写产生的新数据 Timestamp。
    *   **流程**：`RGVisibility.exportDeletionBlocks` (导出所有删除) -> Java 层转换 RowId (Old -> New) -> `NewRGVisibility.prependDeletionBlocks` (插入头部)。

*   **填充 (Padding) [CRITICAL]**:
    *   **约束**: Pixels 的 Memory GC (`collectTileGarbage`) 假设链表中除了 Tail Block 外，所有中间 Block 都是满的 (`BLOCK_CAPACITY`)。
    *   **问题**: 导出的历史数据最后一个 Block 可能未满。如果直接链接到新文件的 Head，会破坏上述约束，导致 Memory GC 提前终止。
    *   **解法**: 在 `prependDeletionBlocks` 中，如果最后一个 Block 未满，**必须**使用该 Block 的最后一个元素重复填充，直至填满。由于 Bitwise OR 操作是幂等的，重复删除记录不会导致数据错误。

### 3.4 提交与原子切换 (Commit & Atomic Switch)
真正的可见性切换发生在 Metadata Service 层。

1.  **预备阶段**：新文件生成，Visibility 同步完成，Index 更新完毕。
2.  **Metadata 原子切换 (Atomic Switch)**：
    *   **接口**: 新增 `MetadataService.AtomicSwapFiles(filesToAdd, filesToDelete)`。
    *   **实现**: 在 `RdbFileDao` 中新增 `atomicSwapFiles()` 方法：
        ```java
        public boolean atomicSwapFiles(List<MetadataProto.File> filesToAdd, List<Long> filesToDelete) {
            Connection conn = db.getConnection();
            try {
                conn.setAutoCommit(false);
                // 1. 删除旧文件
                deleteByIds(filesToDelete);
                // 2. 插入新文件
                insertBatch(filesToAdd);
                conn.commit();
                return true;
            } catch (SQLException e) {
                conn.rollback();
                return false;
            } finally {
                conn.setAutoCommit(true);
            }
        }
        ```
    *   **验证**: 从 `RdbFileDao.java` 确认已有 `deleteByIds()` 和 `insertBatch()` 方法，可直接复用
    *   **事务保证**: 使用 JDBC 事务确保原子性，失败时自动回滚
3.  **物理清理 (Physical Cleanup)**：
    *   切换成功后，注销 `RetinaResourceManager` 中的映射。
    *   旧文件进入 Soft Delete 状态，延迟物理删除（可通过后台任务异步清理）。

---

## 4. 代码实现方案 (Implementation Plan)

### 4.1 模块：pixels-daemon (Metadata Service) [NEW]
*   **`proto/metadata.proto`**:
    *   新增 `rpc AtomicSwapFiles (AtomicSwapFilesRequest) returns (AtomicSwapFilesResponse);`
*   **`FileDao.java` / `RdbFileDao.java`**:
    *   新增 `boolean atomicSwapFiles(List<File> filesToAdd, List<Long> filesToDelete)`。
    *   使用 JDBC Transaction (`conn.setAutoCommit(false)`) 保证原子性。
*   **`MetadataServiceImpl.java`**:
    *   实现对应的 gRPC 接口。

### 4.2 模块：pixels-retina (C++ Native Layer)
*   **`TileVisibility.h/cpp`**:
    *   **新增方法**: `std::vector<uint64_t> exportDeletionBlocks()`
        - 遍历 `currentVersion->head` 链表，收集所有 DeleteIndexBlock 中的 items
        - 返回 raw items（每个 item 包含 rowId 和 timestamp）
    *   **新增方法**: `void prependDeletionBlocks(const uint64_t* items, size_t count)`
        - 将 items 按 BLOCK_CAPACITY (8) 分组，构建新的 DeleteIndexBlock 链表
        - **Padding [CRITICAL]**: 如果最后一个 Block 未满（count % 8 != 0），用最后一个 item 重复填充至满
        - 使用 CAS 原子更新 `currentVersion->head`，将新链表插入头部
        - 注意处理空链表时的 `tail` 指针初始化
*   **`RGVisibility.h/cpp`**:
    *   **新增方法**: `std::vector<uint64_t> exportDeletionBlocks()`
        - 遍历所有 TileVisibility，调用其 `exportDeletionBlocks()`
        - 将 Tile-local rowId 转换为 RG-global rowId: `globalRowId = tileIndex * CAPACITY + localRowId`
        - 返回所有 Tile 的删除记录（已转换为 global rowId）
    *   **新增方法**: `void prependDeletionBlocks(const uint64_t* items, size_t count)`
        - 按 rowId 将 items 分发到对应的 TileVisibility
        - 对每个 Tile，将 global rowId 转换回 local rowId，调用其 `prependDeletionBlocks()`

### 4.3 模块：pixels-retina (Java/JNI)
*   **`RGVisibility.java`**:
    *   **新增 JNI 方法**: `public native long[] exportDeletionBlocks(long handle)`
    *   **新增 JNI 方法**: `public native void prependDeletionBlocks(long[] items, long handle)`
    *   **已有方法**: `public native double getInvalidRatio(long handle)` (已实现)
*   **`RGVisibilityJni.h/cpp`**:
    *   新增 JNI 函数：`Java_io_pixelsdb_pixels_retina_RGVisibility_exportDeletionBlocks`
    *   新增 JNI 函数：`Java_io_pixelsdb_pixels_retina_RGVisibility_prependDeletionBlocks`
*   **`RetinaResourceManager.java`**:
    *   **新增字段**: `private final Map<Long, RedirectInfo> forwardMap = new ConcurrentHashMap<>();`
    *   **新增字段**: `private final Map<Long, RedirectInfo> backwardMap = new ConcurrentHashMap<>();`
    *   **新增内部类**:
        ```java
        private static class RedirectInfo {
            final long targetFileId;
            final int[] rowIdMapping;  // mapping[oldRowId] = newRowId, -1 if deleted
            RedirectInfo(long targetFileId, int[] rowIdMapping) {
                this.targetFileId = targetFileId;
                this.rowIdMapping = rowIdMapping;
            }
        }
        ```
    *   **修改方法**: `deleteRecord(long fileId, int rgId, int rgRowOffset, long timestamp)`
        - 检查 forwardMap，如果存在映射，同时删除新文件对应的行
        - 检查 backwardMap，如果存在映射，同时删除旧文件对应的行
    *   **新增方法**: `void registerRedirection(long oldFileId, long newFileId, int[] mapping)`
    *   **新增方法**: `void unregisterRedirection(long oldFileId, long newFileId)`

### 4.4 新增组件：StorageGarbageCollector (Java)
*   **位置**: `pixels-retina` 模块，新建 `io.pixelsdb.pixels.retina.StorageGarbageCollector` 类。
*   **核心逻辑**:
    1.  **Scan Phase**:
        - 遍历 `RetinaResourceManager.rgVisibilityMap`，调用 `getInvalidRatio()` 获取每个 RG 的无效率
        - 从 MetadataService 获取文件元数据（pathId, fileName, numRowGroup 等）
        - 计算文件级别的无效率：`fileInvalidRatio = sum(rgInvalidCount) / sum(rgTotalRows)`
        - 筛选出 `fileInvalidRatio > threshold` 的文件
        - 按 (tableId, retinaNodeId) 分组，合并文件直到达到目标大小
    2.  **Rewrite Phase**:
        - 使用 `PixelsReaderImpl.newBuilder().build()` 打开旧文件（已有组件）
        - 使用 `PixelsWriterImpl.newBuilder().build()` 创建新文件（已有组件）
        - 逐 RowGroup 读取数据
        - 调用 `RGVisibility.getBaseBitmap()` 获取 Base Bitmap（需要新增）
        - 根据 Base Bitmap 过滤数据，生成 RowId Mapping
        - 将有效数据写入新文件
    3.  **Sync Phase**:
        - 注册映射：`retinaResourceManager.registerRedirection(oldFileId, newFileId, mapping)`
        - 导出旧文件的 Visibility：`long[] deletions = oldRGVisibility.exportDeletionBlocks()`
        - 转换 RowId：`long[] newDeletions = convertRowIds(deletions, mapping)`
        - 插入新文件：`newRGVisibility.prependDeletionBlocks(newDeletions)`
    4.  **Commit Phase**:
        - 调用 `metadataService.atomicSwapFiles(filesToAdd, filesToDelete)`
        - 成功后注销映射：`retinaResourceManager.unregisterRedirection(oldFileId, newFileId)`
        - 失败时回滚：删除新文件，保留旧文件和映射

---

## 5. 实现细节与注意事项 (Implementation Details & Caveats)

### 5.1 Visibility 导出与插入的正确性保证
*   **时序保证**: 导出的删除记录的 timestamp 必然小于双写期间产生的新删除记录，因此头部插入不会破坏时序
*   **Padding 必要性**: Memory GC 的 `collectTileGarbage()` 假设除 tail 外所有 Block 都是满的（见 `TileVisibility.cpp:228`）
*   **幂等性**: 重复填充同一个删除记录不会导致错误，因为 Bitmap 使用 Bitwise OR 操作

### 5.2 双写期间的并发控制
*   **映射注册时机**: 必须在新文件写入完成、Visibility 同步完成后，才能注册映射开启双写
*   **查询隔离**: 双写期间，旧查询继续读取旧文件，新查询可能读取新文件，但都能看到最新的删除
*   **切换原子性**: Metadata 层的原子切换确保查询要么看到旧文件集合，要么看到新文件集合，不会看到中间状态

### 5.3 失败处理与回滚
*   **Rewrite 失败**: 删除新文件，保留旧文件，不影响系统运行
*   **Sync 失败**: 删除新文件和映射，保留旧文件
*   **Commit 失败**: 删除新文件和映射，保留旧文件，可在下次 GC 时重试
*   **部分成功**: 如果部分文件切换成功，部分失败，需要记录状态，避免重复处理

### 5.4 性能优化建议
*   **批量处理**: 一次 GC 可以处理多个文件，减少 Metadata 操作次数
*   **异步执行**: Storage GC 可以在后台线程中异步执行，不阻塞正常的读写操作
*   **增量 GC**: 优先处理无效率最高的文件，避免一次性处理过多文件
*   **限流控制**: 控制 GC 的 I/O 带宽，避免影响正常业务

### 5.5 监控与日志
*   **GC 指标**: 记录每次 GC 的文件数、回收空间、耗时等
*   **失败告警**: GC 失败时记录详细日志，必要时发送告警
*   **状态追踪**: 记录每个文件的 GC 状态（待处理、处理中、已完成、失败）

## 6. 代码验证结果 (Code Verification Results)

### 6.1 已验证的现有组件 (Verified Existing Components)
经过深入代码分析，以下组件已确认可用：

1. **TileVisibility.cpp (Line 228-237)**: 
   ```cpp
   size_t count = (blk == tail.load(std::memory_order_acquire))
                      ? tailUsed.load(std::memory_order_acquire)
                      : DeleteIndexBlock::BLOCK_CAPACITY;
   // Unfilled blocks are not reclaimed.
   if (count < DeleteIndexBlock::BLOCK_CAPACITY) {
       break;
   }
   ```
   **验证**: Memory GC 确实假设除 tail 外所有 Block 都是满的（BLOCK_CAPACITY = 8），Padding 机制必须实现。

2. **RetinaResourceManager.runGC() (Line 724-757)**:
   ```java
   private void runGC() {
       timestamp = TransService.Instance().getSafeGcTimestamp();
       createCheckpoint(timestamp, CheckpointType.GC);
       for (Map.Entry<String, RGVisibility> entry: this.rgVisibilityMap.entrySet()) {
           rgVisibility.garbageCollect(timestamp);
       }
   }
   ```
   **验证**: GC 触发机制已存在，Storage GC 可以在此基础上扩展。

3. **RdbFileDao (Line 1-273)**:
   - `insertBatch(List<MetadataProto.File> files)` (Line 195-217): ✅ 已实现
   - `deleteByIds(List<Long> ids)` (Line 253-271): ✅ 已实现
   - **验证**: 原子切换所需的数据库操作方法已就绪，只需添加事务包装。

4. **PixelsWriterImpl.Builder (Line 203-398)**:
   ```java
   PixelsWriterImpl.newBuilder()
       .setSchema(schema)
       .setPixelStride(pixelStride)
       .setRowGroupSize(rowGroupSize)
       .setStorage(storage)
       .setPath(filePath)
       .setCompressionKind(compressionKind)
       .setEncodingLevel(encodingLevel)
       .build();
   ```
   **验证**: Writer 创建接口完整，可直接用于重写文件。

5. **PixelsReaderImpl + PixelsRecordReaderImpl**:
   - `read(PixelsReaderOption option)` 返回 `PixelsRecordReader`
   - `readBatch()` 返回 `VectorizedRowBatch`
   - `RGVisibility.getBaseBitmap()` 需要新增（返回 `VersionedData.baseBitmap`）
   **验证**: 读取机制完整，需要新增 `getBaseBitmap()` 方法用于数据重写。

6. **MetadataService RPC 接口 (metadata.proto Line 31-75)**:
   - `AddFiles` (Line 67): ✅ 已存在
   - `DeleteFiles` (Line 73): ✅ 已存在
   - **缺失**: `AtomicSwapFiles` 需要新增

### 6.2 需要新增的组件 (Components to Add)

#### 6.2.1 Metadata Service 层
**需要新增的接口**：

1. **`proto/metadata.proto`**: 新增 `rpc AtomicSwapFiles` 方法
2. **`RdbFileDao.java`**: 新增 `atomicSwapFiles(List<File> filesToAdd, List<Long> filesToDelete)` 方法
   - 可复用已有的 `deleteByIds()` (Line 252) 和 `insertBatch()` (Line 192) 方法
   - 使用 JDBC 事务保证原子性
3. **`MetadataServiceImpl.java`**: 实现对应的 gRPC 接口

#### 6.2.2 C++ Native Layer
**需要新增的 C++ 方法**（基于已有的 `TileVisibility.h` 和 `RGVisibility.h`）：

**`TileVisibility.h`** (当前已有方法见 Line 1-122):
- 新增: `std::vector<uint64_t> exportDeletionBlocks()` - 导出 Deletion Chain
- 新增: `void prependDeletionBlocks(const uint64_t* items, size_t count)` - 插入 Deletion Chain 到头部
- 新增: `uint64_t* getBaseBitmap()` - 返回 `currentVersion->baseBitmap`

**`RGVisibility.h`** (当前已有方法见 Line 1-55):
- 新增: `std::vector<uint64_t> exportDeletionBlocks()` - 聚合所有 Tile 的 Deletion Chain
- 新增: `void prependDeletionBlocks(const uint64_t* items, size_t count)` - 分发到各个 Tile
- 新增: `std::vector<uint64_t> getBaseBitmap()` - 聚合所有 Tile 的 baseBitmap

**实现要点**：
- `exportDeletionBlocks`: 遍历 `currentVersion->head` 链表，收集所有 DeleteIndexBlock 中的 items
- `prependDeletionBlocks`: 需要实现 Padding 逻辑（如果最后一个 Block 未满，用最后一个元素重复填充）
- `getBaseBitmap`: 直接返回 `currentVersion->baseBitmap` 指针

#### 6.2.3 Java/JNI Layer
**需要新增的 Java/JNI 方法**（基于已有的 `RGVisibility.java` Line 1-215）：

**`RGVisibility.java`** (当前已有方法: `deleteRecord`, `getVisibilityBitmap`, `garbageCollect`, `getInvalidRatio`):
- 新增: `public long[] exportDeletionBlocks()` - 导出 Deletion Chain
- 新增: `public void prependDeletionBlocks(long[] items)` - 插入 Deletion Chain
- 新增: `public long[] getBaseBitmap()` - 获取 Base Bitmap
- 对应的 native 方法声明需要添加

**`RGVisibilityJni.cpp`**:
- 新增对应的 JNI 函数实现，调用 C++ 层的方法

#### 6.2.4 RetinaResourceManager 扩展
**需要扩展的 RetinaResourceManager.java**：

**新增字段**：
- `Map<Long, RedirectInfo> forwardMap` - 旧文件 -> 新文件的映射
- `Map<Long, RedirectInfo> backwardMap` - 新文件 -> 旧文件的映射
- `RedirectInfo` 内部类，包含 `targetFileId` 和 `rowIdMapping`

**修改方法**：
- `deleteRecord()` - 增加双写逻辑，检查 forward/backward 映射

**新增方法**：
- `registerRedirection(long oldFileId, long newFileId, int[] mapping)` - 注册映射
- `unregisterRedirection(long oldFileId, long newFileId)` - 注销映射

#### 6.2.5 StorageGarbageCollector
**需要新建的 StorageGarbageCollector 类**：

**位置**: `pixels-retina` 模块，`io.pixelsdb.pixels.retina.StorageGarbageCollector`

**核心方法**：
1. `runStorageGC()` - 主入口
2. `scanFiles()` - 扫描文件，计算无效率
3. `groupFiles()` - 按 (tableId, retinaNodeId) 分组
4. `rewriteFileGroup()` - 重写文件组

**依赖的已有组件**：
- `PixelsReaderImpl` - 读取旧文件
- `PixelsWriterImpl` - 写入新文件
- `RetinaResourceManager` - 管理 Visibility 和映射
- `MetadataService` - 原子切换文件

### 6.3 实现路径 (Implementation Path)
1. **Phase 1**: 实现 C++ Native Layer 的 export/prepend 方法
2. **Phase 2**: 实现 JNI 绑定和 Java 层接口
3. **Phase 3**: 扩展 RetinaResourceManager 的双写逻辑
4. **Phase 4**: 实现 MetadataService 的 AtomicSwapFiles
5. **Phase 5**: 实现 StorageGarbageCollector 主逻辑
6. **Phase 6**: 集成到 runGC 中，添加配置参数

### 6.4 配置参数 (Configuration Parameters)
```properties
# Storage GC 配置
storage.gc.enabled=true
storage.gc.threshold=0.5
storage.gc.target.file.size=134217728  # 128MB
storage.gc.interval=3600000  # 1 hour in ms
storage.gc.max.files.per.run=10
```

### 6.5 风险与限制 (Risks & Limitations)
1. **双写期间的性能开销**: 每次删除需要操作两个文件的 Visibility
2. **映射内存开销**: 大文件的 RowId Mapping 可能占用较多内存
3. **长尾查询影响**: 双写期间可能影响查询性能
4. **索引同步复杂度**: 如果启用了 Range/Point Index，需要同步更新

### 6.6 测试计划 (Testing Plan)
1. **单元测试**: TileVisibility 的 export/prepend 方法
2. **集成测试**: 完整的 Storage GC 流程
3. **并发测试**: 双写期间的并发删除和查询
4. **故障测试**: 各阶段失败的回滚机制
5. **性能测试**: GC 对正常业务的影响
