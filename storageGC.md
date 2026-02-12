# Pixels Storage Garbage Collection (Storage GC) 设计文档

## 1. 背景与目标 (Background & Objective)
Pixels 文件组织采用 Row Group (RG) -> Tile 的层级结构。删除信息存储在 Visibility 组件中（RGVisibility -> TileVisibility），以 Block 链表形式记录删除操作的时间戳（Timestamp）。

目前已实现 **Memory GC**（回收内存中过期的 Visibility Block）。本项目的目标是实现 **Storage GC**，回收高删除率的物理文件，通过重写数据、同步 Visibility 和更新索引，释放存储空间并优化读取性能。

## 2. 核心机制 (Core Mechanisms)

### 2.1 触发策略 (Trigger Strategy)
*   **指标计算**：在 Memory GC 遍历 Visibility 链表时，统计**无效行数 (Invalid Row Count)**。
    *   **注意**：统计时仅计算 `baseBitmap` (Safe Timestamp 对应的基准位图) 中的无效位。**不包含** Visibility 链表中尚未 Compact 的动态删除记录。
*   **阈值判定**：
    *   **单文件判定**：若 `(文件无效行数 / 文件总行数) > 阈值` (如 0.5)，标记为待回收。
    *   **合并判定**：将多个待回收文件组合，目标是重写后的新文件大小达到配置的**目标文件大小**。
    *   **合并约束 (关键)**：仅允许合并**属于同一张表 (Table)** 且 **由同一 Retina 节点 (Retina Node) 写入** 的文件。
        *   **原因**：只有同一 Retina 节点的数据才能保证 Commit Timestamp 的严格单调递增性，跨节点合并可能破坏时序逻辑。
        *   **识别**：通过解析文件名 `<Host>_<Timestamp>_<Counter>.pxl` 识别 Retina Node ID。采用**从右向左解析**策略以兼容 Hostname 中包含下划线的情况（先提取 Counter，再提取 Timestamp，剩余为 Host）。

### 2.2 重写流程 (Rewrite Process)
Storage GC 的执行分为三个主要阶段：
1.  **数据重写 (Data Rewrite)**：
    *   读取旧文件，利用 `RetinaResourceManager` 获取当前的 Visibility Bitmap。
    *   过滤掉已删除的行。
    *   将剩余有效数据写入新文件。
    *   **生成映射**: 在重写过程中，生成 **RowId Mapping**: `int[] mapping`，其中 `mapping[OldRowId] = NewRowId`。
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

### 3.2 Visibility 同步 (Visibility Synchronization)
新文件在创建时是“干净”的，我们需要将旧文件的 Visibility 历史数据（那些未被物理删除、但在 Memory GC 中尚未 Compact 的动态 Block）迁移过来。

*   **头部插入 (Head Insertion)**：
    *   旧数据的 Timestamp 必然 **小于** 双写产生的新数据 Timestamp。
    *   **流程**：`RGVisibility.exportDeletionBlocks` (导出所有删除) -> Java 层转换 RowId (Old -> New) -> `NewRGVisibility.prependDeletionBlocks` (插入头部)。

*   **填充 (Padding) [CRITICAL]**:
    *   **约束**: Pixels 的 Memory GC (`collectTileGarbage`) 假设链表中除了 Tail Block 外，所有中间 Block 都是满的 (`BLOCK_CAPACITY`)。
    *   **问题**: 导出的历史数据最后一个 Block 可能未满。如果直接链接到新文件的 Head，会破坏上述约束，导致 Memory GC 提前终止。
    *   **解法**: 在 `prependDeletionBlocks` 中，如果最后一个 Block 未满，**必须**使用该 Block 的最后一个元素重复填充，直至填满。由于 Bitwise OR 操作是幂等的，重复删除记录不会导致数据错误。

### 3.3 提交与原子切换 (Commit & Atomic Switch)
真正的可见性切换发生在 Metadata Service 层。

1.  **预备阶段**：新文件生成，Visibility 同步完成，Index 更新完毕。
2.  **Metadata 原子切换 (Atomic Switch)**：
    *   **接口**: 新增 `MetadataService.AtomicSwapFiles(filesToAdd, filesToDelete)`。
    *   **实现**: 在 `FileDao` (RdbFileDao) 中开启数据库事务，在一个事务中执行 `DELETE FROM FILES WHERE ID IN (...)` 和 `INSERT INTO FILES (...)`。
3.  **物理清理 (Physical Cleanup)**：
    *   切换成功后，注销 `RetinaResourceManager` 中的映射。
    *   旧文件进入 Soft Delete 状态，延迟物理删除。

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
*   **`TileVisibility.cpp`**:
    *   `exportDeletionBlocks()`: 返回 `std::vector<uint64_t>` (raw items)。
    *   `prependDeletionBlocks(uint64_t* items, size_t count)`:
        *   构建链表。
        *   **Padding**: 检查最后一个 Block，若未满，用最后一个 item 填满。
        *   原子更新 `head` (注意处理空链表时的 `tail` 指针初始化)。
*   **`RGVisibility.cpp`**:
    *   `exportDeletionBlocks`: 遍历所有 Tile，收集并转换为 RG-Relative RowId (或 Global RowId) 返回。
    *   `prependDeletionBlocks`: 接收 Global RowId 列表，分发给对应的 Tile。

### 4.3 模块：pixels-retina (Java/JNI)
*   **`RGVisibility.java`**:
    *   新增 JNI 接口：`exportDeletionBlocks`, `prependDeletionBlocks`, `getInvalidRowCount`。
*   **`RetinaResourceManager.java`**:
    *   新增 `Map<Long, RedirectInfo> redirectionMap`。
    *   `RedirectInfo` 包含 `targetFileId`, `int[] rowIdMapping`。
    *   修改 `deleteRecord` 实现双写逻辑。

### 4.4 新增组件：StorageGarbageCollector (Java)
*   **位置**: `pixels-retina` 模块。
*   **核心逻辑**:
    1.  **Scan**: 遍历 `RetinaResourceManager`，计算 Invalid Ratio。
    2.  **Rewrite**: 使用 `PhysicalReader` (配合 Visibility 过滤) 和 `PixelsWriter` 重写文件。
    3.  **Sync**: 建立映射，开启双写，迁移 Visibility (Export -> Convert -> Prepend)。
    4.  **Commit**: 调用 `MetadataService.atomicSwapFiles`。

---

## 5. 待确认细节 (Checklist)
1.  **Protocol Buffers 重新编译**: 修改 proto 后需要重新生成代码 (已确认编译流程)。
2.  **文件名解析**: 确认采用从右向左解析策略 (已确认)。
