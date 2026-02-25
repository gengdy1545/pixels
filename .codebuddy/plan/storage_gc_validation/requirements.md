# Storage GC实现方案验证需求文档

## 1. 引言

本文档基于对Pixels项目Storage GC（存储垃圾回收）实现方案的全面代码验证。通过逐行检查关键组件代码，验证方案文档中技术声明的准确性，确保实现与设计一致。

## 2. 验证目标

### 2.1 核心验证目标
- 确认所有Storage GC相关组件已正确实现
- 验证方案文档中的技术声明与代码实现一致
- 识别实现与设计之间的差异和优化点
- 提供完整的实现状态评估

### 2.2 验证范围
- C++ Native层：TileVisibility、RGVisibility
- JNI绑定层：RGVisibilityJni
- Java接口层：RGVisibility.java
- 双写逻辑：RetinaResourceManager
- 主逻辑：StorageGarbageCollector
- Metadata服务：RdbFileDao、metadata.proto

## 3. 需求列表

### 需求 1：核心组件完整性验证

**用户故事**：作为系统架构师，我希望验证所有Storage GC核心组件是否已完整实现，以便确认系统架构的正确性。

#### 验收标准
1. WHEN 检查RGVisibility.java THEN 系统SHALL包含所有Storage GC方法（exportDeletionBlocks、prependDeletionBlocks、getBaseBitmap、getInvalidCount、getTotalRowCount）
2. WHEN 检查RetinaResourceManager.java THEN 系统SHALL实现完整的双写逻辑（forwardMap、backwardMap、registerRedirection、双写deleteRecord）
3. WHEN 检查StorageGarbageCollector.java THEN 系统SHALL实现Scan→Rewrite→Sync→Commit完整流程
4. WHEN 检查C++层 THEN 系统SHALL在TileVisibility.h和RGVisibility.h中定义所有必要方法

### 需求 2：技术一致性验证

**用户故事**：作为质量保证工程师，我希望验证方案文档中的技术声明是否与代码实现一致，以便确保设计文档的准确性。

#### 验收标准
1. WHEN 验证Base Bitmap语义 THEN 系统SHALL确认1表示已删除、0表示有效的位图语义
2. WHEN 验证Deletion Block编码 THEN 系统SHALL确认rowId（高16位）和timestamp（低48位）的编码格式正确
3. WHEN 验证Memory GC集成 THEN 系统SHALL确认Storage GC在Memory GC完成后作为第三阶段执行
4. WHEN 验证原子交换 THEN 系统SHALL确认Metadata层的atomicSwapFiles使用JDBC事务保证原子性

### 需求 3：差异识别与优化建议

**用户故事**：作为开发工程师，我希望识别实现与设计之间的差异，以便进行必要的优化和修正。

#### 验收标准
1. IF 发现硬编码配置参数 THEN 系统SHALL标记为待优化项
2. IF 发现RowId Mapping转换逻辑 THEN 系统SHALL验证转换精度和边界条件
3. IF 发现多文件合并限制 THEN 系统SHALL评估backwardMap的一对一映射限制
4. IF 发现配置参数动态获取 THEN 系统SHALL建议从旧文件PostScript读取替代硬编码

### 需求 4：实现状态评估

**用户故事**：作为项目经理，我希望获得完整的实现状态评估，以便规划后续开发工作。

#### 验收标准
1. WHEN 评估C++ Native层 THEN 系统SHALL给出"已实现"状态评级
2. WHEN 评估Java接口层 THEN 系统SHALL给出"已实现"状态评级  
3. WHEN 评估Metadata服务 THEN 系统SHALL给出"已实现"状态评级
4. WHEN 评估配置参数 THEN 系统SHALL给出"部分硬编码，需要优化"状态评级

## 4. 验证方法论

### 4.1 代码检查方法
- 逐行阅读关键组件源代码
- 对比方案文档中的技术声明
- 验证方法签名、实现逻辑和集成点
- 检查配置参数和硬编码值

### 4.2 一致性评估标准
- ✅ 完全一致：声明与实现完全匹配
- ⚠️ 部分一致：核心逻辑正确但存在优化空间
- ❌ 不一致：声明与实现存在重大差异

## 5. 预期输出

### 5.1 验证报告结构
- 执行摘要：总体评估结果
- 详细验证：按组件分类的验证结果
- 差异分析：识别的不一致和优化点
- 建议措施：具体的改进建议

### 5.2 质量指标
- 实现完整性：已实现组件占比
- 一致性评分：声明与实现的一致性程度
- 风险评估：潜在的技术风险点
- 优化优先级：改进建议的紧急程度

## 6. 成功标准

### 6.1 技术成功标准
- 所有核心组件实现完整性 ≥ 95%
- 关键技术声明一致性 ≥ 90%
- 识别关键差异数量 ≤ 5个

### 6.2 业务成功标准
- 提供可操作的优化建议
- 明确后续开发优先级
- 为项目决策提供可靠依据