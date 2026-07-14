# Pixels Retina 核心更新操作独立吞吐基准

本模块基于当前仓库的真实实现，为 Retina 更新过程中的五类操作提供彼此独立的 throughput benchmark：

1. Transaction Begin；
2. Index primary update；
3. Visibility delete；
4. WriteBuffer Add；
5. Transaction Commit。

它不是端到端 CDC benchmark。每个命令只保留目标模块运行所需的初始化和嵌套依赖，不主动执行其他模块的完整流程。

针对现有 TPC-H SF100 环境的快照内容、停写导出顺序、`bin/export-snapshot`、SHA-256 校验以及 Index/Visibility/WriteBuffer 恢复命令，见 [`SNAPSHOT.md`](SNAPSHOT.md)。加上 `--snapshot-dir` 后，这三个 benchmark 会切换到快照场景；不加时仍使用本文后面的便携合成场景。

## 1. 源码审计结论

### 1.1 一个必须明确的仓库边界

当前仓库不包含 CDC 上游 `pixels-sink` 的源码；[`pixels-retina/README.md`](../pixels-retina/README.md) 明确把它链接到独立仓库。Retina 的更新 RPC 已经携带上游分配的 `timestamp`，见 [`proto/retina.proto`](../proto/retina.proto) 的 `TableUpdateData.timestamp`，服务端在 [`RetinaServerImpl`](../pixels-daemon/src/main/java/io/pixelsdb/pixels/daemon/retina/RetinaServerImpl.java) 中读取该值。

因此，不能从本仓库证明“外部 sink 的哪一行代码调用 Begin/Commit”。本模块对这两项采取可由当前仓库完整审计的最窄真实边界：复用 `TransService` 请求结构、生成的 gRPC stub、`transaction.proto` 和生产 `TransServiceImpl`，不构造本地事务替身。其余三项则能从本仓库的 Retina 更新分支直接追到真实本地调用。

### 1.2 五类操作总览

| 操作 | 被测真实入口 | 属性 | 计时区内的主要调用链 | 外部依赖 |
|---|---|---|---|---|
| Transaction Begin | `TransService.beginTrans(false)` / `beginTransBatch` | RPC | blocking stub → `BeginTrans` → `TransServiceImpl` → `PersistentAutoIncrement` → `TransContextManager.addTransContext` | Transaction Server、etcd |
| Index | `LocalIndexService.updatePrimaryIndexEntries` | Local | 默认 Memory、快照模式 RocksDB `SinglePointIndex.updatePrimaryEntries` → SQLite `MainIndex.getLocations/putEntries` | 无外部服务 |
| Visibility | `RetinaResourceManager.deleteRecord(RowLocation,timestamp)` | Local + JNI | ResourceManager map lookup → `RGVisibility.deleteRecord` → JNI → C++ RG/Tile deletion chain | Linux JNI；无外部服务 |
| WriteBuffer Add | `PixelsWriteBuffer.addRow` | Local | `MemTable.add` → `RowIdAllocator.getRowId` → 必要时 switch/flush → 填充 `RowLocation` | Metadata/Node Server、MySQL、etcd、localfs、Linux JNI |
| Transaction Commit | `TransService.commitTrans(id,false)` / `commitTransBatch` | RPC | blocking stub → `CommitTrans` → `TransServiceImpl` → `TransContextManager.setTransCommit` → watermark 推进 | Transaction Server、etcd |

### 1.3 Transaction Begin

真实客户端位于 [`TransService.java`](../pixels-common/src/main/java/io/pixelsdb/pixels/common/transaction/TransService.java)：

```text
TransService.beginTrans(false)
  -> BeginTransRequest{readOnly=false}
  -> TransServiceGrpc.TransServiceBlockingStub.beginTrans
  -> transaction.proto BeginTrans
  -> TransServiceImpl.beginTrans
  -> PersistentAutoIncrement.getAndIncrement
  -> TransContextManager.addTransContext
  -> BeginTransResponse -> TransContext
```

RPC 定义见 [`proto/transaction.proto`](../proto/transaction.proto)，服务端见 [`TransServiceImpl.java`](../pixels-daemon/src/main/java/io/pixelsdb/pixels/daemon/transaction/TransServiceImpl.java)。写事务使用事务 ID 作为时间戳，并在服务端留下带 lease 的 pending context。ID 的持久分段来自 etcd，不能用 `AtomicLong` 代替。

Benchmark 计时区只执行 Begin。成功返回的真实 context 被保存，phase 结束后再通过真实 CommitBatch 清理；清理不计入 Begin 吞吐。正式阶段内服务端 pending map 会随成功 Begin 增长，这是该操作的真实状态成本。

### 1.4 Index

[`RetinaServerImpl` 构造器](../pixels-daemon/src/main/java/io/pixelsdb/pixels/daemon/retina/RetinaServerImpl.java) 固定取得 `IndexServiceProvider.ServiceMode.local`。更新分支的主索引路径是：

```text
RetinaServerImpl update branch
  -> LocalIndexService.updatePrimaryIndexEntries
  -> SinglePointIndex.updatePrimaryEntries
  -> MainIndex.getLocations(previousRowIds)
  -> MainIndex.putEntries(newEntries)
```

具体实现见 [`LocalIndexService.java`](../pixels-common/src/main/java/io/pixelsdb/pixels/common/index/service/LocalIndexService.java)。独立 Index benchmark 不调用 Index RPC Server，也不调用事务、Visibility、WriteBuffer 或 `RetinaServerImpl` 外层 striped lock。

每个计时调用只包含同一 bucket 的 entries，并使用对应 `IndexOption(vNodeId=bucket)`。key 的 bucket 通过真实 [`IndexUtils.getBucketIdFromByteBuffer`](../pixels-common/src/main/java/io/pixelsdb/pixels/common/utils/IndexUtils.java) 计算。准备阶段先用真实 `putPrimaryIndexEntries` 放入旧版本；正式调用使用相同逻辑 key、同一批次事务时间戳、唯一新 rowId 和新 `RowLocation`，并校验返回的旧位置数量。

为了让独立 fat JAR 不必为 catalog discovery 启动 Metadata Service，setup 使用源码公开的 `SinglePointIndexFactory.TableIndex` descriptor 注册 `tableId/indexId/scheme/unique`；正式计时仍完整经过 `LocalIndexService`、SinglePointIndex 和 MainIndex。便携默认后端是 `memory + sqlite`，结果必须标注为该组合，不能解释成 RocksDB 上限。

### 1.5 Visibility

Retina update/delete 获得旧位置后调用本地 Visibility mutation；它不是分析查询使用的 `QueryVisibility` RPC：

```text
RetinaServerImpl
  -> RetinaResourceManager.deleteRecord(RowLocation, timestamp)
  -> deleteRecord(fileId, rgId, rgRowOffset, timestamp)
  -> checkRGVisibility / ConcurrentHashMap lookup
  -> RGVisibility.deleteRecord
  -> JNI RGVisibility::deleteRGRecord
  -> C++ TileVisibility::deleteTileRecord
```

Java 路径见 [`RetinaResourceManager.java`](../pixels-retina/src/main/java/io/pixelsdb/pixels/retina/RetinaResourceManager.java) 和 [`RGVisibility.java`](../pixels-retina/src/main/java/io/pixelsdb/pixels/retina/RGVisibility.java)，JNI/C++ 路径见 [`cpp/pixels-retina/lib`](../cpp/pixels-retina/lib)。

初始化采用 `FileWriterManager` 创建新文件时使用的同一方法：

```java
addVisibility(fileId, rgId, recordNum, 0L, null, false)
```

每个 worker 独占 file/RG，每个物理行只删除一次；timestamp 大于 0、小于 `2^48`，并在每个 worker 内单调递增。phase 结束后、回收 JNI 对象前，程序会通过 `queryVisibility` 抽样确认 bit 已置位。正式测试强制关闭 Visibility GC 和 Storage GC，否则会混入 Transaction RPC、checkpoint 或 dual-write。

快照模式按生产 `RetinaServerImpl` 的启动范围恢复 readable layout 的第一个 ordered/compact path：存在 checkpoint 时走真实 `CheckpointFileIO.readCheckpointParallel -> addVisibility(..., T_snap, bitmap, true)`，其余 RG 使用上述 clean-add。每个 phase 都重建相同初态，恢复和校验不计时。

### 1.6 WriteBuffer Add

真实链路为：

```text
Retina streaming update RPC
  -> RetinaServerImpl.processUpdateRequest
  -> RetinaResourceManager.insertRecord
  -> PixelsWriteBuffer.addRow                    [benchmark boundary]
  -> MemTable.add
  -> RowIdAllocator.getRowId
  -> switchMemTable / asynchronous flush as needed
  -> populate RowLocation
```

入口和同步逻辑见 [`PixelsWriteBuffer.java`](../pixels-retina/src/main/java/io/pixelsdb/pixels/retina/PixelsWriteBuffer.java)，直接上游见 [`RetinaResourceManager.insertRecord`](../pixels-retina/src/main/java/io/pixelsdb/pixels/retina/RetinaResourceManager.java)。只调用 `MemTable.add` 会绕过 row-id 分配、`rowLock`、位置填充、memtable 切换和真实 flush，因此本模块没有这样简化。

虽然 `addRow` 是本地方法，它的真实嵌套依赖仍被保留：

- `FileWriterManager` 通过真实 MetadataService RPC 执行 `addFiles/getFileId`，见 [`FileWriterManager.java`](../pixels-retina/src/main/java/io/pixelsdb/pixels/retina/FileWriterManager.java)；
- `RowIdAllocator` 经过 LocalIndexService 和 SQLite MainIndex，并由 `PersistentAutoIncrement` 从 etcd 分段取 rowId，见 [`RowIdAllocator.java`](../pixels-common/src/main/java/io/pixelsdb/pixels/common/index/RowIdAllocator.java) 与 [`SqliteMainIndex.java`](../pixels-index/pixels-index-main-sqlite/src/main/java/io/pixelsdb/pixels/index/main/sqlite/SqliteMainIndex.java)；
- 新文件创建真实 `RGVisibility`；
- flush 走真实 `StorageFactory -> localfs -> PixelsWriter`。

默认行 schema 是 TPCH nation 风格：`nationkey bigint, name varchar(25), regionkey bigint, comment varchar(152)`。bigint 使用 big-endian 8-byte 编码，字符串使用 UTF-8。一个 driver batch 共用事务时间戳，但仍逐行调用真实标量 `addRow`。

快照模式不复制活跃 MemTable，而是恢复源 schema、canonical 真实行样本和 WriteBuffer/Pixels writer/storage 参数，再创建全新的 benchmark table、SQLite 和 buffers；这样保留真实数据宽度与 flush 行为，同时不会复用生产 table ID 或输出路径。

### 1.7 Transaction Commit

Commit 必须消费真实存在且仍 pending 的事务 ID：

```text
TransService.commitTrans(realId, false)
  -> CommitTransRequest{transId}
  -> blocking gRPC
  -> TransServiceImpl.commitTrans
  -> TransContextManager.setTransCommit
  -> remove/terminate transaction state
  -> pushWatermarks
```

正式计时前，worker 通过真实 BeginTransBatch 预填唯一写事务；计时区只执行 Commit。每个 ID 最多消费一次。RPC 异常可能意味着远端结果未知，因此程序不会自动重试同一 ID；未使用的预填 ID 在 phase 结束后、计时区外清理。

## 2. Benchmark 统计模型

### 2.1 通用参数

所有命令支持 `--key=value` 和 `--key value` 两种形式。

| 参数 | 默认值 | 含义 |
|---|---:|---|
| `--threads` | CPU 核数 | 固定 producer 线程池大小 |
| `--clients` | 1 | Begin/Commit 的 dedicated RPC channel 数，或 WriteBuffer 的真实 vnode/buffer 数；Index/Visibility 的精确语义见下表 |
| `--warmup-seconds` | 10 | 预热时间上限；0 表示禁用 |
| `--duration-seconds` | 30 | 正式阶段时间上限 |
| `--data-size` | 1,000,000 | 正式阶段最多执行的 logical operations |
| `--warmup-operations` | `min(data-size,100000)` | 预热最多执行的 logical operations |
| `--batch-size` | 1 | 真实 RPC/Index batch，或标量本地调用的 driver 分组 |
| `--fail-fast` | false | 首个失败组后停止 |
| `--rpc-deadline-ms` | 30000 | 仅 Begin/Commit：每次 blocking gRPC 调用的最大等待时间，必须大于 0 |

测试同时受时间和 data-size 限制，先到者结束；`stop_reason` 会输出 `duration`、`data-size` 或 `error`。`duration-seconds` 是 soft deadline：runner 只在相邻两个 `execute` group 之间检查时间，已经进入的本地调用可以在 deadline 后完成。Begin/Commit 的在途 blocking RPC 也可能跨过 phase deadline，但每次调用都会被 `--rpc-deadline-ms` 限定；phase 后的事务清理、校验和资源关闭位于计时区外，因此进程总运行时间还可能更长。

### 2.2 operation、batch、client 的准确含义

| Benchmark | 一个 logical op | `batch-size > 1` | `clients` |
|---|---|---|---|
| Begin | 一个写事务 Begin | 调用真实 `BeginTransBatch`；一个 batch 是一次 RPC | 预创建并复用的 active gRPC channels |
| Index | 一条 `PrimaryIndexEntry` update | 同 bucket entries 的一次真实本地 batch API | 只作为生成 key 时的 namespace；不创建连接或独立 index，所有线程共享该 phase 的 table/index/MainIndex |
| Visibility | 一次唯一物理行 delete | 只是 driver 领取/计时分组；内部仍是 N 次标量本地/JNI 调用 | 仅保留为通用 runner 的分组标签；不创建模块 client，也不改变每个 worker 独占 file/RG 的状态拓扑 |
| WriteBuffer Add | 一次真实 `addRow` | 同 timestamp 的 driver 分组；内部仍是 N 次标量调用 | 每个 client 是一个真实 vnode/`PixelsWriteBuffer` |
| Commit | 一个真实 pending ID 的 Commit | 调用真实 `CommitTransBatch`；一个 batch 是一次 RPC | 预创建并复用的 active gRPC channels |

`TransService` 类初始化本身还会构造生产默认 singleton channel；多客户端 benchmark 的流量只使用并统计 `--clients` 个 dedicated active channels，该默认 channel 保持空闲并由 JVM shutdown hook 关闭。

对 Index 和 Visibility，实际模块并不存在“client 数”这一资源维度：吞吐并发度由 `--threads` 控制。Index 的 `clientId` 仅写入合成主键以隔离 key namespace；Visibility 的 worker 状态始终按 `workerId` 创建，改变 `--clients` 不会改变 file/RG 数量、JNI 对象数量或共享关系。二者仍受通用校验 `clients <= threads` 约束，通常保持 `--clients 1` 即可。

结果同时输出 logical operations 与 API calls。`throughput_ops_per_second` 使用成功 logical ops；`attempted_ops_per_second` 包含失败尝试。

### 2.3 延迟口径和框架开销

每个 `execute` group 只读取两次 `System.nanoTime()`，使用线程本地计数器和线程本地 HdrHistogram；正式结束后才合并。线程池、RPC channels、测试 protobuf/行池均复用；热路径不打印日志，也不创建 benchmark 线程或连接。

- logical latency：`execute group elapsed / logical ops`；
- API latency：`execute group elapsed / reported API calls`；
- `batch-size=1` 时 logical P50/P95/P99 是逐操作分位数；
- 一次 group 只有一个真实 API call 时，API P50/P95/P99 是准确调用分位数；
- 标量 Visibility/WriteBuffer 在 `batch-size>1` 时输出的是 group 内归一化估计，不是每个标量调用单独计时的 tail latency。需要精确逐调用分位数时使用 `--batch-size 1`。

框架开销主要是 timer 和 histogram 记录；`batch-size=1` 的极高吞吐测试中，这些成本会压低测得上限。RPC 的 protobuf/stub 开销和目标模块本身产生的对象属于真实调用链，不视为框架噪声。

## 3. 状态、预填与重置

| Benchmark | 是否可重复使用数据 | 预填/清理策略 | 状态增长影响 |
|---|---|---|---|
| Begin | 不能重复使用事务 ID | phase 后真实 CommitBatch 清理已返回 context；无法识别的 ambiguous context 等 lease 到期 | 计时中 pending map/skip-list 增长 |
| Index | 逻辑 key 只在该 phase 更新一次；timestamp/rowId 不复用 | warmup 与正式使用不同 table/index；正式前预填旧版本 | SinglePointIndex 保存 MVCC 版本，MainIndex 增加新 rowId |
| Visibility | 物理 `RowLocation` 不复用 | 每 phase 使用新 file IDs；结束后 `reclaimVisibility` | deletion chain 随 delete 增长；GC 被禁用 |
| WriteBuffer | 行值池可循环复用；物理 rowId/location/timestamp 始终新建 | warmup 与正式使用不同 metadata table 和 buffers；结束时真实 drain/close | memtable、异步对象和最终文件增长；close 时间单列且不计入吞吐 |
| Commit | ID 不可重复提交 | 正式前真实 Begin 预填；每个 ID 消费一次；剩余 ID 计时外清理 | pending set 在提交过程中缩小，watermark 推进 |

建议每次正式对比使用新 JVM。Transaction Server 也应重启，以清除其 JVM static/singleton 状态；etcd/MySQL 是否清空取决于是否需要全新持久状态。WriteBuffer 默认使用唯一 schema 名，不默认删除 schema；若确认该 schema 只属于本次 benchmark，可设置 `--writebuffer-drop-schema true`。

预热的目的就是保留 JIT、连接和缓存热度，因此不是“恢复进程到冷启动”：Transaction 预热 context 会被清理，但 ID counter/high watermark 已前进；Index/Visibility/WriteBuffer 使用新的正式数据状态，却仍保留 JVM、allocator 和 OS cache 热度。需要 cold-state 数字时设置 `--warmup-seconds 0`，并使用新服务进程和持久目录。

## 4. Linux 构建

### 4.1 前置条件

- Linux，且构建机与运行机使用相同 CPU 架构（例如都为 x86_64）；
- JDK 8+（建议 JDK 17）和 Maven 3.8+；
- CMake、GNU Make、C/C++ toolchain；
- native 构建所需的常规开发工具和网络访问。

不需要预先安装 Pixels。构建时只创建一个临时 `PIXELS_HOME`，因为现有 `pixels-retina` Maven/CMake 流程把 native 产物安装到 `$PIXELS_HOME/lib`。

### 4.2 构建命令

在仓库根目录执行：

```bash
export PIXELS_HOME=/tmp/pixels-retina-benchmark-build
install -d -m 0700 "$PIXELS_HOME/etc" "$PIXELS_HOME/lib"
cp pixels-retina-benchmark/deploy/etc/pixels.properties "$PIXELS_HOME/etc/pixels.properties"
# 仅把这一项改成源 snapshot.json/effectiveConfig 中的值
sed -i 's/^retina.tile.visibility.capacity=.*/retina.tile.visibility.capacity=65536/' \
  "$PIXELS_HOME/etc/pixels.properties"

mvn -T 3 -pl pixels-retina-benchmark -am -DskipTests package
```

`65536` 只是命令格式示例，不是 SF100 固定值；不要把含数据库/云凭证的完整生产配置复制进构建目录。恢复现有 Visibility/WriteBuffer 快照时，必须保证 `retina.tile.visibility.capacity` 与源值完全相同；该值会编译进 JNI。运行时会读取 native capacity 并与 snapshot manifest 强校验，不一致时直接拒绝启动。

根 POM 默认设置了 Surefire `skipTests=true`；上面的参数也明确表示本次只构建。构建会编译当前源码对应的 `libpixels-retina.so`，把它和 jemalloc、Java 依赖一起放入 executable fat JAR，并通过 Shade 的 `ServicesResourceTransformer` 合并 Storage/Index SPI。

产物：

```text
pixels-retina-benchmark/target/pixels-retina-benchmark-0.2.0-SNAPSHOT-full.jar
pixels-retina-benchmark/target/pixels-retina-benchmark-0.2.0-SNAPSHOT-runtime.tar.gz
```

runtime tarball 还包含：

```text
pixels-retina-benchmark/
  README.md
  SNAPSHOT.md
  pixels-retina-benchmark-full.jar
  bin/
    export-snapshot
    run-benchmark
    start-metadata-server
    start-transaction-server
    stop-transaction-server
  etc/
  docker-compose.yml
  mysql-init/metadata_schema.sql
```

它不要求服务器安装完整 Pixels。

## 5. 最小部署与运行

目标 Linux 服务器不需要安装 Pixels，但需要满足以下运行时条件：

- 安装与 Java 8 bytecode 兼容的 64-bit Linux JRE（建议 JRE 17），且 CPU 架构与构建机及打包的 JNI 一致；
- 运行 Transaction Begin、Transaction Commit 或 WriteBuffer Add 时，安装 Docker Engine 与支持 `docker compose` 命令的 Compose v2 插件；Index 和 Visibility 不需要 Docker；
- 系统提供与构建产物兼容的 glibc 和 libstdc++。fat JAR 虽然包含 `libpixels-retina.so` 和 jemalloc，但这些 ELF 库仍动态依赖目标机的 C/C++ runtime。最稳妥的方式是在与目标机相同的发行版/版本上构建，或使用不高于目标机 glibc/libstdc++ baseline 的构建环境。

解压并进入 runtime 目录：

```bash
tar -xzf pixels-retina-benchmark-0.2.0-SNAPSHOT-runtime.tar.gz
cd pixels-retina-benchmark
```

默认端口：

| 组件 | 端口 | 用途 |
|---|---:|---|
| etcd | 2379 | Transaction ID 与 WriteBuffer rowId 分段分配 |
| MySQL | 3306 | Metadata catalog |
| Metadata Server | 18888 | WriteBuffer 文件注册与查询 |
| Transaction Server | 18889 | Begin/Commit RPC |
| Node Server | 18891 | Metadata 建表时查询 Retina 节点；由 metadata launcher 一并启动 |

功能性最小部署可从 2 CPU、4 GiB RAM 起步；吞吐上限测试应至少让 `threads` 对应可用 CPU，并为预生成数据、pending transaction、MVCC versions 和 JNI deletion chain 预留 heap/native memory。默认一百万条数据建议从 `-Xms4g -Xmx4g` 起，数百万至千万级通常需要 8 GiB 或更多 heap。WriteBuffer 还需要足以容纳最终 `.pxl` 文件和中间 object 的本地磁盘；具体量随 schema 字节数、data-size、flush 时机和编码率变化，不能用固定常数替代容量规划。

### 5.1 Transaction Begin benchmark

终端 A：

```bash
docker compose up -d --wait etcd
bin/start-transaction-server
```

终端 B：

```bash
bin/run-benchmark transaction-begin \
  --threads 32 --clients 4 \
  --warmup-seconds 10 --duration-seconds 60 \
  --data-size 5000000 --batch-size 1 \
  --rpc-deadline-ms 30000
```

事务专用参数：

- `--trans-host`，默认 `127.0.0.1`；
- `--trans-port`，默认 `18889`；
- `--rpc-deadline-ms`，默认 30000；限制每次 Begin/BeginBatch 以及计时外清理 CommitBatch 的 blocking RPC 等待时间；
- `--trans-cleanup-batch-size`，phase 后清理 Begin context 的 batch 大小。

### 5.2 Index benchmark

无需外部服务：

```bash
bin/run-benchmark index \
  --threads 32 --clients 1 \
  --warmup-seconds 10 --duration-seconds 60 \
  --data-size 2000000 --batch-size 64
```

Index 专用参数：

- `--index-buckets`，默认 128；
- `--index-rows-per-rg`，默认 100000，仅用于生成语义正确的 `RowLocation`；
- `--index-sqlite-path`，默认创建本次 JVM 专用临时目录。

数据预生成需要为每个 key 搜索目标 bucket；较大 data-size 会增加计时前 CPU 和内存需求。

`--clients` 不创建独立 IndexService 或 MainIndex；它只改变合成 key 中的 namespace。测试模块并发度请使用 `--threads`。

### 5.3 Visibility benchmark

无需外部服务；fat JAR 会在启动时提取并加载同一次构建生成的 JNI 与 jemalloc：

```bash
bin/run-benchmark visibility \
  --threads 32 --clients 1 \
  --warmup-seconds 10 --duration-seconds 60 \
  --data-size 5000000 --batch-size 1 \
  --visibility-rows-per-rg 65536
```

`batch-size=1` 用于获得精确的逐次 JNI 调用延迟。`data-size` 是有限、唯一物理行数量；耗尽后停止，不回绕复用。

Visibility 没有模块 client 概念；`--clients` 只是通用 runner 接受的分组标签，不改变按 worker 创建的 file/RG 和 JNI 状态，通常保持 1。

### 5.4 WriteBuffer Add benchmark

终端 A：

```bash
docker compose up -d --wait etcd mysql
bin/start-metadata-server
```

该 launcher 启动仓库真实的 `MetadataServer/MetadataServiceImpl` 和 `NodeServer/NodeServiceImpl`，不是 mock。Node Service 依赖 etcd；隔离部署中没有注册 Retina 节点，因此 Metadata 建表不会额外远程创建 write buffer。

终端 B：

```bash
bin/run-benchmark write-buffer-add \
  --threads 32 --clients 4 \
  --warmup-seconds 10 --duration-seconds 60 \
  --data-size 2000000 --batch-size 64 \
  --writebuffer-row-pool-size 100000
```

WriteBuffer 专用参数：

| 参数 | 默认值 | 含义 |
|---|---|---|
| `--writebuffer-row-pool-size` | 100000，且不超过 data-size | 预生成 TPCH-like 行数；值可复用，物理版本不复用 |
| `--writebuffer-memtable-size` | 配置文件值 10240 | 必须为 64 的正整数倍 |
| `--writebuffer-flush-count` | 20 | 一个文件包含的 memtable 阈值 |
| `--writebuffer-flush-threads` | 4 | object flush 线程数 |
| `--writebuffer-flush-interval` | 30 | file flush 间隔，秒 |
| `--writebuffer-encoding-level` | 2 | Pixels writer encoding level：0、1 或 2 |
| `--writebuffer-nulls-padding` | false | 是否启用 writer null padding |
| `--writebuffer-block-size` | 配置文件值 | Pixels writer block size |
| `--writebuffer-replication` | 配置文件值 | storage replication |
| `--writebuffer-vnodes` | 配置文件值 | `node.virtual.num`，必须不小于 clients |
| `--writebuffer-storage-scheme` | file | 新 benchmark table 的 storage scheme |
| `--writebuffer-base-uri` | `/tmp` 下唯一 file URI | ordered/compact table base path |
| `--writebuffer-object-folder` | 配置文件值 | localfs object 中间目录 |
| `--writebuffer-object-storage-scheme` | 配置文件值 | immutable MemTable object storage scheme |
| `--writebuffer-enabled-storage-schemes` | 配置文件值 | 启用的 Storage SPI 列表 |
| `--writebuffer-seed-buffers` | 合成 false；快照 true | 每个 buffer 在计时外预先执行一次真实 `addRow` |
| `--writebuffer-schema` | 唯一名称 | Metadata schema |
| `--writebuffer-host-name` | `benchmark` | 真实 Pixels 文件名中的 host 部分 |
| `--writebuffer-drop-schema` | false | 结束后是否删除本次 schema |

单个 `PixelsWriteBuffer` 的热路径受源码 `rowLock` 串行化；增加 threads 主要显示锁竞争。`clients>1` 创建多个真实 vnode/buffer，是 Retina 的横向接纳单位。

快照运行的完整命令和防止写回生产路径的规则见 [`SNAPSHOT.md`](SNAPSHOT.md)。非 file 存储必须显式指定独立的 `--writebuffer-base-uri` 和 `--writebuffer-object-folder`。

### 5.5 Transaction Commit benchmark

建议重启 Transaction Server 后单独运行，以获得干净的 JVM 事务状态：

```bash
bin/run-benchmark transaction-commit \
  --threads 32 --clients 4 \
  --warmup-seconds 10 --duration-seconds 60 \
  --data-size 3000000 --batch-size 1 \
  --trans-prefill-batch-size 1024 \
  --rpc-deadline-ms 30000
```

`--trans-prefill-batch-size` 只控制计时前 Begin 预填 RPC；正式 Commit 仍严格使用 `--batch-size`。`--rpc-deadline-ms`（默认 30000）限制预填 Begin、正式 Commit 和计时外清理 RPC 的单次 blocking 等待时间。预填加正式测试必须显著短于写事务 lease（当前服务端约 300 秒），否则早期 ID 会过期。

完成后停止依赖容器：

```bash
docker compose down -v
```

## 6. 输出示例

下面只是格式示例，不是本仓库或某台机器的实测结论：

```text
benchmark=index
phase=measurement
threads=32
clients=1
batch_size=64
requested_operations=2000000
total_operations=2000000
successful_operations=2000000
errors=0
error_rate=0.000000
error_rate_percent=0.000000
elapsed_seconds=12.500000
attempted_ops_per_second=160000.000000
throughput_ops_per_second=160000.000000
average_latency_us=18.420000
p50_latency_us=17.200000
p95_latency_us=25.600000
p99_latency_us=41.300000
api_calls=31250
api_calls_per_second=2500.000000
average_api_latency_us=1178.880000
p50_api_latency_us=1100.000000
p95_api_latency_us=1638.000000
p99_api_latency_us=2643.000000
stop_reason=data-size
first_error=none
detail.call=LocalIndexService.updatePrimaryIndexEntries (local)
detail.singlePointIndex=memory
detail.mainIndex=sqlite
```

每次运行还会输出场景调用属性、后端、状态策略、close/drain 时间或首个模块错误等 `detail.*` 字段。

## 7. 被测程序、模块和依赖的区分

- benchmark 程序：本模块的 runner、worker、timer、histogram、数据准备和结果输出；
- 被测模块：`TransService + TransServiceImpl`、`LocalIndexService`、`RetinaResourceManager/RGVisibility` 或 `PixelsWriteBuffer`；
- 外部依赖：仅在真实源码链要求时启动的 etcd、MySQL 和最小真实 gRPC server。

Transaction launcher 直接构造 [`TransServer`](../pixels-daemon/src/main/java/io/pixelsdb/pixels/daemon/transaction/TransServer.java)，Metadata launcher 直接构造 [`MetadataServer`](../pixels-daemon/src/main/java/io/pixelsdb/pixels/daemon/metadata/MetadataServer.java) 与 [`NodeServer`](../pixels-daemon/src/main/java/io/pixelsdb/pixels/daemon/node/NodeServer.java)。它们没有复制协议或重写服务实现。

## 8. 解释结果时的限制

- 这些数字是五个模块各自的上限测试，不能相加后当作端到端 Retina 更新吞吐；
- Begin/Commit 的 CDC 上游调用点不在当前仓库，本模块只能保证仓库内 Transaction API、wire protocol 和 server path 完全真实；
- 不带 `--snapshot-dir` 的默认 Index 是 memory SinglePointIndex + SQLite MainIndex；快照模式使用生产 RocksDB + SQLite 的一次性副本。两类结果必须明确区分，当前快照恢复不支持 Rockset；
- WriteBuffer 指标是 `addRow` 接纳吞吐。异步 flush 可能滞后，最终 `close` 的 drain/persist 时间在计时外单列；它不等于持续存储带宽；
- WriteBuffer 内部异步任务的某些异常只由原实现记录日志，不能精确归因到某次已经返回的 `addRow`；同步错误和最终 close 错误仍会使测试失败或计入错误；
- `duration-seconds` 是 group 边界检查的 soft deadline；本地在途调用可能延后结束，事务在途调用则由 `rpc-deadline-ms` 给出单次等待上界，计时外清理/关闭还会增加进程总用时；
- JNI fat JAR 必须面向目标 Linux 架构构建，并与目标机 glibc/libstdc++ ABI 兼容；不要把 macOS、其他架构或要求更高 C/C++ runtime baseline 的产物复制到目标机；
- 一个 Transaction Server 进程使用一套 JVM singleton 状态；最小部署只应运行一个副本；
- 数据准备发生在计时外，但 Index protobuf、Visibility row locations 和 Commit pending IDs 仍占内存。增大 `data-size` 前应预留足够 heap，例如通过 `JAVA_OPTS="-Xms8g -Xmx8g"` 配置；
- 配置由 `PIXELS_CONFIG`/`PIXELS_HOME` 在 JVM 首次初始化 singleton 时读取。每个命令使用独立 JVM，避免同一 JVM 内切换 backend 或 endpoint。
