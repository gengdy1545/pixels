# Pixels Retina 独立吞吐基准

`pixels-retina-benchmark` 按当前生产代码边界分别测量 Transaction、Index、Visibility 和 WriteBuffer。它不是端到端 CDC 基准；各命令只执行被测操作及其真实嵌套依赖，不能把不同命令的吞吐相加解释为 Retina 总吞吐。

当前命令：

- Transaction：`transaction-begin`、`transaction-commit`、`transaction-begin-commit`
- Index：`index-put-primary`、`index-update-primary`、`index-delete-primary`、`index-put-secondary`、`index-update-secondary`、`index-delete-secondary`
- Visibility：`visibility`
- WriteBuffer：`write-buffer-add`
- Snapshot v2：`snapshot-export`、`snapshot-validate`
- 最小服务：`transaction-server`、`metadata-server`

快照导出、一致性与恢复规则见 [SNAPSHOT.md](SNAPSHOT.md)。

## 1. 依赖边界

### 1.1 无外部服务

Index 和 Visibility 不连接 MySQL、etcd 或 gRPC 服务：

- 六个 Index 命令只打开 Snapshot v2 中 RocksDB/SQLite 的一次性工作副本；
- Visibility 只读取 manifest footer 拓扑，以 clean baseline 调用本地 JNI；
- 两者都必须提供 `--snapshot-dir` 和 `--snapshot-table`；
- Index 还要求快照包含停写后复制的完整 `state/index`。

### 1.2 Transaction

三个 Transaction 命令都需要：

1. 用户自行部署的外部 etcd；
2. runtime 中 `bin/start-transaction-server` 启动的真实 `TransServer`；
3. benchmark 客户端可访问的 Transaction 地址。

不需要 MySQL。典型端口是 etcd `2379`、Transaction gRPC `18889`，实际值必须与 `etc/pixels.properties` 一致。

### 1.3 WriteBuffer

`write-buffer-add` 需要：

1. 用户自行部署的外部 MySQL；
2. 用户自行部署的外部 etcd；
3. 已用 runtime 中 `mysql-init/metadata_schema.sql` 初始化的 Metadata 数据库；
4. `bin/start-metadata-server` 启动的真实 `MetadataServer` 和 `NodeServer`；
5. Snapshot v2 manifest，以及独立、可写的 benchmark 存储路径。

典型端口是 MySQL `3306`、etcd `2379`、Metadata gRPC `18888`、Node gRPC `18891`。实际 host 和 port 均由用户部署决定。

## 2. 配置外部服务

解压 runtime 后先编辑 `etc/pixels.properties`。该文件是物理机模板，所有 `REPLACE_WITH_*` 都必须替换，尤其是：

- MySQL host、port、user、password；
- etcd hosts、port；
- 本机可绑定且客户端可访问的 benchmark host；
- Metadata、Transaction、Retina、Node 端口。

不要使用生产账号、生产 Metadata 库或生产 etcd namespace。不要把真实凭证提交到源码仓库；写入密码后应限制配置文件权限。

模板中的 `/var/tmp/pixels-retina-benchmark` 路径仅供本地 benchmark 状态使用。运行账号必须拥有这些目录，且这些目录不能指向生产数据。Snapshot 恢复使用独立工作目录，不会直接写原快照。

初始化外部 MySQL 时使用交互式密码输入，不在命令行或文档中保存密码：

```bash
mysql --host="$MYSQL_HOST" --port="$MYSQL_PORT" \
  --user="$MYSQL_USER" --password \
  < mysql-init/metadata_schema.sql
```

`metadata_schema.sql` 继续随 runtime 分发，只用于用户管理的外部 MySQL；本模块不提供 MySQL 或 etcd 容器。

## 3. 构建 fat JAR 与 runtime

要求 Linux、JDK 8+（建议 JDK 17）、Maven 3.8+、CMake、GNU Make 和 C/C++ toolchain。Visibility/WriteBuffer 使用 JNI，构建机和运行机必须具有相同 CPU 架构，并兼容目标机 glibc/libstdc++。

在仓库根目录执行：

```bash
export PIXELS_HOME=/tmp/pixels-retina-benchmark-build
install -d -m 0700 "$PIXELS_HOME/etc" "$PIXELS_HOME/lib"
cp pixels-retina-benchmark/deploy/etc/pixels.properties \
  "$PIXELS_HOME/etc/pixels.properties"

# 必须改成目标 Snapshot v2 manifest 中 semanticConfig 的真实值。
sed -i 's/^retina.tile.visibility.capacity=.*/retina.tile.visibility.capacity=65536/' \
  "$PIXELS_HOME/etc/pixels.properties"

mvn -T 3 -pl pixels-retina-benchmark -am -DskipTests package
```

`65536` 只是替换格式示例，不是默认值。`retina.tile.visibility.capacity` 会编译进 `libpixels-retina.so`；Visibility 和 WriteBuffer 启动时会将 native capacity 与 snapshot manifest 强校验。构建过程只需要该 capacity，不需要连接模板中的外部服务。

产物：

```text
pixels-retina-benchmark/target/pixels-retina-benchmark-0.2.0-SNAPSHOT-full.jar
pixels-retina-benchmark/target/pixels-retina-benchmark-0.2.0-SNAPSHOT-runtime.tar.gz
```

runtime 内容：

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
  etc/
    pixels.properties
    retina-benchmark-log4j2.xml
  mysql-init/
    metadata_schema.sql
```

目标机不需要安装完整 Pixels。解压后：

```bash
tar -xzf pixels-retina-benchmark-0.2.0-SNAPSHOT-runtime.tar.gz
cd pixels-retina-benchmark
export PIXELS_CONFIG="$PWD/etc/pixels.properties"
```

## 4. 通用参数与指标

所有参数同时支持 `--key=value` 和 `--key value`。

- `--threads`：producer 线程数，默认 CPU 核数。
- `--clients`：Transaction 的 gRPC channel 数、WriteBuffer 的 vnode/buffer 数；Index 和 Visibility 仅作为 runner 分片标签。默认 `1`，且不能大于 threads。
- `--warmup-seconds`：预热时间上限，默认 `10`；`0` 禁用预热。
- `--warmup-operations`：预热 logical operations 上限，默认 `min(data-size,100000)`。
- `--duration-seconds`：正式阶段 soft deadline，默认 `30`。
- `--data-size`：正式阶段 logical operations 上限，默认 `1000000`。
- `--batch-size`：真实 RPC/Index batch，或标量本地调用的 driver 分组，默认 `1`。
- `--fail-fast`：首个失败组后是否停止，默认 `false`。
- `--rpc-deadline-ms`：Transaction 单次 blocking RPC deadline，默认 `30000`。

正式阶段由时间和 data-size 共同限制，先到者结束。`duration-seconds` 只在相邻 execute group 间检查；已经开始的本地调用可以越过 deadline，Transaction RPC 则另受 `rpc-deadline-ms` 限制。准备、恢复、校验和 close/drain 均不计入被测吞吐。

每次结果至少包含：

- `total_operations`、`successful_operations`、`errors`、`error_rate`；
- `elapsed_seconds`、`attempted_ops_per_second`、`throughput_ops_per_second`；
- logical operation 的 average/P50/P95/P99 latency；
- `api_calls`、`api_calls_per_second` 和 API latency；
- `stop_reason`、`first_error` 及场景特有的 `detail.*`。

`batch-size=1` 时标量场景可得到逐调用延迟。批量或 driver group 大于 1 时，logical latency 是 group 耗时按 logical operation 归一化，不能解释为每个标量调用的独立 tail latency。

## 5. Transaction 三个命令

先确保外部 etcd 可用并已替换配置，然后在终端 A 启动前台服务：

```bash
export PIXELS_CONFIG="$PWD/etc/pixels.properties"
bin/start-transaction-server
```

可用 `--trans-port` 或 `--port` 覆盖服务监听端口。一个 benchmark 环境只应运行一个 Transaction Server 实例。

终端 B 运行以下任一命令：

```bash
bin/run-benchmark transaction-begin \
  --trans-host BENCHMARK_HOST --trans-port 18889 \
  --threads 32 --clients 4 --batch-size 64 \
  --warmup-seconds 10 --duration-seconds 60 --data-size 5000000
```

```bash
bin/run-benchmark transaction-commit \
  --trans-host BENCHMARK_HOST --trans-port 18889 \
  --threads 32 --clients 4 --batch-size 64 \
  --trans-prefill-batch-size 1024 \
  --warmup-seconds 10 --duration-seconds 60 --data-size 3000000
```

```bash
bin/run-benchmark transaction-begin-commit \
  --trans-host BENCHMARK_HOST --trans-port 18889 \
  --threads 32 --clients 4 --batch-size 64 \
  --warmup-seconds 10 --duration-seconds 60 --data-size 3000000
```

真实调用链：

```text
transaction-begin
  TransService.beginTrans / beginTransBatch
  -> blocking gRPC stub
  -> TransServiceImpl
  -> PersistentAutoIncrement(etcd) + TransContextManager
```

Begin 的一个 logical operation 是一个成功创建的写事务。成功 context 在计时结束后通过真实 CommitBatch 清理；清理不计时。`--trans-cleanup-batch-size` 控制清理批次。

```text
transaction-commit
  计时前：BeginTrans / BeginTransBatch 创建真实 pending ID
  计时内：TransService.commitTrans / commitTransBatch
  -> blocking gRPC stub
  -> TransServiceImpl
  -> TransContextManager.setTransCommit + watermark 推进
```

Commit 不使用伪造 ID。`--trans-prefill-batch-size` 只影响计时前预填；每个 ID 最多提交一次。预填和测试总时长必须短于写事务 lease。

```text
transaction-begin-commit
  BeginTrans -> CommitTrans
  或 BeginTransBatch -> CommitTransBatch
```

Begin-Commit 的一个 logical operation 是一个完成的事务生命周期；每个 execute group 固定产生两个 API calls，不预填、不保留 pending window，也不对未知结果的 RPC 自动重试。

## 6. Index 六个命令

Index 必须使用 Snapshot v2 的完整 RocksDB 与 SQLite 状态，但不需要任何外部服务。每次 JVM 都先把 `state/index` 复制到临时目录或 `--snapshot-work-dir` 指定的空目录，再打开真实 `LocalIndexService`：

```text
LocalIndexService
  -> RocksDB SinglePointIndex
  -> SQLite MainIndex
```

六个独立命令及计时入口：

- `index-put-primary` → `putPrimaryIndexEntries`
- `index-update-primary` → `updatePrimaryIndexEntries`
- `index-delete-primary` → `deletePrimaryIndexEntries`
- `index-put-secondary` → `putSecondaryIndexEntries`
- `index-update-secondary` → `updateSecondaryIndexEntries`
- `index-delete-secondary` → `deleteSecondaryIndexEntries`

主索引示例：

```bash
bin/run-benchmark index-update-primary \
  --snapshot-dir /data/snapshots/node-a \
  --snapshot-table lineitem \
  --threads 32 --clients 1 --batch-size 64 \
  --warmup-seconds 10 --duration-seconds 60 --data-size 1000000
```

将命令替换为 `index-put-primary` 或 `index-delete-primary` 即可测量另外两个主索引入口。

Secondary 命令必须用索引 ID、单列名或逗号分隔的 key-column 名唯一指定目标：

```bash
bin/run-benchmark index-update-secondary \
  --snapshot-dir /data/snapshots/node-a \
  --snapshot-table lineitem \
  --snapshot-secondary-index SECONDARY_INDEX_ID_OR_KEY_COLUMNS \
  --threads 32 --clients 1 --batch-size 64 \
  --warmup-seconds 10 --duration-seconds 60 --data-size 1000000
```

将命令替换为 `index-put-secondary` 或 `index-delete-secondary` 即可测量另外两个 secondary 入口。

每个计时 batch 只包含同一 bucket 的 entries，并使用真实 `IndexUtils.getBucketIdFromByteBuffer` 分桶。Put 在计时前确认 key 不存在；Update/Delete 用真实 put 在计时外准备旧版本。Secondary fixture 还会准备可由 MainIndex 解析的真实 primary row。warmup 与 measurement 使用不相交的 key、rowId 和 timestamp 域；结束时抽查 lookup 结果。

`--clients` 不创建多个 IndexService；Index 并发度由 `--threads` 控制。原始快照只读，但工作副本会增长 MVCC 版本和 MainIndex row ranges。

## 7. Visibility

Visibility 不需要外部服务，必须提供 Snapshot v2。真实计时链：

```text
RetinaResourceManager.deleteRecord(RowLocation, timestamp)
  -> RGVisibility.deleteRecord
  -> JNI RGVisibility::deleteRGRecord
  -> C++ TileVisibility::deleteTileRecord
```

运行：

```bash
bin/run-benchmark visibility \
  --snapshot-dir /data/snapshots/node-a \
  --snapshot-table lineitem \
  --threads 32 --clients 1 --batch-size 1 \
  --warmup-seconds 10 --duration-seconds 60 --data-size 1000000
```

Visibility 只有 clean baseline：每个 phase 都根据 manifest footer 的 `recordNum` 调用 `addVisibility(fileId, rgId, recordNum, 0, null, false)`，不读取或恢复任何 Visibility state。

每个 operation 删除一个唯一物理行。若 manifest 包含物理 Index 状态，删除 timestamp 为自动记录的 `snapshotTimestamp+1`；否则为 `1`。warmup 与 measurement 使用不相交的行；每个 phase 都重建同一 clean baseline，并在计时后用 `queryVisibility` 校验所有成功删除。GC 和 Storage GC 在场景内强制关闭。

`--clients` 只影响 runner 分片，不创建外部 client。吞吐并发度由 `--threads` 控制；需要精确 JNI 单次延迟时保持 `--batch-size 1`。

## 8. WriteBuffer Add

先初始化外部 MySQL、确认外部 etcd 可用并完成配置替换。终端 A 启动前台 Metadata/Node 服务：

```bash
export PIXELS_CONFIG="$PWD/etc/pixels.properties"
bin/start-metadata-server
```

可用 `--metadata-port` 和 `--node-port` 覆盖监听端口。看到两个服务 ready 后，在终端 B 运行：

```bash
export PIXELS_CONFIG="$PWD/etc/pixels.properties"
export RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"

bin/run-benchmark write-buffer-add \
  --snapshot-dir /data/snapshots/node-a \
  --snapshot-table lineitem \
  --threads 32 --clients 4 --batch-size 64 \
  --warmup-seconds 10 --duration-seconds 60 --data-size 1000000 \
  --writebuffer-storage-scheme file \
  --writebuffer-base-uri "file:///data/retina-bench/${RUN_ID}/table" \
  --writebuffer-object-storage-scheme file \
  --writebuffer-object-folder "/data/retina-bench/${RUN_ID}/objects" \
  --writebuffer-row-pool-size 100000
```

真实计时链：

```text
PixelsWriteBuffer.addRow
  -> MemTable.add
  -> RowIdAllocator.getRowId
  -> LocalIndexService / SQLite MainIndex / PersistentAutoIncrement(etcd)
  -> 必要时 switchMemTable 和异步 flush
  -> 填充 RowLocation
```

构造和关闭还通过真实 MetadataService RPC 注册/查询文件，并创建 JNI Visibility。场景从 Snapshot v2 manifest 读取列 schema 和语义配置，按列类型生成确定性、合法的 benchmark 行；它不恢复源端行 payload、活跃 MemTable 或生产 table ID。warmup 和 measurement 各自创建新的 Metadata table、SQLite 状态和 `PixelsWriteBuffer`。

常用覆盖项：

- `--writebuffer-row-pool-size`：预生成值池大小，默认 `100000` 且不超过 data-size；值可复用，rowId/location 不复用。
- `--writebuffer-memtable-size`：正数且为 64 的倍数。
- `--writebuffer-flush-count`、`--writebuffer-flush-threads`、`--writebuffer-flush-interval`。
- `--writebuffer-encoding-level`：`0`、`1` 或 `2`。
- `--writebuffer-nulls-padding`、`--writebuffer-block-size`、`--writebuffer-replication`。
- `--writebuffer-vnodes`：必须不小于 clients。
- `--writebuffer-storage-scheme`、`--writebuffer-base-uri`。
- `--writebuffer-object-storage-scheme`、`--writebuffer-object-folder`。
- `--writebuffer-enabled-storage-schemes`。
- `--writebuffer-seed-buffers`：默认 `true`，每个 buffer 在计时外先执行一次真实 addRow。
- `--writebuffer-schema`、`--writebuffer-host-name`、`--writebuffer-drop-schema`。

一个 client 对应一个真实 vnode/`PixelsWriteBuffer`。同一 buffer 的热路径受 `rowLock` 串行化；增加 threads 会包含锁竞争，增加 clients 才会创建更多 buffer。`throughput_ops_per_second` 是 addRow 接纳吞吐；异步 flush 的最终 drain/persist 在计时外，并通过 close 指标单列。

所有输出 URI/prefix 都必须专属于本轮 benchmark。程序会拒绝与 manifest 中生产 ordered/compact 路径相同或互为父子的目标，但该字符串检查不能替代账号、bucket、文件系统和权限隔离。

## 9. 结果解释限制

- Transaction 测量真实客户端、wire protocol、服务实现和 etcd 分配路径；本仓库不包含外部 CDC sink 的调用点。
- Index 测量一次性副本上的 RocksDB + SQLite 物理操作，不代表在线服务包含的 RPC、锁或 CDC 调度成本。
- Visibility 测量本地 delete/JNI，不包含 Index lookup、Transaction 或 RPC。
- WriteBuffer 测量 addRow 接纳，不等于端到端持久化带宽。
- `batch-size>1` 的 Transaction 和 Index 是真实 batch API；Visibility/WriteBuffer 仍逐条调用标量 API。
- 所有有状态命令都应在新 JVM 中运行。重复对比应使用新的 Snapshot 工作目录和独立 WriteBuffer 输出 prefix。
