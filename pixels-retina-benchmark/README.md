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
export JAVA_OPTS='-Xms40g -Xmx40g'
```

本实验约定所有压测相关 Java 进程（服务端和 benchmark 客户端）均使用
`-Xms40g -Xmx40g`。不要同时保留当前场景不需要的 Java 服务，以免多个
40 GiB heap 加上 native/off-heap 内存超过物理内存。

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

推荐用 runtime 自带的编排脚本一次完成三项测试。脚本默认使用 `scenario`
隔离模式：每个场景创建全新的 etcd data dir 和 Transaction Server，自动等待
服务就绪、将服务端和客户端 JVM 均设为 `-Xms40g -Xmx40g`、保存日志、
检查结果，并在失败时停止，不继续运行
已受污染的下一项。

```bash
export PIXELS_HOME="$PWD"
export PIXELS_CONFIG="$PWD/etc/pixels.properties"

bin/run-transaction-suite \
  --isolation scenario \
  --etcd-bin /path/to/etcd \
  --etcdctl-bin /path/to/etcdctl \
  --results-dir /data/retina-benchmark-results
```

正式默认参数对齐 16 vCPU：16 threads、4 clients、batch 64、生命周期
测量 60 秒；Begin/Commit 各以 `2e7` operations 限制 pending 规模。先用
`--smoke` 做小规模功能验证：

```bash
bin/run-transaction-suite \
  --isolation scenario \
  --etcd-bin /path/to/etcd \
  --etcdctl-bin /path/to/etcdctl \
  --smoke
```

`--isolation suite` 则整套测试共用一个全新 etcd，但每项仍重启 Transaction
Server；适合模拟事务 ID 和 watermark 持续递增的生产语义。脚本不会删除或
覆盖已有 etcd 数据，也不会杀死未知的端口占用进程。默认端口 `2379/2380/18889`
必须空闲，可用对应 CLI 参数覆盖。

如只需手动运行单项，仍可用 `bin/start-transaction-server` 和
`bin/run-benchmark transaction-*`。此时必须自行隔离 etcd、重启服务并处理
Begin cleanup；不建议用手动方式跑整套正式实验。

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

推荐用 runtime 自带的脚本一次验证三个主索引入口。脚本会先执行
`snapshot-validate --snapshot-require index`，再依次运行
Put、Update、Delete；每项使用不同且原本不存在的 `--snapshot-work-dir`，
因此每个 JVM 都从只读 snapshot 重新复制完整 RocksDB 和 SQLite，不继承上一项
的写入或删除：

```bash
export PIXELS_HOME="$PWD"
export PIXELS_CONFIG="$PWD/etc/pixels.properties"

bin/run-index-suite \
  --snapshot-dir /data/snapshots/node-a \
  --snapshot-table customer \
  --work-dir-root /data/retina-benchmark-work/index \
  --results-dir /data/retina-benchmark-results
```

正式默认参数为 16 threads、1 client、batch 64、warmup 最多 10 万 entries、
测量最多 100 万 entries 或 20 秒，JVM 为 `-Xms40g -Xmx40g`。100 万 entries
对应约 15,625 次完整 batch API，避免在计时前预生成 `1e9` 请求。首次运行先加 `--smoke`
做小规模功能验证。结果写入 `index-<RUN_ID>`。每项成功并记录结果后，脚本会
立即删除该项工作副本，再从 snapshot 恢复下一项；失败项则保留现场。调试时可
加 `--keep-work-dirs` 保留三个副本。每个副本的 RocksDB 和 SQLite 复制完成后
会立即输出
`snapshot_index_copy_completed=<path>/index`。Index suite 不需要 etcd、
MySQL、Metadata Server 或 S3。

正式性能对比无需重复全部 12 个同实现的 unique primary index。推荐测试
`item`（4B）、`stock`（8B）、`order`（12B）、`orderline`（16B）和
`history`（28B/default prefix），分别运行三种主索引操作；其余表只做 profile
与 smoke 验证。

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

正式测试推荐使用 runtime 自带的脚本，固定测试 ordered-only `orderline`：

```bash
bin/run-visibility-suite \
  --snapshot-dir /data/snapshots/node-a \
  --results-dir /data/retina-benchmark-results
```

脚本默认使用 16 threads、batch 1、10 秒/10 万行 warmup，并对最多
2990 万行执行最长 180 秒的正式测量。Warmup 与 Measurement 合计最多覆盖
`orderline` 全部 3000 万行；先到达 180 秒或行数上限即停止。每个成功删除都在
计时后用 `queryVisibility` 验证。首次使用可加 `--smoke`。

Visibility 只有 clean baseline：每个 phase 都只选择 readable layout 中生产标记的
`orderedPaths[0]`，根据 manifest footer 的 `recordNum` 调用
`addVisibility(fileId, rgId, recordNum, 0, null, false)`。compact、secondary 和
projection 路径不参与测试，也不读取或恢复任何 Visibility state。

每个 operation 删除一个唯一物理行。若 manifest 包含物理 Index 状态，删除 timestamp 为自动记录的 `snapshotTimestamp+1`；否则为 `1`。warmup 与 measurement 使用不相交的行；每个 phase 都重建同一 clean baseline，并在计时后用 `queryVisibility` 校验所有成功删除。GC 和 Storage GC 在场景内强制关闭。

`--clients` 只影响 runner 分片，不创建外部 client。吞吐并发度由 `--threads` 控制；需要精确 JNI 单次延迟时保持 `--batch-size 1`。

## 8. WriteBuffer Add

正式测试使用 runtime 自带的隔离脚本。MySQL 必须已经初始化且
`pixels.properties` 中的登录信息正确；etcd 和 Metadata/Node Server 不要手工启动，
脚本会在空闲端口上自动管理：

```bash
bin/run-write-buffer-suite \
  --snapshot-dir /data/snapshots/node-a \
  --s3-root s3://YOUR_BUCKET/retina-benchmark \
  --results-dir /data/retina-benchmark-results \
  --smoke
```

冒烟通过后去掉 `--smoke` 运行正式测试。默认固定使用 `orderline`、16 threads、
4 clients/vnodes、batch 64、30 秒/10 万行 warmup，以及 **180 秒**
measurement（仅时间截止，无实际行数上限）；服务端和客户端 JVM 均使用
`-Xms40g -Xmx40g`。
flush 参数为 10240 行/MemTable、20 个 MemTable/文件、4 个对象存储 flush
线程、30 秒定时 flush、encoding level 2、2 GiB block。

每轮隔离方式如下：

- etcd 使用新的 data dir 和 `initial-cluster-state=new`，成功后删除，失败时保留；
- Metadata Server 仍连接配置中的现有 MySQL catalog，但 benchmark 使用唯一逻辑
  schema，正常 close 时自动 drop，不读取生产表状态；
- warmup 和 measurement 分别创建独立 Metadata table、SQLite MainIndex 和
  `PixelsWriteBuffer`；
- S3 目标自动追加唯一 `RUN_ID`，成功后保留供检查，不会与其他测试共用；
- Snapshot 只读，提供 `orderline` 列 schema、类型、语义配置和初始描述，不恢复
  etcd、MySQL、MemTable 或生产 table ID；
- benchmark client 自动 preload 内置 `libjemalloc.so.2`，避免 JNI 与 glibc
  malloc 混用导致 native 崩溃。

脚本检查 Snapshot profile、benchmark 返回码、`errors=0`、成功行数、S3/flush
参数和异步 flush 错误；安装 AWS CLI 时还会确认唯一 prefix 下实际产生了对象。
可使用 `--dry-run` 查看最终参数，或通过端口参数避开其他服务。不要把
`--s3-root` 指向 Snapshot 的生产数据 prefix。

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
