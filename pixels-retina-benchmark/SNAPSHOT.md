# Retina SF100 benchmark 快照规范与使用说明

本文说明如何从一套已经构建好的 Pixels/TPC-H SF100 环境导出 benchmark 快照，以及 Index、Visibility、WriteBuffer Add 如何用该快照恢复可重复的初始状态。

快照用于模块级压测，不是数据库备份，也不会恢复完整 Retina 进程。Transaction Begin/Commit 仍使用独立的真实 Transaction Service RPC，不消费本快照。

## 1. 三类模块的快照边界

| 模块 | 从生产环境保留的状态 | 每轮如何恢复 | 不保留的状态 |
|---|---|---|---|
| Index | 原始 table/index ID、主键字节样本、整套 RocksDB 目录、整套 SQLite MainIndex 目录、Index 配置 | 先校验 SHA-256，再把 RocksDB/SQLite 复制到一次性工作目录；在副本上打开真实 RocksDB + SQLite | 生产 Metadata/Transaction 服务进程 |
| Visibility | 原始 file/RG ID 与 footer `recordNum`、`T_snap`、可选 GC/offload checkpoint、真实 RowLocation 样本、编译期 tile capacity | 每个 warmup/measurement phase 都重新执行 `CheckpointFileIO.readCheckpointParallel -> addVisibility`；有 checkpoint 时精确恢复 `T_snap` 的逻辑 bitmap，无 checkpoint 时按生产启动逻辑 clean-add | 历史 deletion chain、Java/C++ 对象地址、线程和 cache |
| WriteBuffer Add | 原 schema、列顺序/类型、真实行值样本、生产 writable layout/path 说明、WriteBuffer/Pixels writer/storage 配置 | 创建新的 Metadata schema/table、新 SQLite MainIndex、全新 `PixelsWriteBuffer`；默认每个 buffer 先执行一次不计时的真实 `addRow` | 活跃 MemTable、immutable MemTable、allocator 缓存、flush future、生产 table ID 和生产输出目录 |

WriteBuffer 的活跃进程状态不可安全序列化。复制一个正在写入的 MemTable 或把生产 SQLite 绑定到新建 benchmark table，都会产生错误的 rowId/tableId 语义。因此这里恢复的是“同一 schema、数据分布和配置下的全新真实 WriteBuffer”，不是生产进程的内存续跑。

## 2. 需要从现有 Pixels 导出的内容

### 2.1 全局一致性信息

- 一个权威事务高水位 `T_snap`。它必须由源 Transaction Service/CDC 调度系统提供，不能由导出器猜测；在暂停 CDC 并排空在途更新后确定，并且必须不小于快照内所有 Index/Visibility 变更时间戳。
- `sourceQuiesced=true`，表示 CDC、load、WriteBuffer、Index 更新、compaction、Visibility GC 和 Storage GC 均已停止产生新状态。
- 源 Retina host 和其 vnode 列表。`sourceVnodeIds` 只记录部署归属，不等于 Index bucket。
- 当前物理 RocksDB 中实际存在的 Index bucket/column family。导出器通常自动发现并写入 `sourceIndexBucketIds`。
- `retina.tile.visibility.capacity`。它同时决定 checkpoint bitmap 布局和 `libpixels-retina.so` 的编译期 `RETINA_CAPACITY`。

`T_snap` 不能仅凭文件修改时间推测。若物理 Index 中仍有晚于 `T_snap` 的版本，再用 `T_snap+1` 写 benchmark 版本会破坏版本顺序，因此必须先排空写入再选取高水位。当前仓库没有一个能同时证明“所有 CDC 已应用”和“所有异步 flush 已落稳”的单一管理 RPC；需要你们现有部署编排提供 drain 证据，并把最终高水位作为 `--snapshot-timestamp` 传入。Visibility/WriteBuffer 还要求后续 benchmark 时间戳能装入 native 48-bit 时间戳字段。

### 2.2 Metadata 与 Pixels footer 拓扑

每张表导出以下真实信息：

- schema/table ID、名称、类型和 storage scheme；
- 列 ID、ordinal、名称、类型、cardinality 和 null fraction；
- 所有 single-point index 的 index ID、scheme、unique/primary 属性、schema version、主键列顺序和固定 prefix/key 长度；`index-samples.bin` 只针对唯一的 primary index，零个或多个 primary index 都会拒绝采样；
- 所有 layout 的 ID、version、readable/writable 状态；
- ordered/compact/projection path 的 ID、URI、API ordinal；
- file ID、path/layout ID、文件类型、文件大小、rowId 范围；
- 从真实 Pixels footer 读取的 row-group 数量、每个 RG 的 `recordNum`、offset/length、pixel stride、压缩和文件版本；
- Retina 实际选取的最新 writable layout，以及第一个 ordered/compact path。

这些 ID 不能在 Index/Visibility 恢复时重新分配：`IndexKey` 内含 table/index ID，`RowLocation` 内含 file/RG ID。

### 2.3 真实数据样本

导出器在指定 readable layout 的 ordered 或 compact 文件中做等距系统采样，而不是构造占位值：

- `index-samples.bin`：使用 `TypeDescription.convertColumnVectorToByte` 得到真实主键字节，并保存 hidden commit timestamp、file ID、RG ID、RG row offset 和真实 SHA-256 bucket；
- `row-samples.bin`：保存全部业务列的 canonical `byte[][]`，包括真实 null；WriteBuffer 直接把它交给 `PixelsWriteBuffer.addRow`。

样本文件有 magic/version/count，manifest 对每个样本和物理状态文件保存 SHA-256。

### 2.4 Index 物理状态

需要从同一台、已经优雅停止的 Retina 节点复制：

- `index.rocksdb.data.path` 的整个目录；
- `index.sqlite.path` 的整个目录。

不能只复制目标表的几个 SST 或单个 SQLite 文件。RocksDB 可能是共享 DB，打开时需要所有已有 column family 的 descriptor；导出器会检查共享 RocksDB 中的每个 `tableId/indexId` 都能在 `--snapshot-tables` 中找到。

一台物理节点生成一个快照。不要把多台节点的 RocksDB 目录合并。

当前导出命令的一份物理快照只支持一个 `--snapshot-schema`。如果同一共享 RocksDB 还包含其他 schema 的 column family，必须拆分物理 DB 或扩展导出器后再使用；不能遗漏 descriptor，也不能把未描述的 CF 当作无关状态。

会写入 manifest 的 Index 参数包括 bucket 数、RocksDB/SQLite 路径，以及 write buffer、background flush/compaction、open files、block cache、block size、level/target file size、compression/compaction style、prefix length、subcompaction 和 stats 开关等 `index.rocksdb.*` 参数。恢复时数据目录改指向一次性副本，stats/log 输出也改到工作目录；生产进程、线程、已有 block cache 和 OS page cache 不属于快照。

### 2.5 Visibility checkpoint

精确 Visibility 初始状态需要在 `T_snap` 生成 GC 或 offload checkpoint。生产路径是：

```text
RetinaResourceManager.createCheckpoint(T_snap)
  -> RGVisibility.getVisibilityBitmap(T_snap)
  -> CheckpointFileIO.writeCheckpoint
```

也可以在暂停更新后通过真实客户端 `RetinaService.registerOffload(T_snap)` 发起 `RegisterOffload` RPC；该阻塞调用成功返回表示 checkpoint future 已完成。在复制完成前不要调用 `UnregisterOffload`。导出器接受该 checkpoint 的 Storage URI，并复制为固定路径 `state/visibility/gc-checkpoint.bin`。源文件或稳定临时 URI 必须保留生产 basename：`vis_gc_<host>_<timestamp>.bin` 或 `vis_offload_<host>_<timestamp>.bin`；导出器会解析文件名并强制 timestamp 等于 `T_snap`。

checkpoint 只保存 `T_snap` 时刻的 bitmap，不保存 GC 后被裁剪的完整 deletion chain。因此只有在 `T_snap` 之后不再发生任何 mutation、compaction、GC 或 Metadata/file 拓扑变化的前提下，它才代表精确的最终初始状态。含 checkpoint 的合法导出必须覆盖 `--snapshot-tables` 在生产启动路径中的全部 RG；覆盖不足会在导出阶段失败，不能静默退化为 clean baseline。

如果不提供 checkpoint，Visibility benchmark 仍可根据真实 file/RG/recordNum 构建 clean baseline，但这只保留 SF100 文件规模和行位置分布，不代表生产环境已有删除率。

### 2.6 WriteBuffer 配置

以下值会写入 manifest，并在 snapshot WriteBuffer 初始化前恢复；命令行参数可以显式覆盖：

- buffer：`retina.buffer.memTable.size`、`retina.buffer.flush.count`、`retina.buffer.object.flush.threads`、`retina.buffer.flush.interval`；
- writer：`retina.buffer.flush.encodingLevel`、`retina.buffer.flush.nullsPadding`、`block.size`、`block.replication`；
- object：`retina.buffer.object.storage.scheme/folder`；
- topology：`node.virtual.num`、`index.main.cache.bucket.num`；
- storage provider：`enabled.storage.schemes`、HDFS config path、S3/MinIO timeout/concurrency/endpoint；
- Pixels layout/encoding：`pixel.stride`、`row.group.size`、column alignment/endian 配置；
- Visibility：`retina.tile.visibility.capacity`。

访问凭证、MySQL 密码、生产对象内容和生产 Metadata 数据库不会写进快照。目标机仍通过自己的 `PIXELS_CONFIG` 提供 Metadata/etcd endpoint、数据库凭证和云存储凭证。

> 安全边界：快照虽然不包含服务密码，但会包含真实全列行值、主键以及完整 RocksDB/SQLite，必须按生产数据分级管理。使用加密传输和加密磁盘、限制 ACL、设置保留期并在测试后销毁；不要提交到 Git 仓库。SHA-256 只用于发现传输损坏，不提供来源认证。WriteBuffer 目标环境还应使用独立账号、bucket/filesystem 和最小权限凭证；URI 字符串冲突检查不能识别 symlink、挂载别名或两个 endpoint alias 实际指向同一后端。

## 3. 快照目录格式

```text
sf100-node-a.snapshot/
  snapshot.json
  tables/
    tpch_lineitem-t<TABLE_ID>/
      index-samples.bin
      row-samples.bin
    ...
  state/
    index/
      rocksdb/
        ... entire source RocksDB ...
      sqlite/
        <TABLE_ID>.main.index.db
        ... entire source SQLite directory ...
    visibility/
      gc-checkpoint.bin
```

`state/index` 和 `state/visibility` 都是可选项；相应 benchmark 会在缺少其必需状态时给出明确错误。`snapshot.json` 最后写入，所以没有 manifest 的目录应视为失败的导出，不应传给 benchmark。

## 4. 一致的导出顺序

下面的停写/停服命令取决于你们的部署系统，因此导出脚本不会自行 kill 生产进程。

1. 确定本节点共享 RocksDB 中涉及的全部 indexed tables。TPC-H 通常从 `customer,lineitem,nation,orders,part,partsupp,region,supplier` 开始核对，而不是只填本次想测的一张表。
2. 阻止新的 CDC/update/load/compaction 进入，等待所有 Retina RPC、WriteBuffer flush 和 Index update 完成。必须显式 drain/close 每个 `PixelsWriteBuffer`，并确认 Metadata 中没有未完成的 `TEMPORARY` 文件；当前普通 `RetinaServer.shutdown()` 只停止 gRPC，不能替代这一步。
3. 停止周期性 Visibility GC 与 Storage GC，记录不小于所有已完成更新的事务高水位 `T_snap`。
4. 在 Retina 仍存活但已经无写入时生成 `T_snap` checkpoint，并确认 checkpoint 写完。可以先把它复制到稳定的临时 URI。
5. 在已经完成第 2 步显式 drain 的前提下，优雅停止拥有 RocksDB/SQLite 的 Retina 进程。不要使用 `SIGKILL`，也不要在线复制仍打开的 RocksDB/SQLite 目录。
6. 保持 Metadata Service 与源 Pixels 文件存储可读，运行下面的导出器。导出器需要它们读取真实 IDs、footer 和样本，但不会修改源表。
7. 把整个输出目录复制到压测机，执行 `snapshot-validate`。

从第 2 步开始，源表必须持续保持只读，直到第 6 步成功输出 `snapshot.json`；这段时间任何 CDC、load、compaction、GC 或 Metadata/file 拓扑变化都会使本轮 checkpoint 和输出作废。导出完成后，先保留快照与 checkpoint，再按你们部署的正常顺序恢复 Retina/GC，最后恢复 mutation 入口；确认服务健康后才可 `UnregisterOffload(T_snap)`。生产侧具体的 drain、TEMPORARY 文件查询和启停命令因部署系统而异，必须把“在途 mutation=0、flush/index update=0、TEMPORARY=0、进程正常退出”作为完成判据，不能只依赖固定等待时间。

## 5. 导出命令

在含 fat JAR 的 runtime 目录中，只在导出进程里让 `SOURCE_PIXELS_CONFIG` 指向现有 Pixels 部署的只读客户端配置。不要在后续 benchmark shell 中继续导出这个变量：

```bash
export SOURCE_PIXELS_CONFIG=/opt/pixels/etc/pixels.properties
export T_SNAP='REPLACE_WITH_CONFIRMED_SOURCE_HIGH_WATERMARK'
PIXELS_CONFIG="$SOURCE_PIXELS_CONFIG" bin/export-snapshot \
  --snapshot-output /data/snapshots/tpch-sf100-node-a \
  --snapshot-schema tpch \
  --snapshot-tables customer,lineitem,nation,orders,part,partsupp,region,supplier \
  --snapshot-timestamp "$T_SNAP" \
  --snapshot-source-host node-a \
  --snapshot-retina-vnodes 0,1,2,3 \
  --snapshot-index-samples 12000000 \
  --snapshot-row-samples 100000 \
  --snapshot-sample-layout ordered \
  --snapshot-source-quiesced true \
  --snapshot-copy-index-state true \
  --snapshot-rocksdb-dir /data/pixels/index/rocksdb \
  --snapshot-sqlite-dir /data/pixels/index/sqlite \
  --snapshot-visibility-checkpoint "file:///data/checkpoints/vis_offload_node-a_${T_SNAP}.bin"
```

`--snapshot-source-host` 应填写拥有这份 RocksDB/SQLite 的 Retina host，默认才是运行导出器机器的 hostname；`--snapshot-retina-vnodes` 只是人工记录该节点归属。不要靠改写 `snapshot.json` 更正 provenance，否则 checksum/审计链失去意义。

主要参数：

| 参数 | 必需性 | 含义 |
|---|---|---|
| `--snapshot-output` | 必需 | 新目录或空目录；失败输出不要复用 |
| `--snapshot-schema` | 必需 | 源 schema 名 |
| `--snapshot-tables` | 必需 | 逗号分隔；物理 RocksDB 导出时必须覆盖其中所有 index descriptor |
| `--snapshot-timestamp` | Index 样本/checkpoint 必需 | 权威 `T_snap`，必须为正数 |
| `--snapshot-index-samples` | 建议 | 每张表导出的全表系统样本数；默认 0 |
| `--snapshot-row-samples` | WriteBuffer 必需 | 每张表真实行样本数；默认 0 |
| `--snapshot-sample-layout` | 可选 | `ordered` 或 `compact`，默认 `ordered` |
| `--snapshot-layout-id` | 可选 | 指定 readable layout；默认选 ID 最大的 readable layout |
| `--snapshot-source-host` | 可选 | 源 Retina host；默认 exporter hostname |
| `--snapshot-retina-vnodes` | 可选 | 源 Retina vnode，仅作 provenance |
| `--snapshot-index-buckets` | 可选 | 物理 Index bucket；通常由 RocksDB CF 自动发现 |
| `--snapshot-source-quiesced` | 物理状态必需 | 必须显式为 `true` |
| `--snapshot-copy-index-state` | 可选 | 是否复制完整 RocksDB/SQLite |
| `--snapshot-rocksdb-dir` | 可选 | 默认读取 `index.rocksdb.data.path` |
| `--snapshot-sqlite-dir` | 可选 | 默认读取 `index.sqlite.path` |
| `--snapshot-visibility-checkpoint` | 可选 | source Storage URI；提供后复制并解析校验 |

导出器会交叉检查 Metadata 与 footer 的 file/RG 数量、文件读取前后长度、checkpoint 文件名中的 type/timestamp、`recordNum/bitmap`、目标生产 RG 的 100% 覆盖率、主键字节长度、bucket 计算以及共享 RocksDB column family descriptor。成功时最后输出 `status=exported` 并以 0 退出；只有这时才可把目录视为完整快照。`sourceQuiesced` 本身只是操作员声明，程序无法替你证明上游已经停写。

## 6. 传输后校验

```bash
bin/run-benchmark snapshot-validate \
  --snapshot-dir /data/snapshots/tpch-sf100-node-a \
  --snapshot-table lineitem \
  --snapshot-require index,visibility,write-buffer
```

该命令检查格式版本、ID/拓扑引用、完整扫描样本二进制及其计数，并校验所有已声明 artifact 的 SHA-256。`--snapshot-require` 可取 `index`、`visibility`、`visibility-clean`、`write-buffer`：其中 `visibility` 强制要求 checkpoint，`visibility-clean` 只要求 footer 拓扑。成功时输出 `status=valid` 并以 0 退出；checkpoint 的深层 native 恢复校验还会在 Visibility setup 再执行一次。它不能证明源端真的停写，也不能防止 manifest 与 artifact 一起被恶意替换。任何校验失败都应阻止压测；不要手工修改 `snapshot.json` 来绕过错误。

## 7. 从快照运行三个 benchmark

### 7.1 Index

```bash
bin/run-benchmark index \
  --snapshot-dir /data/snapshots/tpch-sf100-node-a \
  --snapshot-table lineitem \
  --threads 32 --clients 1 \
  --warmup-seconds 10 --warmup-operations 100000 \
  --duration-seconds 60 --data-size 1000000 --batch-size 64
```

恢复器先将物理 Index 状态复制到临时目录。setup 使用 `lookupUniqueIndex(key,T_snap)` 剔除 tombstone/不可见样本，并用实际返回的当前 `RowLocation` 替换文件样本中的旧位置；这些 lookup 和 cache warmup 不计时。计时区只执行：

```text
LocalIndexService.updatePrimaryIndexEntries
  -> RocksDBIndex.updatePrimaryEntries
  -> SqliteMainIndex.getLocations/putEntries
```

可选 `--snapshot-work-dir /data/retina-work/index-run-001` 保留工作副本供检查；该目录必须尚不存在或为空，并且每轮使用新目录。不指定时使用临时目录并在退出时自动删除。`--snapshot-index-target-files` 和 `--snapshot-index-rows-per-rg` 只决定新版本的语义化目标 RowLocation，不创建真实 Pixels 文件。

### 7.2 Visibility

```bash
bin/run-benchmark visibility \
  --snapshot-dir /data/snapshots/tpch-sf100-node-a \
  --snapshot-table lineitem \
  --threads 32 --clients 1 \
  --warmup-seconds 10 --warmup-operations 100000 \
  --duration-seconds 60 --data-size 1000000 --batch-size 1
```

恢复范围与生产 `RetinaServerImpl` 初始化一致：每个 readable layout 的第一个 ordered/compact path，projection 排除。含 checkpoint 的快照精确恢复所有覆盖 RG 在 `T_snap` 的逻辑 visibility bitmap，但不恢复历史 deletion chain 或进程内部状态；无 checkpoint 时所有 RG 按 footer clean-add。每个 phase 使用同一初态但不重用 warmup/measurement 的物理行，计时区只执行 `RetinaResourceManager.deleteRecord(RowLocation,T_snap+1)`，结束后用 `queryVisibility` 验证成功删除。

Visibility 和下面的 WriteBuffer snapshot 命令都只支持兼容的 64-bit Linux/JNI。启动时会读取 native `RETINA_CAPACITY` 并与 snapshot 配置强校验；不一致时必须用源部署相同的 `retina.tile.visibility.capacity` 重新构建 fat JAR。

### 7.3 WriteBuffer Add

先生成一份只连接隔离压测环境的配置。必须逐项确认 Metadata、etcd、MySQL 以及 S3/HDFS endpoint 都不是生产地址；不要复用上面的 `SOURCE_PIXELS_CONFIG`：

```bash
export BENCH_PIXELS_CONFIG="$PWD/etc/pixels.properties"
export PIXELS_CONFIG="$BENCH_PIXELS_CONFIG"
```

终端 A 启动 runtime 自带的最小依赖；`start-metadata-server` 是前台进程，看到 Metadata/Node server ready 日志后再继续：

```bash
docker compose up -d --wait etcd mysql
bin/start-metadata-server
```

终端 B 使用每轮唯一的输出路径运行 localfs 示例：

```bash
export PIXELS_CONFIG="$BENCH_PIXELS_CONFIG"
export RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"
bin/run-benchmark write-buffer-add \
  --snapshot-dir /data/snapshots/tpch-sf100-node-a \
  --snapshot-table lineitem \
  --threads 32 --clients 4 \
  --warmup-seconds 10 --duration-seconds 60 \
  --data-size 1000000 --batch-size 64 \
  --writebuffer-storage-scheme file \
  --writebuffer-base-uri "file:///data/retina-bench/${RUN_ID}/lineitem" \
  --writebuffer-object-storage-scheme file \
  --writebuffer-object-folder "/data/retina-bench/${RUN_ID}/objects" \
  --writebuffer-row-pool-size 100000
```

S3/MinIO/HDFS 模式必须显式给出专用 benchmark prefix，例如：

```bash
  --writebuffer-storage-scheme s3 \
  --writebuffer-base-uri s3://bench-bucket/run-001/table \
  --writebuffer-object-storage-scheme s3 \
  --writebuffer-object-folder s3://bench-bucket/run-001/objects \
  --writebuffer-enabled-storage-schemes s3,file
```

程序拒绝与 manifest 中生产 ordered/compact/object 路径相同、互为父子目录的目标。snapshot localfs 模式未指定目标时会使用一次性工作目录；非 file 模式必须显式指定 base URI 和 object folder。目标 prefix 必须新建且为空，并使用独立账号/bucket/filesystem；程序的 URI 比较不是生产隔离证明。每轮结束后停止前台 Metadata Server，再执行 `docker compose down`；确认输出仅属于该 run 后，按你们的数据保留策略清理 schema、SQLite、Pixels 文件和 immutable objects。

WriteBuffer 可覆盖参数：

| 参数 | 作用 |
|---|---|
| `--writebuffer-row-pool-size` | 读取多少真实行样本；值可循环复用，rowId/location/timestamp 不复用 |
| `--writebuffer-memtable-size` | `retina.buffer.memTable.size`，必须为 64 的正整数倍 |
| `--writebuffer-flush-count` | 每个 file 的 MemTable 数阈值 |
| `--writebuffer-flush-threads` | object flush executor 线程数 |
| `--writebuffer-flush-interval` | file flush scheduler 间隔，秒 |
| `--writebuffer-encoding-level` | 0、1 或 2 |
| `--writebuffer-nulls-padding` | writer null padding |
| `--writebuffer-block-size` | Pixels writer block size |
| `--writebuffer-replication` | storage replication |
| `--writebuffer-vnodes` | `node.virtual.num`，必须不小于 clients |
| `--writebuffer-enabled-storage-schemes` | 可用 storage provider 列表 |
| `--writebuffer-storage-scheme` | 新 Metadata table 的 storage scheme |
| `--writebuffer-base-uri` | 新 table 的专用 writable base URI |
| `--writebuffer-object-storage-scheme` | immutable MemTable object storage scheme |
| `--writebuffer-object-folder` | immutable MemTable object 专用目录/prefix |
| `--writebuffer-seed-buffers` | 默认 snapshot 模式为 true；每个 client 在计时外先执行一次真实 addRow |
| `--writebuffer-schema` | 新建 Metadata schema 名；默认随机唯一 |
| `--writebuffer-host-name` | 生成 Pixels 文件名的 host 部分 |
| `--writebuffer-drop-schema` | 结束后是否删除本次 schema；默认 false |

目标 Metadata/etcd 是运行必需依赖，因为真实 `PixelsWriteBuffer` 构造会注册文件，并通过 SQLite `RowIdAllocator/PersistentAutoIncrement` 从 etcd 分配 rowId 段。它们不是快照中的被测状态。

`sourceVnodeIds` 只用于记录源节点归属；与生产 `RetinaResourceManager.addWriteBuffer` 一样，fresh fixture 会为 `clients` 个 buffer 使用本地 vnode `0..clients-1`。如果要复现生产每表的完整 buffer 数量，应令 `--clients` 等于源 `node.virtual.num`，并使 `--writebuffer-vnodes` 保持同一值。

## 8. SF100 样本规模怎么选

### Index

Index 至少需要 `warmup-operations + data-size` 个在 `T_snap` 有效且不重复、并且属于该物理节点 bucket 的 key。因为导出的是全表系统样本，可用下面的近似式估算：

```text
N_export >= (N_warmup + N_measurement) / (bucket_coverage * active_key_ratio) * safety_factor
```

例如节点持有 128 个 bucket 中的 16 个、估计 95% 样本仍 active、取 1.2 安全系数，那么一百万 measurement 加十万 warmup 约需导出 `1.1M / (16/128*0.95) * 1.2 ≈ 11.1M` 个全表样本。setup 会报告过滤掉的 duplicate、inactive 和 relocated 数量；不足时直接失败，不会回绕重复 key。

大样本导出会在生成二进制前暂存采样目标和结果，需要数 GiB 级 JVM heap；按实际样本量设置例如 `JAVA_OPTS="-Xms8g -Xmx8g"`，并监控后再放大。校验和 benchmark 恢复采用流式扫描，不会把整个 index sample 文件一次性载入内存，但仍需为 `warmup + measurement` 个 active key 预留 heap。

### Visibility

Visibility 需要相同数量的唯一、在 checkpoint 中尚未删除的物理行。程序优先使用真实 Index sample 的 RowLocation，不足部分按真实 footer topology 在 RG 间 round-robin 选择，因此无需为每个待删除行都导出 key。初始数据规模、RG 数、每 RG 行数和 checkpoint 删除率都会影响缓存、native 对象数量和吞吐，应保留完整 SF100 topology。

### WriteBuffer

WriteBuffer 的初始性能主要由 schema 宽度/null/变长值分布、MemTable/flush 配置和存储后端决定，而不是预先存在多少历史行。建议至少导出 10 万条 row samples；样本值可以循环，但每个 driver batch 使用新的事务时间戳，batch 内各行共用该时间戳，而 rowId/RowLocation 逐行唯一。`data-size` 决定本轮新状态增长量，需为 `.pxl` 文件、中间 object、Metadata 记录和 SQLite 预留空间。

## 9. 重置、可重复性和限制

- 原始 snapshot 只读。Index 每个 JVM 都从它复制新工作目录；同一 JVM 的 warmup 会先写工作副本，measurement 使用不相交 key 继续测试，因此若要求 measurement 前再次物理复位，应禁用 warmup并分两次运行。Visibility 每个 phase 重建 JNI 对象；WriteBuffer 每个 phase 新建 table/buffer。
- 进程内预热会保留 JIT、RocksDB block cache 和 OS page cache，这是 warmup 的目的。测 cold state 时禁用 warmup并使用新 JVM/清理 OS cache策略。
- Index 工作副本会增加 MVCC version 和 SQLite row ranges；WriteBuffer 会增加 Metadata、对象和 Pixels 文件。不要把同一 `--snapshot-work-dir` 重复用于多轮。
- 源 Pixels 文件 payload 不复制进快照。导出时需要可读，恢复后的 Index/Visibility/WB benchmark 只使用已导出的 footer、样本和物理状态。
- manifest 不包含生产数据库/etcd dump。Index/Visibility 保留原始逻辑 ID；WriteBuffer 则有意创建新 ID，因此把生产 Metadata dump 恢复到 benchmark 环境既不需要也不安全。
- snapshot format 当前为 version 1。代码会拒绝未知版本、路径逃逸、symlink、缺失文件和 checksum 不一致。
- 这套快照只服务 Index、Visibility 和 WriteBuffer 的独立模块上限测试，不能替代端到端 CDC 一致性测试。

## 10. 构建与 native capacity

运行 Visibility 或 WriteBuffer 的 fat JAR 必须用与源部署相同的 tile capacity 构建。在与目标服务器 ABI 兼容的 64-bit Linux 构建机上，从仓库根目录执行；不要复制含生产凭证的完整配置，只从 benchmark 默认配置生成脱敏构建配置并修改 capacity：

```bash
export PIXELS_HOME=/tmp/pixels-retina-benchmark-build
install -d -m 0700 "$PIXELS_HOME/etc" "$PIXELS_HOME/lib"
cp pixels-retina-benchmark/deploy/etc/pixels.properties "$PIXELS_HOME/etc/pixels.properties"
# 把下行值改成 snapshot.json/effectiveConfig 中的源值
sed -i 's/^retina.tile.visibility.capacity=.*/retina.tile.visibility.capacity=65536/' \
  "$PIXELS_HOME/etc/pixels.properties"

mvn -T 3 -pl pixels-retina-benchmark -am -DskipTests package
```

打包完成并保存产物后删除这个临时 `PIXELS_HOME`。`65536` 只是命令格式示例，不是 SF100 固定值；运行时仍会用 JNI `getNativeTileCapacity()` 与 manifest 强校验。

构建产物与普通 benchmark 相同：

```text
pixels-retina-benchmark/target/pixels-retina-benchmark-0.2.0-SNAPSHOT-full.jar
pixels-retina-benchmark/target/pixels-retina-benchmark-0.2.0-SNAPSHOT-runtime.tar.gz
```

runtime tarball 已包含本文、Linux JNI，以及正文依赖的 `bin/export-snapshot`、`bin/run-benchmark`、`bin/start-metadata-server`、`bin/start-transaction-server` 和 `bin/stop-transaction-server`；目标服务器不需要安装完整 Pixels。

## 11. 术语边界

- `RG`（row group）：Pixels 文件内的行组；Visibility 以 `(fileId, rgId)` 管理 bitmap。
- `clean-add`：只按 footer 的 `recordNum` 新建“初始全部可见”的 RG，不带生产删除状态。
- `deletion chain`：Visibility 内按事务时间组织的历史删除版本；checkpoint format v1 只保存某一时刻 bitmap。
- hidden commit timestamp：Pixels 文件里随行保存、但不属于业务 schema 的提交时间戳。
- canonical `byte[][]`：由 Pixels `TypeDescription` 按真实列类型转换的逐列字节值，null 保持为 null。
- `TEMPORARY` file：Pixels writer/GC 尚未原子提升为 `REGULAR` 的 catalog 文件；存在时说明写入或替换尚未完成。
- bucket coverage：此物理节点拥有的 Index bucket 占全局 `index.bucket.num` 的比例；它直接决定全表样本中可用于本节点的 key 比例。
