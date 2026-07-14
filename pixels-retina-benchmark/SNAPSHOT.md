# Retina Benchmark Snapshot v2

Snapshot v2 为 Index、Visibility 和 WriteBuffer 独立基准保存可校验输入。它不是 Pixels 数据库备份，也不能恢复完整 Retina 进程。

导出器读取 Metadata 和 Pixels footer；仅在要求复制物理 Index 状态时读取源 etcd 并复制已关闭的 RocksDB/SQLite。快照只包含：

- `snapshot.json` manifest；
- 可选 `state/index`：停写后复制的完整 RocksDB 和 SQLite。

快照不包含 Visibility state、行 payload、MySQL dump、Pixels `.pxl` 文件或 Transaction 状态。Visibility 从 footer 构造 clean baseline；WriteBuffer 从 manifest 恢复 schema 和语义配置，并生成确定性的合法 benchmark 行。

## 1. v2 目录结构

```text
node-a.snapshot/
  snapshot.json
  state/
    index/
      rocksdb/
        ... complete source RocksDB directory ...
      sqlite/
        <TABLE_ID>.main.index.db
        ... complete source SQLite directory ...
```

`state/index` 是唯一可选 state 目录：

- 六个 Index 命令要求完整 `state/index`；
- Visibility 只使用 manifest footer 拓扑构造 clean baseline；
- WriteBuffer 不读取 state 目录。

`snapshot.json` 最后写入。没有 manifest 的输出目录属于失败导出。manifest 的 `artifactsSha256` 保存所有复制 artifact 的 SHA-256；原始 snapshot 必须只读。

## 2. Manifest 内容

### 2.1 来源与一致性

manifest 记录：

- `formatVersion=2`、创建时间和源 Retina host；
- 源 host 的 vnode provenance；
- 物理 RocksDB 中发现的 bucket/column family；
- 操作员声明的 `sourceQuiesced`；
- 是否包含物理 Index 状态；
- 复制物理 Index 状态时，由源 etcd `trans_high_watermark` 自动计算的 `snapshotTimestamp=HWM-1`；不复制时为 `0`；
- 所有 artifact checksum。

用户不提供 snapshot timestamp。只有 `--snapshot-copy-index-state true` 才会访问源 etcd 并计算 `HWM-1`；因此必须先停写，并让仍在运行的 Transaction Service 至少完成一次固定 10 秒周期的 watermark checkpoint，确保 etcd 中的 `trans_high_watermark` 已反映停写后的状态。

### 2.2 Metadata 与 footer

每张表记录：

- schema/table ID、名称、类型、storage scheme、Metadata row count；
- 列 ID、ordinal、名称、类型、cardinality、null fraction；
- primary 和 secondary index descriptor，包括 index ID、scheme、unique/primary、schema version、key columns、canonical key length 和 RocksDB prefix length；
- layout ID/version/permission/readable/writable；
- ordered、compact、projection path 及 Retina 实际选择的第一个 ordered/compact path；
- file ID、path/layout、类型、大小、rowId 范围和完整 URI；
- footer row count、row-group 数、每个 RG 的 `recordNum`、data length、footer offset/length、pixel stride、compression 和 file version；
- 最新 writable layout 及其生产 ordered/compact URI。

导出器会在读取 footer 前后比较文件长度，并交叉校验 Metadata/file ID、Metadata RG 数、footer RG 数和 RG row count。它只读取 `.pxl` footer，不把源文件复制进快照。

这些逻辑 ID 在基准中必须保持不变，因为 `IndexKey` 包含 table/index ID，Visibility 使用的 `RowLocation` 包含 file/RG ID。

### 2.3 语义配置

manifest 只保存影响持久状态或 benchmark 语义的配置，例如：

- Index bucket、cache 与 `index.rocksdb.*`；
- `retina.tile.visibility.capacity`；
- WriteBuffer memtable/flush/object 参数；
- Pixels writer block、replication、row group、pixel stride、alignment/endian；
- storage scheme 列表和 vnode/MainIndex cache 参数。

endpoint、MySQL 用户/密码、etcd 地址、云凭证和源本地绝对状态路径不会写入 manifest。目标环境必须通过自己的 `PIXELS_CONFIG` 提供服务连接和凭证。

### 2.4 导出进程配置

导出进程沿用 `ConfigFactory` 的配置优先级：

1. 已设置 `PIXELS_CONFIG` 时读取它；
2. 否则读取 `PIXELS_HOME/etc/pixels.properties`。

`bin/run-benchmark` 的 fat JAR 和 log4j 配置始终从脚本自身的 `ROOT_DIR` 加载，不随 `PIXELS_HOME` 改变。因此导出时可让 `PIXELS_HOME` 直接指向源 Pixels 部署；如环境中已有 `PIXELS_CONFIG`，它仍会优先于 `PIXELS_HOME`。

## 3. 物理状态边界

### 3.1 Index

必须从同一个已优雅停止的 Retina 节点复制：

- `index.rocksdb.data.path` 整个目录；
- `index.sqlite.path` 整个目录。

不能在线复制仍打开的 RocksDB/SQLite，也不能只复制部分 SST、单个 column family 或目标表的一个 SQLite 文件。共享 RocksDB 打开时需要完整 column-family descriptor；导出器会发现所有 CF，并验证它们都能由本次 `--snapshot-tables` 中的 index descriptor 解释。

一份物理快照只描述一个 `--snapshot-schema` 和一台物理节点。共享 RocksDB 若还包含其他 schema 的 index，当前导出必须先调整物理边界，使所有 CF 都能由该 schema 的导出表集合完整描述。

恢复时不会打开原始目录。每个 Index JVM 先将完整 RocksDB/SQLite 复制到新的工作目录，再把 stats/log 和数据库路径指向该副本。

### 3.2 Visibility

Visibility 只有 clean baseline。每个 phase 根据 manifest 中生产选择路径上的 file/RG footer `recordNum` 调用：

```text
addVisibility(fileId, rgId, recordNum, 0, null, false)
```

它不导出、读取或恢复源端 Visibility bitmap，也不创建 `state/visibility`。该场景测量相同物理拓扑上的 delete/JNI，不表示源端已有删除分布。

### 3.3 WriteBuffer

活跃 MemTable、immutable MemTable、allocator 缓存、flush future 和生产 table ID 不属于可复制状态。WriteBuffer 恢复会：

1. 读取 manifest 的 schema 和语义配置；
2. 在 benchmark Metadata 中创建新 schema/table；
3. 创建新的 SQLite MainIndex；
4. 创建全新 `PixelsWriteBuffer`；
5. 按列类型生成确定性 benchmark 行；
6. 写入专属 benchmark 路径。

目标 Metadata、etcd 和存储均是运行依赖，不是快照内容。

## 4. 一致导出流程

从停写开始到 `snapshot.json` 写完，源 Metadata、Pixels 文件、RocksDB 和 SQLite 必须代表同一稳定状态。

1. 盘点该节点共享 RocksDB 中的全部 indexed tables 和 column families，形成完整 `--snapshot-tables`。
2. 阻止新的 CDC、update、load 和 compaction；停止 Visibility GC 与 Storage GC。
3. drain/close 每个 `PixelsWriteBuffer`，等待 Index update、异步 flush 和文件提升完成；确认在途 mutation 为零，且 Metadata 中没有未完成的 `TEMPORARY` 文件。
4. 保持 Transaction Service 运行，并在停写后等待它至少完成一次 10 秒 watermark checkpoint，使 etcd 中的 `trans_high_watermark` 更新到稳定值。
5. 优雅停止拥有 RocksDB/SQLite 的 Retina 进程。普通 gRPC shutdown 不等于 WriteBuffer drain；不要使用 `SIGKILL`。
6. 保持源 etcd、Metadata Service 和源 Pixels storage 可达；以 `--snapshot-source-quiesced true --snapshot-copy-index-state true` 运行导出器。导出器读取 catalog/footer 和 etcd，并复制已经关闭的物理目录。
7. 将输出传输到压测机，立即运行 `snapshot-validate`。任何失败都应阻止压测。
8. 验证并保存快照后，按部署正常顺序恢复源 Retina/GC，最后恢复 mutation 入口。

导出脚本不会停止生产服务，也不能证明 `sourceQuiesced=true`。部署侧必须以“在途 mutation=0、flush/index update=0、TEMPORARY=0、RocksDB/SQLite 已正常关闭”为完成判据。

## 5. 导出命令

导出前，用户必须在当前环境中将 `PIXELS_HOME` 设置为源 Pixels 部署；未设置时脚本会直接报错退出。确认 `PIXELS_CONFIG` 未错误指向其他环境后，在解压后的 benchmark runtime 目录执行：

```bash
bin/export-snapshot \
  --snapshot-output /data/snapshots/tpch-sf100-node-a \
  --snapshot-schema tpch \
  --snapshot-tables customer,lineitem,nation,orders,part,partsupp,region,supplier \
  --snapshot-source-host node-a \
  --snapshot-retina-vnodes 0,1,2,3 \
  --snapshot-source-quiesced true \
  --snapshot-copy-index-state true
```

默认从 `$PIXELS_HOME/etc/pixels.properties` 读取 Metadata、etcd、storage 和 Index 路径。若环境中设置了 `PIXELS_CONFIG`，它的优先级高于 `PIXELS_HOME`，因此必须确保它同样指向源部署。

必需参数：

- `--snapshot-output`：新目录或空目录；
- `--snapshot-schema`：源 schema；
- `--snapshot-tables`：逗号分隔表名。

来源与物理状态参数：

- `--snapshot-source-host`：拥有该 RocksDB/SQLite 的 Retina host，默认 exporter hostname；
- `--snapshot-retina-vnodes`：源 vnode provenance；
- `--snapshot-index-buckets`：通常无需设置，由 RocksDB CF 自动发现；
- `--snapshot-source-quiesced true`：复制物理 Index 状态时必须显式设置；
- `--snapshot-copy-index-state true`：复制完整 RocksDB/SQLite，并从源 etcd 读取 `trans_high_watermark`，记录 `snapshotTimestamp=HWM-1`；
- `--snapshot-rocksdb-dir`：默认读取源配置的 `index.rocksdb.data.path`；
- `--snapshot-sqlite-dir`：默认读取源配置的 `index.sqlite.path`。

成功导出最后输出 `status=exported` 并以 0 退出。失败目录不要复用或手工补写 manifest。

## 6. 校验

传输后先做通用校验：

```bash
bin/run-benchmark snapshot-validate \
  --snapshot-dir /data/snapshots/tpch-sf100-node-a
```

校验器检查 v2 manifest 语义、ID/路径引用、声明 artifact、symlink/路径逃逸和 SHA-256。

按模块增加 profile：

```bash
# 六个物理 Index 命令
bin/run-benchmark snapshot-validate \
  --snapshot-dir /data/snapshots/tpch-sf100-node-a \
  --snapshot-table lineitem \
  --snapshot-require index

# Visibility clean baseline 所需的 footer 拓扑
bin/run-benchmark snapshot-validate \
  --snapshot-dir /data/snapshots/tpch-sf100-node-a \
  --snapshot-table lineitem \
  --snapshot-require visibility

# WriteBuffer schema 与语义配置
bin/run-benchmark snapshot-validate \
  --snapshot-dir /data/snapshots/tpch-sf100-node-a \
  --snapshot-table lineitem \
  --snapshot-require write-buffer
```

校验 profile 仅支持 `index`、`visibility`、`write-buffer`，也可用逗号组合。成功输出 `status=valid`。SHA-256 只能发现传输损坏，不能提供来源认证；快照应使用受控传输、ACL 和加密存储。

## 7. 各模块恢复与运行

### 7.1 Primary Index

```bash
bin/run-benchmark index-put-primary \
  --snapshot-dir /data/snapshots/tpch-sf100-node-a \
  --snapshot-table lineitem \
  --threads 32 --clients 1 --batch-size 64 \
  --warmup-seconds 10 --duration-seconds 60 --data-size 1000000

bin/run-benchmark index-update-primary \
  --snapshot-dir /data/snapshots/tpch-sf100-node-a \
  --snapshot-table lineitem \
  --threads 32 --clients 1 --batch-size 64 \
  --warmup-seconds 10 --duration-seconds 60 --data-size 1000000

bin/run-benchmark index-delete-primary \
  --snapshot-dir /data/snapshots/tpch-sf100-node-a \
  --snapshot-table lineitem \
  --threads 32 --clients 1 --batch-size 64 \
  --warmup-seconds 10 --duration-seconds 60 --data-size 1000000
```

恢复器将 `state/index/rocksdb` 和 `state/index/sqlite` 完整复制到一次性工作目录，再打开真实 RocksDB + SQLite。测试使用确定性生成且已检查不存在的 key；Update/Delete 的旧版本 fixture 在计时外通过真实 put 创建。

可用 `--snapshot-work-dir /data/retina-work/index-run-001` 保留工作副本。目录必须不存在或为空，每轮必须使用新目录。

### 7.2 Secondary Index

```bash
bin/run-benchmark index-put-secondary \
  --snapshot-dir /data/snapshots/tpch-sf100-node-a \
  --snapshot-table lineitem \
  --snapshot-secondary-index SECONDARY_INDEX_ID_OR_KEY_COLUMNS \
  --threads 32 --clients 1 --batch-size 64 \
  --warmup-seconds 10 --duration-seconds 60 --data-size 1000000

bin/run-benchmark index-update-secondary \
  --snapshot-dir /data/snapshots/tpch-sf100-node-a \
  --snapshot-table lineitem \
  --snapshot-secondary-index SECONDARY_INDEX_ID_OR_KEY_COLUMNS \
  --threads 32 --clients 1 --batch-size 64 \
  --warmup-seconds 10 --duration-seconds 60 --data-size 1000000

bin/run-benchmark index-delete-secondary \
  --snapshot-dir /data/snapshots/tpch-sf100-node-a \
  --snapshot-table lineitem \
  --snapshot-secondary-index SECONDARY_INDEX_ID_OR_KEY_COLUMNS \
  --threads 32 --clients 1 --batch-size 64 \
  --warmup-seconds 10 --duration-seconds 60 --data-size 1000000
```

Secondary selector 必须按 ID、单列名或逗号分隔 key-column 名唯一匹配一个非 primary index。fixture 会在计时外创建有效 primary row 和所需旧 secondary 版本。

### 7.3 Visibility

```bash
bin/run-benchmark visibility \
  --snapshot-dir /data/snapshots/tpch-sf100-node-a \
  --snapshot-table lineitem \
  --threads 32 --clients 1 --batch-size 1 \
  --warmup-seconds 10 --duration-seconds 60 --data-size 1000000
```

每个 phase 都从 footer `recordNum` 调用 `addVisibility(..., 0, null, false)` 重建同一 clean baseline，使用不相交物理行，并在计时后校验 delete 结果。没有 Visibility state、checkpoint profile 或恢复流程。

运行的 fat JAR 必须用 manifest 中相同的 `retina.tile.visibility.capacity` 构建。native capacity 不一致会在 setup 阶段失败。

### 7.4 WriteBuffer Add

先使用专属 benchmark 配置连接外部 MySQL/etcd并启动 Metadata/Node Server：

```bash
export PIXELS_CONFIG="$PWD/etc/pixels.properties"
bin/start-metadata-server
```

另一个终端：

```bash
export PIXELS_CONFIG="$PWD/etc/pixels.properties"
export RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"

bin/run-benchmark write-buffer-add \
  --snapshot-dir /data/snapshots/tpch-sf100-node-a \
  --snapshot-table lineitem \
  --threads 32 --clients 4 --batch-size 64 \
  --warmup-seconds 10 --duration-seconds 60 --data-size 1000000 \
  --writebuffer-storage-scheme file \
  --writebuffer-base-uri "file:///data/retina-bench/${RUN_ID}/table" \
  --writebuffer-object-storage-scheme file \
  --writebuffer-object-folder "/data/retina-bench/${RUN_ID}/objects" \
  --writebuffer-row-pool-size 100000
```

WriteBuffer 不复制源物理写入状态。它使用 manifest schema/配置创建 fresh table、SQLite 和 buffers。输出路径必须新建、为空且与生产路径隔离；非 file 存储必须显式提供专属 base URI 和 object folder。

## 8. Transaction 与快照边界

Transaction benchmark 不读取 Snapshot v2：

- `transaction-begin` 通过真实 RPC 创建事务并在计时外清理；
- `transaction-commit` 在计时前通过真实 Begin 预填 pending ID；
- `transaction-begin-commit` 在计时内完成 Begin 后立即 Commit。

它们的状态由外部 etcd 和当前 Transaction Server 进程管理，不能从 snapshot manifest 恢复。

## 9. 安全与可重复性

- Snapshot 可能包含真实 catalog、路径、主键 descriptor 和 RocksDB/SQLite，应按生产数据分级保护。
- 不要把 snapshot、源配置或真实凭证提交到 Git。
- Index 原始 snapshot 保持只读；每轮使用新工作副本。
- Visibility 每个 phase 重建 JNI 对象；不要在测量时启用 GC。
- WriteBuffer 每轮使用新 schema/table、存储 prefix 和本地工作目录。
- 目标 Metadata、etcd、对象存储和文件系统必须与生产环境隔离。
- manifest 与 artifact 一起被恶意替换时，SHA-256 无法证明真实性；需要额外的签名或受控传输链路。
- Snapshot v2 只服务模块级上限测试，不能替代端到端 CDC 一致性验证。
