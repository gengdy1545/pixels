>[!note]
[retina-evaluation](https://github.com/gengdy1545/pixels/tree/evaluation/moduleAblation)
## 测试背景
pixels-retina 在实时更新过程中，主要涉及四个模块，分别是 transaction、index、visibility和write buffer。为了分析 retina 的吞吐高的具体原因，同时也为后期找到 retina 吞吐的瓶颈做准备，我们分别对每个模块进行 TPC-H 上的压力测试，验证模块单独吞吐上限。
## 测试流程
### 生成 TPC-H 数据导入后的 snapshot
1. 在独立代码目录构建 benchmark：

```bash
mvn -pl pixels-retina-benchmark -am package
tar -xzf pixels-retina-benchmark/target/*-runtime.tar.gz
cd pixels-retina-benchmark
```

2. 指向现有 Pixels：

```bash
export PIXELS_HOME=/opt/pixels
```

3. 停止 CDC、load 和更新，等待 WriteBuffer flush 完成且无 `TEMPORARY` 文件。

4. 停止 Retina，避免后台 flush、compaction 和 GC：

```bash
$PIXELS_HOME/sbin/stop-retina.sh
```

保持 Transaction、Metadata、etcd 和存储运行，并等待约 10 秒刷新事务高水位。

5. 导出：

```bash
bin/export-snapshot \
  --snapshot-output /data/snapshots/node-a \
  --snapshot-schema tpch \
  --snapshot-tables customer,lineitem,orders \
  --snapshot-source-quiesced true \
  --snapshot-copy-index-state true
```

6. 校验：

```bash
bin/run-benchmark snapshot-validate \
  --snapshot-dir /data/snapshots/node-a \
  --snapshot-table lineitem \
  --snapshot-require index,visibility,write-buffer
```

7. 校验成功后重启 Retina。快照位于 `/data/snapshots/node-a`。

### 在本地节点验证 snapshot

目标：在便宜本地节点（如 cds2）确认导出的 Snapshot v2 **可校验、可加载**，再上 AWS 跑完整压测。Snapshot **不是**整库备份：含 `snapshot.json` + 可选 `state/index`（RocksDB/SQLite），不含 MySQL dump、Visibility 原状态、MemTable、`.pxl` 文件。

**谁依赖 snapshot**

| 模块 | 是否读 snapshot | 说明 |
|------|-----------------|------|
| Index | 是 | 复制 `state/index` 到工作目录后打开 |
| Visibility | 是 | 只用 manifest 中 file/RG `recordNum` 建 clean baseline |
| WriteBuffer | 是 | 只用列 schema + 语义配置；运行时新建临时表，不恢复源 MemTable |
| Transaction | **否** | 只依赖 etcd + TransServer |

本地验证 snapshot 的步骤：

1. 本机构建并解压 runtime。`retina.tile.visibility.capacity` 会编进 `libpixels-retina.so`，**必须与 snapshot manifest 中值一致**（本快照为 `10240`）：

```bash
cd /path/to/pixels   # 例如 /mnt/disk2/pixels

export PIXELS_HOME=/tmp/pixels-retina-benchmark-build
install -d -m 0700 "$PIXELS_HOME/etc" "$PIXELS_HOME/lib"
cp pixels-retina-benchmark/deploy/etc/pixels.properties \
  "$PIXELS_HOME/etc/pixels.properties"
sed -i 's/^retina.tile.visibility.capacity=.*/retina.tile.visibility.capacity=10240/' \
  "$PIXELS_HOME/etc/pixels.properties"

# 若 pixels-retina 测试源码与主代码短暂不一致，用 -Dmaven.test.skip=true 跳过 testCompile
mvn -T 3 -pl pixels-retina-benchmark -am -DskipTests -Dmaven.test.skip=true package

tar -xzf pixels-retina-benchmark/target/pixels-retina-benchmark-*-runtime.tar.gz \
  -C /mnt/disk2/
cd /mnt/disk2/pixels-retina-benchmark
```

> `cpp/pixels-retina/build` 若 cmake 秒失败，先 `rm -rf cpp/pixels-retina/build` 再编。  
> 后续压测请 `export PIXELS_HOME` / `PIXELS_CONFIG` 指向本 runtime，避免误用 `/home/ubuntu/opt/pixels` 等旧配置。

2. 将源机导出的快照放到本地并保持只读，例如 `/mnt/disk2/node-a`：

```text
/mnt/disk2/node-a/
  snapshot.json
  state/index/rocksdb/
  state/index/sqlite/
```

Index 等场景会复制到 `--snapshot-work-dir`，不直接改原快照。

3. 做完整性校验（不连 MySQL / etcd）。`snapshot-validate` 只读本地目录：校验 manifest 语义、artifact SHA-256、路径逃逸等，**不必**配置 MySQL 密码：

```bash
cd /mnt/disk2/pixels-retina-benchmark
export PIXELS_HOME=/mnt/disk2/pixels-retina-benchmark
export PIXELS_CONFIG=$PWD/etc/pixels.properties

# 整库校验（所有表 + 全部 artifact）
bin/run-benchmark snapshot-validate \
  --snapshot-dir /mnt/disk2/node-a

# 按压测场景做 profile 校验（不写 --snapshot-table 则检查所有表）
bin/run-benchmark snapshot-validate \
  --snapshot-dir /mnt/disk2/node-a \
  --snapshot-require index,visibility,write-buffer

# 若只打算压某张表，可再指定（示例）
bin/run-benchmark snapshot-validate \
  --snapshot-dir /mnt/disk2/node-a \
  --snapshot-table orderline \
  --snapshot-require index,visibility,write-buffer
```

成功标志：`status=valid`。一般**不必**对每张表单独跑一遍；整库 + 一次 profile 即可。zsh 续行时勿把参数拆成多条命令。本快照预期要点：`formatVersion=2`、`sourceQuiesced=true`、`physicalIndexStateIncluded=true`、正数 `snapshotTimestamp`、`retina.tile.visibility.capacity=10240`；若无 secondary index，则不能跑 `index-*-secondary`。

4. （可选）用极小参数做 Index / Visibility 物理加载冒烟，确认工作副本可打开（仍不强制 MySQL）。Transaction / WriteBuffer 冒烟放到「验证压测」；WriteBuffer 另需 Metadata + MySQL + etcd，存储可用本机可达的 S3：

```bash
COMMON=(--threads 2 --clients 1 --batch-size 1 --warmup-seconds 0 --duration-seconds 2 --data-size 100)

bin/run-benchmark index-put-primary \
  --snapshot-dir /mnt/disk2/node-a --snapshot-table orderline \
  --snapshot-work-dir /tmp/rb-smoke-idx $COMMON

bin/run-benchmark visibility \
  --snapshot-dir /mnt/disk2/node-a --snapshot-table orderline \
  --snapshot-work-dir /tmp/rb-smoke-vis $COMMON
```

每轮换新的 `--snapshot-work-dir`。至此认为：**snapshot 在本地可验证、可加载**，再进入节点配置与压测流程。

### 本地节点配置

在跑四模块正式压测前，本地节点需要：外部依赖（etcd / MySQL）+ runtime `pixels.properties` + 压测专用 Metadata/Transaction 服务。Index / Visibility 本身不连这些服务，但完整验证含 Transaction 与 WriteBuffer 时必需。

**外部依赖边界**

| 依赖 | Transaction | WriteBuffer | Index / Visibility / snapshot-validate |
|------|-------------|-------------|----------------------------------------|
| etcd | 需要 | 需要 | 不需要 |
| MySQL + Metadata Server | 不需要 | 需要 | 不需要 |
| S3（或其它对象存储） | 不需要 | 需要（本实验用真实 S3） | 不需要 |

说明：

- **不必**从源机（cds1）整库导出/恢复 MySQL 业务元数据。Snapshot 不含 MySQL；WriteBuffer 会经 Metadata 新建临时 schema/table（默认 `retina_bench_<uuid>`）。
- MySQL 只需具备可用的 `pixels_metadata` catalog 表结构（`DBS`/`TBLS`/…）。可复用本机已有库，或另建 `pixels_metadata_bench` 隔离。
- 源 snapshot 中表存储多为 `minio`；本实验改为 **S3**，跑 WriteBuffer 时用 CLI 指定 `s3://...`，不要复用源生产路径。

配置与启动步骤：

1. 启动 etcd（勿与生产 etcd 混用）。若本机原有 PixelsCoordinator 占着 `18888/18889/18891`，压测前先停掉，改由 runtime 内服务占用：

```bash
ETCD_BIN=/home/ubuntu/opt/etcd-v3.3.4-linux-amd64-bin/etcd   # 按本机路径调整
DATA_DIR=/var/tmp/pixels-retina-benchmark/etcd-data
mkdir -p "$DATA_DIR"

"$ETCD_BIN" \
  --name retina-bench-etcd \
  --data-dir "$DATA_DIR" \
  --listen-client-urls http://127.0.0.1:2379 \
  --advertise-client-urls http://127.0.0.1:2379 \
  --listen-peer-urls http://127.0.0.1:2380 \
  --initial-advertise-peer-urls http://127.0.0.1:2380 \
  --initial-cluster retina-bench-etcd=http://127.0.0.1:2380 \
  --initial-cluster-token retina-bench \
  --initial-cluster-state new
```

自检：`etcdctl --endpoints=http://127.0.0.1:2379 endpoint health`。

2. 确认 MySQL 可用（示例用户 `pixels`）。若 catalog 表结构缺失，用 runtime 内或仓库中的 `metadata_schema.sql` 初始化；密码写入下一步的 `pixels.properties`，并用 `chmod 600` 限制权限：

```bash
mysql -upixels -p -e 'USE pixels_metadata; SHOW TABLES;'
```

3. 编辑 runtime 配置 `/mnt/disk2/pixels-retina-benchmark/etc/pixels.properties`，并在每个压测终端导出环境变量（避免读到旧 `PIXELS_HOME`）：

```bash
export PIXELS_HOME=/mnt/disk2/pixels-retina-benchmark
export PIXELS_CONFIG=/mnt/disk2/pixels-retina-benchmark/etc/pixels.properties
export AWS_REGION=us-east-2
export AWS_DEFAULT_REGION=us-east-2
export JAVA_OPTS='-Xms40g -Xmx40g'
```

> 若只设了 `PIXELS_HOME=/home/ubuntu/opt/pixels` 而未设 `PIXELS_CONFIG`，`bin/run-benchmark` 会加载 **opt 下旧配置**（例如错误密码），Metadata 会报 `Access denied`。
> 本实验约定后续所有压测 Java 服务端和客户端均使用
> `-Xms40g -Xmx40g`。每轮仅保留当前模块必需的服务，避免多个 40 GiB
> heap 加上 native/off-heap 内存超过 128 GiB。

服务与元数据库示例：

```properties
metadata.db.user=pixels
metadata.db.password=<本机可登录密码>
metadata.db.url=jdbc:mysql://localhost:3306/pixels_metadata?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false

metadata.server.host=localhost
metadata.server.port=18888
trans.server.host=localhost
trans.server.port=18889
retina.server.host=localhost
retina.server.port=18890
node.server.host=localhost
node.server.port=18891

etcd.hosts=localhost
etcd.port=2379
```

注意：`metadata.db.url` 不要写成 `jdbc:mysql://jdbc:mysql://...`。

与 snapshot 对齐 / 故意不同的项：

| 配置 | 要求 |
|------|------|
| `retina.tile.visibility.capacity` | **必须** = manifest（本快照 `10240`） |
| WriteBuffer / writer 语义（memTable、flush、block.size 等） | 宜与 snap `semanticConfig` 一致；跑 Index/Vis/WB 时多数键会被 snap **覆盖注入** |
| `index.bucket.num` / `node.virtual.num` | props 默认可与 snap 不同（如 128 vs 16、16 vs 4）；运行时以 snap 为准。WB 的 `--clients` **≤** snap 的 `node.virtual.num`（本快照为 **4**） |
| `enabled.storage.schemes` | 本实验：`s3,file`（snap 可能含 `minio`，属故意替换） |
| `retina.buffer.object.storage.scheme` | `s3`（snap 源端可能是 `minio`） |
| `retina.buffer.object.storage.folder` | 如 `s3://home-dongyang/retina-benchmark/objects`；正式跑 WriteBuffer 仍须用 CLI 带 `${RUN_ID}` 覆盖，避免多轮互相覆盖 |

S3 客户端配置（凭证走 `~/.aws`，不要把 AK/SK 写入 properties）：

```properties
enabled.storage.schemes=s3,file
retina.buffer.object.storage.scheme=s3
retina.buffer.object.storage.folder=s3://home-dongyang/retina-benchmark/objects

s3.enable.async=true
s3.use.async.client=true
s3.connection.timeout.sec=3600
s3.connection.acquisition.timeout.sec=3600
s3.client.service.threads=40
s3.max.request.concurrency=1000
s3.max.pending.requests=100000
```

写权限自检与本地状态目录（Metadata/Index 默认路径等仍可能用到，与 S3 数据面无关；路径须与 properties 中 `pixels.var.dir`、`retina.checkpoint.dir`、`index.*.path` 等一致）：

```bash
echo ok | aws s3 cp - s3://home-dongyang/retina-benchmark/_write_test.txt
aws s3 rm s3://home-dongyang/retina-benchmark/_write_test.txt

mkdir -p /var/tmp/pixels-retina-benchmark/{var,history,metrics,checkpoints,objects,rocksdb,rocksdb-stats,sqlite}
chmod 600 /mnt/disk2/pixels-retina-benchmark/etc/pixels.properties
```

4. 服务启动方式按模块区分。Transaction 自动化脚本会自己创建全新 etcd
和 Transaction Server，因此运行 Transaction 前必须保证 `2379/2380/18889`
空闲，**不要**手工预启动这两个服务。完成 Transaction 后，如需测试
WriteBuffer，再手工启动 etcd 与 Metadata Server：

```bash
cd /mnt/disk2/pixels-retina-benchmark
export PIXELS_HOME=/mnt/disk2/pixels-retina-benchmark
export PIXELS_CONFIG=$PWD/etc/pixels.properties
export AWS_REGION=us-east-2
export AWS_DEFAULT_REGION=us-east-2

# etcd 已按前文方式启动后，在独立终端运行
bin/start-metadata-server
```

确认端口：

```bash
ss -lntp | grep -E '2379|18888|18891'
```

之后即可进入「在本地节点验证压测」。WriteBuffer 命令需显式传：

```text
--writebuffer-storage-scheme s3
--writebuffer-object-storage-scheme s3
--writebuffer-base-uri s3://home-dongyang/retina-benchmark/<RUN_ID>/table
--writebuffer-object-folder s3://home-dongyang/retina-benchmark/<RUN_ID>/objects
--writebuffer-enabled-storage-schemes s3,file
```

否则默认可能仍按 snapshot 表的 `storageScheme=minio` 解析。

### 在本地节点验证压测

最终 aws 测试机器是 `i7i.4xlarge` (_16 vCPUs, 128 GiB of memory_)。
Index 的测量窗默认 **60s**，Visibility 最长 **180s**，WriteBuffer 为
**60s**。Transaction、Index 和 Visibility 的参数与隔离由各自的专用编排脚本
管理。

```bash
THREADS=16
WARMUP=30
DURATION=60
IDX_DURATION=60
DATA_SIZE=100000000         # 足够大，保证多为 stop_reason=duration
IDX_DATA_SIZE=10000000
IDX_WARMUP_OPERATIONS=100000
FORMAL_IDX=(--threads $THREADS --clients 1 --batch-size 4 \
  --warmup-seconds $WARMUP --warmup-operations $IDX_WARMUP_OPERATIONS \
  --duration-seconds $IDX_DURATION --data-size $IDX_DATA_SIZE)

FORMAL_WB=(--threads $THREADS --clients 4 --batch-size 64 \
  --warmup-seconds $WARMUP --duration-seconds $DURATION --data-size $DATA_SIZE)
```

zsh 下上述数组可直接展开；若写成单字符串须用 `${=VAR}`。四类模块结果
**不要相加**解释为 Retina 总吞吐。

#### Index

Index 不需要 etcd、MySQL、Metadata Server 或 S3。使用
`bin/run-index-suite` 一次验证三个主索引入口：

1. 脚本先对 snapshot 执行 `snapshot-validate --snapshot-require index`。
2. 依次运行 `index-put-primary → index-update-primary → index-delete-primary`。
3. 每项都启动新的 benchmark JVM，并使用独立且原本不存在的
   `--snapshot-work-dir`。运行时在打开 Index 前重新复制 snapshot 中完整的
   RocksDB 和 SQLite，所以三项不会共享前一项修改后的 Index。
   复制完成时立即输出 `snapshot_index_copy_completed=<工作目录>/index`。
4. 合成 RowId 按 old/new、warmup/measurement 划分为连续且互不重叠的区间，
   四类 RowLocation 使用不同 fileId；同一文件内只在 RG 边界形成新 SQLite range，
   不再用人为 RowId 空洞制造每行一个 range。
5. Update/Delete 默认使用 `--main-index-state warm-cache`：计时前将旧
   RowLocation fixture 刷入 SQLite，并预热其全部 range。`hot-buffer` 保留
   fixture 在 MainIndexBuffer；`cold-start` 刷盘后关闭并重开 MainIndex。
   这些准备均不计入 API 延迟；Put 不需要旧 RowLocation，因此该选项不影响 Put。
6. 任一项退出非零、`errors != 0`、成功操作数为零或停止原因不符合预期时，
   脚本立即终止并保留失败现场。成功项在记录结果和工作目录大小后立即删除，
   不会与下一项的副本同时占用磁盘。

先执行冒烟验证：

```bash
cd /mnt/disk2/pixels-retina-benchmark
export PIXELS_HOME=$PWD
export PIXELS_CONFIG=$PWD/etc/pixels.properties

bin/run-index-suite \
  --snapshot-dir /mnt/disk2/node-a \
  --snapshot-table customer \
  --work-dir-root /mnt/disk2/retina-bench-work/index \
  --results-dir /mnt/disk2/retina-bench-results \
  --smoke
```

冒烟通过后，正式性能测试选择 5 个代表性主索引：`item`（4B 单列）、
`stock`（8B 双列）、`order`（12B 三列）、`orderline`（16B 四列、最大表）
和 `history`（28B 六列、默认 prefix）。每张表分别运行 Put、Update、Delete，
总计 15 个测量场景；任一场景失败后立即终止：

```bash
bash <<'EOF'
set -Eeuo pipefail

cd /mnt/disk2/pixels-retina-benchmark
export PIXELS_HOME=$PWD
export PIXELS_CONFIG=$PWD/etc/pixels.properties

TABLES=(
  item stock order orderline history
)

for TABLE in "${TABLES[@]}"
do
  echo "===== formal Index benchmark: ${TABLE} ====="
  bin/run-index-suite \
    --snapshot-dir /mnt/disk2/node-a \
    --snapshot-table "${TABLE}" \
    --work-dir-root "/mnt/disk2/retina-bench-work/index/${TABLE}" \
    --results-dir "/mnt/disk2/retina-bench-results/index/${TABLE}" \
    --java-opts '-Xms40g -Xmx40g' \
    --threads 16 \
    --batch-size 4 \
    --warmup-seconds 30 \
    --warmup-operations 100000 \
    --duration-seconds 60 \
    --data-size 10000000 \
    --main-index-state warm-cache
done
EOF
```

正式参数是 16 threads、1 client、batch 4、每个场景 warmup 30 秒、测量
最多 60 秒、warmup 上限 10 万 entries、measurement 上限 1000 万 entries，
MainIndex `warm-cache`，以及 `-Xms40g -Xmx40g`。1000 万 entries 对应约
2,500,000 次完整 batch API，贴近生产中个位数 entries 的请求规模；
时间或 entries 上限先到即停止，分别得到 `duration` 或 `data-size`。该上限避免
旧参数 `1e9` 在计时前生成十亿请求导致 OOM。默认情况下任一时刻只保留当前场景
的一份副本，脚本要求工作盘至少留出 snapshot Index 状态大小的 2 倍。单场景
Put 或 Update 仍可能继续增加数据，运行前应额外留出增长空间。

结果目录包含 `snapshot-validation.txt`、`parameters.env`、
`system-info.txt`、三项完整输出和 `summary.txt`。这些结果文件不会被清理。
只有显式传入 `--keep-work-dirs` 时，独立工作副本才保留为：

```text
<work-dir-root>/index-<RUN_ID>/
  put/index/{rocksdb,sqlite,rocksdb-stats}
  update/index/{rocksdb,sqlite,rocksdb-stats}
  delete/index/{rocksdb,sqlite,rocksdb-stats}
```

即使成功副本已删除，仍可比较各项结果中的 `detail.workingCopy`，确认恢复路径
各不相同。失败场景不会被自动删除；需要调试全部成功副本时，在单次复现命令中
增加 `--keep-work-dirs`，检查后手工删除。`--snapshot-table` 指定单次 suite
的目标表；上述循环逐表执行，避免将不同表的 key 描述、表 ID 和文件拓扑混为
同一个 workload。其余 7 张表只需通过 snapshot profile 与小规模 smoke 验证，
无需重复正式性能测试。

#### Visibility

Visibility 不需要 etcd、MySQL、Metadata Server、S3，也不复制 Index。正式性能
测试固定使用 `orderline` 的 production-selected ordered 路径；该路径包含
128 个 RG、3000 万物理行。compact、secondary 和 projection 路径不参与。

`bin/run-visibility-suite` 默认参数：

- 16 threads、1 client、batch 1；
- warmup 最长 10 秒、最多删除 10 万行；
- measurement 最长 180 秒、最多删除 2990 万行；
- Warmup 与 Measurement 使用互不重复的位置，合计最多覆盖全部 3000 万行；
- JVM 使用 `-Xms40g -Xmx40g`；
- 默认不校验最终 bitmap，仍检查 API 错误数和 worker/result 计数；诊断正确性时
  可显式传入 `--validate-final-bitmap`。

先执行冒烟：

```bash
cd /mnt/disk2/pixels-retina-benchmark
export PIXELS_HOME=$PWD
export PIXELS_CONFIG=$PWD/etc/pixels.properties

bin/run-visibility-suite \
  --snapshot-dir /mnt/disk2/node-a \
  --results-dir /mnt/disk2/retina-bench-results \
  --smoke
```

冒烟通过后执行正式测试：

```bash
bin/run-visibility-suite \
  --snapshot-dir /mnt/disk2/node-a \
  --results-dir /mnt/disk2/retina-bench-results
```

正式阶段先达到 180 秒或 2990 万行上限即停止，对应
`stop_reason=duration` 或 `data-size`。脚本强制拒绝超过 180 秒的参数，并检查
`manifestVisibilityRows=30000000`、`errors=0`、API calls 与成功数一致、
Java/native capacity 一致。最终 bitmap 校验暂时默认关闭，避免 native 并发
丢更新导致吞吐测试中断；使用 `--validate-final-bitmap` 时才检查成功数与
`lastValidatedDeletes` 一致。

当前实现会在计时前预生成最多 3000 万个 RowLocation，准备时间不计入吞吐；
启用最终 bitmap 校验时，计时后的全量校验也不计入吞吐。它主要占用 JVM 内存，
不会像 Index 一样生成多份磁盘副本。
结果保存在 `visibility-<RUN_ID>/{parameters.env,system-info.txt,
snapshot-validation.txt,result.txt,summary.txt}`。

#### WriteBuffer

WriteBuffer 正式测试固定使用 Snapshot 中 `orderline` 的列 schema 和语义配置，
测量 `PixelsWriteBuffer.addRow` 的接纳吞吐。它需要真实 Metadata/Node Server、
MySQL、etcd 和 S3，但不恢复源端的 MySQL、etcd、MemTable 或生产 table ID。

`bin/run-write-buffer-suite` 默认参数：

- 16 threads、4 clients/vnodes、batch 64；
- warmup 最长 30 秒、最多 10 万行；
- measurement 最长 **60 秒**、最多 1 亿行，先到者停止，对应
  `stop_reason=duration` 或 `data-size`，最多覆盖 2 次 30 秒定时 flush，
  并保证 warmup/measurement timestamp 不超过 native 48-bit 表示；
- row value pool 为 10 万行；pool 中的值允许复用，但 Row ID 和 RowLocation
  不复用；
- MemTable 10240 行、每文件 20 个 MemTable、4 个 object flush threads、
  flush interval 30 秒、encoding level 2、2 GiB block；
- Metadata Server 和 benchmark client JVM 均为 `-Xms40g -Xmx40g`。

每轮测试使用四层隔离：

1. 脚本创建全新的 etcd data dir，并以 `initial-cluster-state=new` 启动；成功后
   删除该目录，失败时保留现场。启动后会检查 `etcd-initial-keys.txt` 为空，确保
   没有继承其他测试的 etcd key。
2. Metadata Server 连接 runtime 配置中的现有 MySQL catalog，但 benchmark 使用
   唯一逻辑 schema `retina_bench_wb_<RUN_ID>`，正常 close 时自动 drop。Snapshot
   不含 MySQL dump，因此这里不是 MySQL 物理恢复；全局 ID 继续递增，但不会复用其他
   测试的表或文件。
3. warmup 和 measurement 各自创建新 Metadata table、SQLite MainIndex 和
   `PixelsWriteBuffer`；本地 SQLite 状态随 benchmark JVM 清理。
4. 所有 S3 输出写入 `<s3-root>/<RUN_ID>`。对象成功后保留供核查，不会与其他测试
   混用。

正式 measurement 结束后不等待 `PixelsWriteBuffer.close()`；benchmark 立即输出
addRow 接纳吞吐并强制结束客户端 JVM，结果记录
`measurement_close_status=skipped`、`persistence_validated=false`。Warmup 仍完整
close。客户端退出后，脚本使用 Metadata RPC 删除本轮唯一 schema，因此该结果不能
解释为最终 S3 持久化吞吐或持久化完整性验证。

benchmark client 会自动 preload 内置 `libjemalloc.so.2`，避免 JNI 与 glibc
malloc 混用导致 `free(): invalid pointer`。

先停止手工启动的 etcd、Metadata/Node Server，确保 2379、2380、18888、18891
空闲。脚本发现端口占用时只报错，不会终止未知进程。然后执行冒烟：

```bash
cd /mnt/disk2/pixels-retina-benchmark
export PIXELS_HOME=$PWD
export PIXELS_CONFIG=$PWD/etc/pixels.properties

bin/run-write-buffer-suite \
  --snapshot-dir /mnt/disk2/node-a \
  --s3-root s3://home-dongyang/retina-benchmark \
  --etcd-bin /home/ubuntu/opt/etcd-v3.3.4-linux-amd64-bin/etcd \
  --etcdctl-bin /home/ubuntu/opt/etcd-v3.3.4-linux-amd64-bin/etcdctl \
  --results-dir /mnt/disk2/retina-bench-results \
  --smoke
```

冒烟通过后去掉 `--smoke` 执行正式测试。正式 measurement 固定 **60 秒**，
最多接纳 1 亿行；计时结束后立即输出结果，不等待 measurement close：

```bash
bin/run-write-buffer-suite \
  --snapshot-dir /mnt/disk2/node-a \
  --s3-root s3://home-dongyang/retina-benchmark \
  --etcd-bin /home/ubuntu/opt/etcd-v3.3.4-linux-amd64-bin/etcd \
  --etcdctl-bin /home/ubuntu/opt/etcd-v3.3.4-linux-amd64-bin/etcdctl \
  --results-dir /mnt/disk2/retina-bench-results
```

`--s3-root` 必须是专用于 benchmark 的 S3 路径，不得指向 Snapshot manifest 中的
生产数据路径。脚本会检查 Snapshot write-buffer profile、`errors=0`、成功行数、
S3/flush 参数和异步 flush 错误；安装 AWS CLI 时还会列出该轮 prefix 下的真实
对象。结果保存在 `write-buffer-<RUN_ID>/{parameters.env,system-info.txt,
snapshot-validation.txt,etcd.log,etcd-initial-keys.txt,metadata-server.log,
result.txt,summary.txt,s3-artifacts.txt}`。`etcd-initial-keys.txt` 必须为空。
S3 对象不会自动删除，检查后需手工清理该轮 `RUN_ID` prefix。

#### Transaction

Transaction **不读 snapshot**，也不需要 Metadata / MySQL / S3；只依赖本机 **etcd + `transaction-server`**。三个子命令含义不同：

| 命令 | 计时内容 | pending 行为 |
|------|----------|----------------|
| `transaction-begin-commit` | Begin 后立刻 Commit（完整生命周期） | 不囤积；适合 60s 稳态 |
| `transaction-begin` | 只 Begin | 成功事务全部 pending，阶段结束后批量 Commit 清理（清理不计时） |
| `transaction-commit` | 计时外预填 Begin，计时内只 Commit | 预填量 ≈ `data-size`，受内存与写事务 lease 约束 |

使用 runtime 中的 `bin/run-transaction-suite` 自动管理三项测试。默认
`--isolation scenario`：每个场景使用全新的 etcd data dir 和 Transaction
Server，因此不会继承上一项的 etcd key、pending context 或 JVM 堆状态。
脚本使用 `set -Eeuo pipefail`，任一 benchmark 或 cleanup 失败就立即停止，
不会继续执行下一项。

默认正式参数：

| 参数 | 值 |
|------|----|
| threads / clients / batch | `16 / 4 / 64` |
| Begin-Commit | warmup 30s，测量 60s，`data-size=1e9` |
| Begin | warmup 30s，`data-size=2e7`，cleanup batch 1024 |
| Commit | warmup 10s，`data-size=2e7`，prefill batch 1024 |
| RPC deadline | 180s |
| 服务端与 benchmark 客户端 JVM | `-Xms40g -Xmx40g` |

验证流程：

1. 确保 runtime 已包含新脚本，并设置配置路径：

```bash
cd /mnt/disk2/pixels-retina-benchmark
export PIXELS_HOME=/mnt/disk2/pixels-retina-benchmark
export PIXELS_CONFIG=$PWD/etc/pixels.properties

test -x bin/run-transaction-suite
```

若当前 runtime 是修改脚本前解压的旧包，需要重新构建并解压 runtime。

2. 停止手工启动的 etcd / Transaction Server，确认脚本所需端口空闲。
脚本发现未知进程占用端口时只会报错退出，不会擅自杀进程：

```bash
ss -lntp | grep -E '2379|2380|18889' || echo 'transaction suite ports are free'
```

3. 先用小参数执行自动化冒烟。脚本会依次运行
`begin-commit → begin → commit`，每项使用全新的 etcd 和 TransServer：

```bash
bin/run-transaction-suite \
  --isolation scenario \
  --etcd-bin /home/ubuntu/opt/etcd-v3.3.4-linux-amd64-bin/etcd \
  --etcdctl-bin /home/ubuntu/opt/etcd-v3.3.4-linux-amd64-bin/etcdctl \
  --results-dir /mnt/disk2/retina-bench-results \
  --smoke
```

4. 冒烟通过后，去掉 `--smoke` 运行正式测试：

```bash
bin/run-transaction-suite \
  --isolation scenario \
  --etcd-bin /home/ubuntu/opt/etcd-v3.3.4-linux-amd64-bin/etcd \
  --etcdctl-bin /home/ubuntu/opt/etcd-v3.3.4-linux-amd64-bin/etcdctl \
  --results-dir /mnt/disk2/retina-bench-results
```

需要模拟生产环境中事务 ID / watermark 连续递增时，可改为
`--isolation suite`：整套测试共用一个全新 etcd，但每项仍重启 TransServer。

5. 脚本结束时会打印 `summary.txt`，结果目录结构如下：

```text
tx-<RUN_ID>/
  parameters.env
  system-info.txt
  summary.txt
  begin-commit/
    etcd.log
    transaction-server.log
    transaction-server-rss.txt
    result.txt
  begin/
    ...
  commit/
    ...
```

成功时三项均满足 `errors=0`；Begin-Commit 应为
`stop_reason=duration`，Begin 应为 `stop_reason=data-size`，Commit 可为
`data-size` 或 `duration`。脚本同时记录每项 TransServer 的峰值 RSS。

说明：`batch-size>1` 时 `throughput_ops_per_second` 是逻辑 op 吞吐；更贴近 RPC 负载的是 `api_calls_per_second` 与 API 延迟。`begin` / `commit` 因 pending 封顶，测量墙钟时间通常短于 60s，属预期；真正 60s 稳态以 `begin-commit` 为准。

6. 默认 `2e7` pending 用于兼顾本地 64 GiB 验证节点和 AWS 正式环境。
若仍发生内存压力，可通过 `--pending-data-size 10000000` 降档重跑；
若 AWS 上要额外探索更大 pending 窗口，应作为单独实验显式覆盖，而不是修改
默认口径。运行前可用 `--dry-run` 检查
最终参数；端口冲突时也可通过 `--etcd-port`、`--etcd-peer-port` 和
`--trans-port` 覆盖。脚本创建的隔离状态位于
`/var/tmp/pixels-retina-benchmark/transaction-suite/tx-<RUN_ID>`，不会覆盖
既有 etcd 数据。

## 正式四模块一键打包与运行

在 cds2 上构建 bundle（使用最终 `pixels.properties`，不含 snapshot 与 AWS 凭证）：

```bash
cd /mnt/disk2/pixels/pixels-retina-benchmark
./build-formal-bundle.sh \
  --config /mnt/disk2/pixels-retina-benchmark/etc/pixels.properties
```

产物：

```text
pixels-retina-benchmark/target/retina-formal-bundle-linux-amd64.tar.gz   # 约 218 MiB
```

将 archive 拷到 AWS Ubuntu 22.04 裸机（建议 i7i.4xlarge，本地 `/data` ≥ 100 GiB）后：

```bash
tar -xzf retina-formal-bundle-linux-amd64.tar.gz -C /opt
cd /opt/formal-bundle

# 首次：安装 MySQL（apt，同 docs/INSTALL.md）、导入 schema、
# 从 s3://home-dongyang/data/node-a 同步 snapshot 到本地
sudo ./install/bootstrap.sh

# 跑完全部正式测试（Index → Visibility → Transaction → WriteBuffer，严格串行）
./run-formal-benchmark.sh
```

结果：

- 本地：`/data/retina-benchmark/results/formal-<RUN_ID>/`
- 自动上传：`s3://home-dongyang/data/retina-result/formal-<RUN_ID>/`

bundle 内含 `third-party/etcd`（3.3.4）、最终 `pixels.properties`、五个子 suite
脚本与 `run-retina-formal-suite` 编排器。Index 正式矩阵通过
`run-index-formal-suite` 跑 5 张表（item/stock/order/orderline/history）。
机器需自带 AWS 凭证；WriteBuffer 仍写入 `s3://home-dongyang/retina-benchmark/<RUN_ID>/`。

常用选项：

```bash
./run-formal-benchmark.sh --smoke          # 各模块冒烟参数
./run-formal-benchmark.sh --dry-run        # 查看路径与参数
./run-formal-benchmark.sh --skip-bootstrap # snapshot/MySQL 已就绪时跳过
./run-formal-benchmark.sh --skip-upload    # 仅保留本地结果
```

详细说明见 bundle 内 `README-FORMAL.md`。

