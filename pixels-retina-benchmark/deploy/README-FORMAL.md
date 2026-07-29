# Retina Formal Benchmark Bundle

This bundle runs the four formal Retina throughput suites sequentially on a
bare-metal Ubuntu 22.04 host:

1. Index (`run-index-formal-suite`)
2. Visibility (`run-visibility-suite`)
3. Transaction (`run-transaction-suite`)
4. WriteBuffer (`run-write-buffer-suite`)

## Contents

- `pixels-retina-benchmark/`: runtime fat JAR, scripts, and the final
  `etc/pixels.properties` copied from the validated cds2 configuration
- `third-party/etcd/`: etcd 3.3.4 binaries
- `install/bootstrap.sh`: installs MySQL via `apt`, imports metadata schema,
  syncs snapshot from S3, and runs host preflight checks
- `formal-defaults.env`: fixed paths and S3 locations
- `run-formal-benchmark.sh`: top-level entry point

The bundle does **not** include:

- AWS credentials
- Snapshot data (downloaded on first bootstrap from
  `s3://home-dongyang/data/node-a`)

## Requirements

- Ubuntu 22.04 x86_64
- JDK 8+ (40 GiB formal suites expect a 128 GiB-class host)
- `aws` CLI with access to:
  - `s3://home-dongyang/data/node-a`
  - `s3://home-dongyang/retina-benchmark`
  - `s3://home-dongyang/data/retina-result`
- Local disk under `/data/retina-benchmark`

## First-time setup

```bash
tar -xzf retina-formal-bundle-linux-amd64.tar.gz -C /opt
cd /opt/formal-bundle
./install/bootstrap.sh
```

Run the bootstrap as the benchmark user. It invokes `sudo` only for directory
creation, package installation, and MySQL administration; running the entire
script as root would leave benchmark directories owned by root.

`bootstrap.sh` is idempotent:

- installs `mysql-server` with `apt` when needed (see `docs/INSTALL.md`)
- creates `pixels` / `password` and imports `mysql-init/metadata_schema.sql`
- syncs snapshot to `/data/retina-benchmark/snapshot/node-a` when missing

## Run the full formal suite

```bash
cd /opt/formal-bundle
./run-formal-benchmark.sh
```

Useful options:

```bash
./run-formal-benchmark.sh --smoke
./run-formal-benchmark.sh --dry-run
./run-formal-benchmark.sh --skip-bootstrap
./run-formal-benchmark.sh --skip-upload
```

## Results

Local results:

```text
/data/retina-benchmark/results/formal-<RUN_ID>/
  manifest.json
  summary.txt
  index/
  visibility/
  transaction/
  write-buffer/
```

Uploaded results:

```text
s3://home-dongyang/data/retina-result/formal-<RUN_ID>/
```

The upload happens automatically at the end of `run-retina-formal-suite`,
including partial results when a stage fails.

## Default paths

| Item | Path |
|------|------|
| Snapshot (local) | `/data/retina-benchmark/snapshot/node-a` |
| Snapshot (S3) | `s3://home-dongyang/data/node-a` |
| WriteBuffer S3 root | `s3://home-dongyang/retina-benchmark` |
| Results (local) | `/data/retina-benchmark/results` |
| Results (S3) | `s3://home-dongyang/data/retina-result` |

Override defaults with environment variables defined in `formal-defaults.env`.
