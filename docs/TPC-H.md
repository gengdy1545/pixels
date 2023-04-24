# TPC-H Evaluation

After the installation of [Pixels + Trino](INSTALL.md), we can evaluate the performance of Pixels using TPC-H.

## Prepare TPC-H

Attach a volume that is larger than the scale factor (e.g., 150GB for SF100) to the EC2 instance.
Mount the attached volume to a local path (e.g., `/data/tpch`).
Download tpch-dbgen to the instance, build it, and generate the dataset and queries into the attached volume.
Here, we put the dataset in `/data/tpch/100g/`.
The file(s) of each table are stored in a separate directory named by the table name.

## Create TPC-H Database
Log in trino-cli and use the SQL statements in `scripts/sql/tpch_schema.sql` to create the TPC-H database in Pixels.
Change the value of the `storage` table property in the create-table statement to `hdfs` if HDFS is used as the
underlying storage system instead of S3.
> Note that trino-cli can execute only one SQL statement at each time.

Then, use `SHOW SCHEMAS` and `SHOW TABLES` statements to check if the tpch database has been
created successfully.

Connect to MySQL using the user `pixels`, and execute the SQL statements in `scripts/sql/tpch_layouts.sql`
to create the table layouts for the tables in the TPC-H database in Pixels. Actually, these layouts
should be created by the storage layout optimizer ([Rainbow](https://ieeexplore.ieee.org/document/8509421)).
However, we directly load the layouts here for simplicity.

Create the container to store the tables in S3. The container name is the same as the hostname
(e.g., `pixels-tpch`) in the `LAYOUT_ORDER_PATH` and `LAYOUT_COMPACT_PATH` of each table layout.
Change the bucket name if it already exists.

During data loading, Pixels will automatically create the folders in the bucket to store the files in each table.

## Load Data

We use `pixels-cli` to load data into Pixels tables.

Under `PIXELS_HOME`, run pixels-cli:
```bash
java -jar ./sbin/pixels-cli-*-full.jar
```

Then use the following commands in pixels-cli to load data for the TPC-H tables:
```bash
LOAD -o file:///data/tpch/100g/customer -s tpch -t customer -n 319150 -r \| -c 1
LOAD -o file:///data/tpch/100g/lineitem -s tpch -t lineitem -n 600040 -r \| -c 1
LOAD -o file:///data/tpch/100g/nation -s tpch -t nation -n 100 -r \| -c 1
LOAD -o file:///data/tpch/100g/orders -s tpch -t orders -n 638300 -r \| -c 1
LOAD -o file:///data/tpch/100g/part -s tpch -t part -n 769240 -r \| -c 1
LOAD -o file:///data/tpch/100g/partsupp -s tpch -t partsupp -n 360370 -r \| -c 1
LOAD -o file:///data/tpch/100g/region -s tpch -t region -n 10 -r \| -c 1
LOAD -o file:///data/tpch/100g/supplier -s tpch -t supplier -n 333340 -r \| -c 1
```
It may take about one hour. The last parameter `-c` of the `LOAD` command is the maximum number
of threads used for loading data. It only effects when the input directory (specified by `-o`)
contains multiple input files. In case that the TPC-H table has multiple parts, you can set
`-c` to the number of parts to improve the data loading performance.

As we don't use pixels-cache for TPC-H, there is no need to load the cache.
Otherwise, we can load the cached table into pixels-cache using:
```bash
./sbin/load-cache.sh layout_version
```
`layout_version` is the version of the table's layout that specifies the columns we want to cache.

## Run Queries
Connect to trino-cli:
```bash
cd ~/opt/trino-server
./bin/trino --server localhost:8080 --catalog pixels --schema tpch
```
Execute the TPC-H queries in trino-cli.

## Data Compaction*
This is optional. It is only needed if we want to test the query performance on the compact layout.
In pixels-cli, use the following commands to compact the files in the ordered path of each table:
```bash
COMPACT -s tpch -t customer -n no -c 2
COMPACT -s tpch -t lineitem -n no -c 16
COMPACT -s tpch -t orders -n no -c 8
COMPACT -s tpch -t part -n no -c 1
COMPACT -s tpch -t partsupp -n no -c 8
COMPACT -s tpch -t supplier -n no -c 1
```
The tables `nation` and `region` are too small, no need to compact them.
The last parameter `-c` of `COMPACT` command is the maximum number
of threads used for data compaction. For large tables such as `lineitem`, you can increase `-c` to
improve the compaction performance. Compaction is normally faster than loading with same number of threads.

To avoid scanning the small files in the ordered path during query execution,
create an empty bucket in S3 and change the ordered path in the metadata database
to the empty bucket.

## Statistics Collection*
This is optional. Data statistics enable cost-based query optimization for the queries.
Start Pixels and Trino, make sure that Trino can execute queries on `tpch` schema.
In pixels-cli, use the following commands to collect the data statistics for the columns in each table.
```bash
STAT -s tpch -t nation -o false -c true
STAT -s tpch -t region -o false -c true
STAT -s tpch -t supplier -o false -c true
STAT -s tpch -t customer -o false -c true
STAT -s tpch -t part -o false -c true
STAT -s tpch -t partsupp -o false -c true
STAT -s tpch -t orders -o false -c true
STAT -s tpch -t lineitem -o false -c true
```
After it is finished, statistics of each tpch column can be found in the `pixels_metadata.COLS` metadata table.
Finally, manually update the row count for each tpch table in `pixels_metadata.TBLS.TBL_ROW_COUNT`.

Set `splits.index.type=cost_based` and restart Trino to benefit from cost-based query optimization.