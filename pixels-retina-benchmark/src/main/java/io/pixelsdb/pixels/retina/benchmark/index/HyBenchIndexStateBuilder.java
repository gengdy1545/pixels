/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
package io.pixelsdb.pixels.retina.benchmark.index;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.common.index.IndexOption;
import io.pixelsdb.pixels.common.index.MainIndexFactory;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.common.index.SinglePointIndexFactory;
import io.pixelsdb.pixels.common.index.service.IndexService;
import io.pixelsdb.pixels.common.index.service.IndexServiceProvider;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.IndexUtils;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.index.rocksdb.RocksDBFactory;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkConfig;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotIO;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotManifest;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotRuntime;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Builds a real-key, LOAD-equivalent primary Index state for HyBench without
 * starting Pixels services or writing Pixels payload files.
 *
 * <p>The builder follows the indexed LOAD ordering: it parses the generated
 * rows, encodes the primary key with Pixels' canonical integer encoding,
 * routes entries by the configured Index bucket, emits row ids and locations,
 * and flushes every row group into RocksDB and SQLite MainIndex. The payload
 * writer is the only part omitted.</p>
 */
public final class HyBenchIndexStateBuilder
{
    private static final int DEFAULT_BATCH_SIZE = 4096;
    private static final int PRIMARY_KEY_BYTES = Integer.BYTES;
    private static final long TABLE_ID_BASE = 10_000L;
    private static final long INDEX_ID_BASE = 20_000L;
    private static final long SCHEMA_ID = 30_000L;
    private static final long SCHEMA_VERSION_BASE = 40_000L;

    /** HyBench's two blocked_* files are anomaly side files, not tables. */
    private static final List<TableSpec> TABLES = Collections.unmodifiableList(Arrays.asList(
            new TableSpec("customer", "customer.csv", 15, "CUSTKEY"),
            new TableSpec("company", "company.csv", 14, "COMPANYKEY"),
            new TableSpec("savingAccount", "savingAccount.csv", 6, "ACCOUNTKEY"),
            new TableSpec("checkingAccount", "checkingAccount.csv", 5, "ACCOUNTKEY"),
            new TableSpec("transfer", "transfer.csv", 7, "TRANSKEY"),
            new TableSpec("checking", "checking.csv", 6, "CHECKKEY"),
            new TableSpec("loanApps", "loanApps.csv", 6, "LOANAPPKEY"),
            new TableSpec("loanTrans", "loanTrans.csv", 9, "LOANKEY")));

    private HyBenchIndexStateBuilder()
    {
    }

    public static void run(BenchmarkConfig config) throws Exception
    {
        configureStandaloneRuntime(config);
        Path sourceSnapshot = Paths.get(config.require("snapshot-dir"))
                .toAbsolutePath().normalize();
        Path dataDirectory = Paths.get(config.require("hybench-data-dir"))
                .toAbsolutePath().normalize();
        Path outputDirectory = Paths.get(config.require("output-dir"))
                .toAbsolutePath().normalize();
        String scale = config.get("hybench-scale", outputDirectory.getFileName().toString());
        long rowsPerFile = config.getLong("hybench-rows-per-file", -1L);
        int batchSize = config.getInt("index-build-batch-size", DEFAULT_BATCH_SIZE);
        long maxRows = config.getLong("hybench-max-rows-per-table", 0L);

        if (!Files.isDirectory(dataDirectory))
        {
            throw new IOException("HyBench data directory does not exist: " + dataDirectory);
        }
        if (rowsPerFile <= 0 || batchSize <= 0 || maxRows < 0)
        {
            throw new IllegalArgumentException("hybench-rows-per-file and index-build-batch-size must be positive; "
                    + "hybench-max-rows-per-table must be non-negative");
        }
        ensureEmptyOutput(outputDirectory);

        SnapshotManifest sourceManifest = SnapshotIO.readManifest(sourceSnapshot);
        int pixelStride = semanticInt(sourceManifest, "pixel.stride");
        int bucketCount = semanticInt(sourceManifest, "index.bucket.num");
        if (pixelStride <= 0 || bucketCount <= 0)
        {
            throw new IllegalArgumentException("source Snapshot has invalid pixel.stride or index.bucket.num");
        }
        Path indexStateDirectory = outputDirectory.resolve("state/index");

        List<SnapshotManifest.TableState> builtTables = new ArrayList<>();
        try (SnapshotRuntime runtime = SnapshotRuntime.open(
                BenchmarkConfig.parse("--snapshot-dir", sourceSnapshot.toString())))
        {
            runtime.prepareEmptyIndexState(indexStateDirectory);
            ConfigFactory pixels = ConfigFactory.Instance();
            pixels.addProperty("index.bucket.num", Integer.toString(bucketCount));
            registerAllIndexKeyLengths();

            IndexService indexService = IndexServiceProvider.getService(
                    IndexServiceProvider.ServiceMode.local);
            for (int tableOrdinal = 0; tableOrdinal < TABLES.size(); tableOrdinal++)
            {
                TableSpec spec = TABLES.get(tableOrdinal);
                Path input = dataDirectory.resolve(spec.fileName);
                if (!Files.isRegularFile(input))
                {
                    throw new IOException("missing HyBench table file: " + input);
                }
                SnapshotManifest.TableState table = buildTable(
                        input, spec, tableOrdinal, indexService, bucketCount,
                        pixelStride, rowsPerFile, batchSize, maxRows,
                        Math.max(1L, sourceManifest.snapshotTimestamp + 1L));
                builtTables.add(table);
                System.out.println("hybench_table=" + spec.tableName
                        + " rows=" + table.metadataRowCount
                        + " files=" + table.files.size());
            }

            writeBuildProperties(outputDirectory, scale, dataDirectory, sourceSnapshot,
                    rowsPerFile, pixelStride, bucketCount, builtTables);
        }

        SnapshotManifest outputManifest = createManifest(sourceManifest, scale,
                rowsPerFile, builtTables, outputDirectory);
        SnapshotIO.writeManifest(outputDirectory, outputManifest);
        System.out.println("hybench_index_state_build_completed=" + outputDirectory);
        System.out.println("hybench_index_state_tables=" + builtTables.size());
        System.out.println("hybench_index_state_scale=" + scale);
    }

    /**
     * The Index-only builder does not start MetadataServer, but the production
     * Index utility classes retain a MetadataService singleton for historical
     * reasons.  Give that unused singleton harmless local endpoints so a
     * deployment template with REPLACE_WITH_* service ports remains usable.
     */
    private static void configureStandaloneRuntime(BenchmarkConfig config)
    {
        ConfigFactory pixels = ConfigFactory.Instance();
        pixels.addProperty("metadata.server.host",
                config.get("index-only-metadata-host", "127.0.0.1"));
        pixels.addProperty("metadata.server.port",
                config.get("index-only-metadata-port", "18888"));
    }

    private static SnapshotManifest.TableState buildTable(
            Path input, TableSpec spec, int tableOrdinal, IndexService indexService,
            int bucketCount, int pixelStride, long rowsPerFile, int batchSize,
            long maxRows, long timestamp) throws Exception
    {
        long tableId = TABLE_ID_BASE + tableOrdinal;
        long indexId = INDEX_ID_BASE + tableOrdinal;
        long schemaVersionId = SCHEMA_VERSION_BASE + tableOrdinal;
        SnapshotManifest.TableState table = tableState(spec, tableOrdinal, tableId,
                indexId, schemaVersionId, 0L, rowsPerFile, pixelStride);
        SnapshotManifest.IndexState index = table.primaryIndex;
        IndexOption[] options = new IndexOption[bucketCount];
        for (int bucket = 0; bucket < bucketCount; bucket++)
        {
            options[bucket] = IndexOption.builder().vNodeId(bucket).build();
        }
        SinglePointIndexFactory singlePointFactory = SinglePointIndexFactory.Instance();
        if (!singlePointFactory.isSchemeEnabled(SinglePointIndex.Scheme.rocksdb))
        {
            throw new IllegalStateException("HyBench Index-only build requires RocksDB to be enabled");
        }
        if (!MainIndexFactory.Instance().isSchemeEnabled(io.pixelsdb.pixels.common.index.MainIndex.Scheme.sqlite))
        {
            throw new IllegalStateException("HyBench Index-only build requires SQLite MainIndex");
        }
        SinglePointIndexFactory.TableIndex descriptor = new SinglePointIndexFactory.TableIndex(
                tableId, indexId, SinglePointIndex.Scheme.rocksdb, true);
        for (IndexOption option : options)
        {
            singlePointFactory.getSinglePointIndex(descriptor, option);
        }

        @SuppressWarnings("unchecked")
        List<IndexProto.PrimaryIndexEntry>[] queues = (List<IndexProto.PrimaryIndexEntry>[]) new List<?>[bucketCount];
        for (int bucket = 0; bucket < bucketCount; bucket++)
        {
            queues[bucket] = new ArrayList<>(batchSize);
        }

        long rowId = 0L;
        long fileId = 1L;
        long fileRows = 0L;
        int rgId = 0;
        int rgRowOffset = 0;
        int pending = 0;
        boolean rowGroupHasEntries = false;
        try (BufferedReader reader = Files.newBufferedReader(input))
        {
            String line;
            while ((line = reader.readLine()) != null)
            {
                if (line.isEmpty())
                {
                    continue;
                }
                if (maxRows > 0 && rowId >= maxRows)
                {
                    break;
                }
                String keyText = firstCsvField(line);
                ByteString key = ByteString.copyFrom(
                        TypeDescription.createInt().convertSqlStringToByte(keyText));
                int bucket = IndexUtils.getBucketIdFromByteBuffer(key);
                IndexProto.PrimaryIndexEntry entry = IndexProto.PrimaryIndexEntry.newBuilder()
                        .setIndexKey(IndexProto.IndexKey.newBuilder()
                                .setTableId(tableId)
                                .setIndexId(indexId)
                                .setKey(key)
                                .setTimestamp(timestamp)
                                .build())
                        .setRowId(rowId)
                        .setRowLocation(IndexProto.RowLocation.newBuilder()
                                .setFileId(fileId)
                                .setRgId(rgId)
                                .setRgRowOffset(rgRowOffset)
                                .build())
                        .build();
                queues[bucket].add(entry);
                pending++;
                rowGroupHasEntries = true;
                rowId++;
                fileRows++;
                rgRowOffset++;

                boolean endRowGroup = rgRowOffset == pixelStride;
                boolean endFile = fileRows == rowsPerFile;
                if (pending >= batchSize || endRowGroup || endFile)
                {
                    flushQueues(indexService, tableId, indexId, queues, options);
                    pending = 0;
                }
                if (endRowGroup || endFile)
                {
                    if (pending != 0)
                    {
                        flushQueues(indexService, tableId, indexId, queues, options);
                        pending = 0;
                    }
                    if (!indexService.flushIndexEntriesOfFile(
                            tableId, indexId, fileId, true, options[0]))
                    {
                        throw new IndexException("failed to flush MainIndex file " + fileId);
                    }
                    if (endFile)
                    {
                        fileId++;
                        fileRows = 0L;
                        rgId = 0;
                        rgRowOffset = 0;
                        rowGroupHasEntries = false;
                    }
                    else
                    {
                        rgId++;
                        rgRowOffset = 0;
                        rowGroupHasEntries = false;
                    }
                }
            }
            if (pending != 0)
            {
                flushQueues(indexService, tableId, indexId, queues, options);
                pending = 0;
            }
            if (rowGroupHasEntries)
            {
                if (!indexService.flushIndexEntriesOfFile(
                        tableId, indexId, fileId, true, options[0]))
                {
                    throw new IndexException("failed to flush MainIndex file " + fileId);
                }
            }
        }
        finally
        {
            try
            {
                indexService.closeIndex(tableId, indexId, true, options[0]);
            }
            catch (Exception closeFailure)
            {
                throw closeFailure;
            }
        }

        if (rowId == 0L)
        {
            throw new IllegalArgumentException("HyBench table is empty: " + input);
        }
        table.metadataRowCount = rowId;
        table.files = tableFiles(table, rowId, rowsPerFile, pixelStride);
        return table;
    }

    private static void flushQueues(IndexService service, long tableId, long indexId,
                                    List<IndexProto.PrimaryIndexEntry>[] queues,
                                    IndexOption[] options) throws Exception
    {
        for (int bucket = 0; bucket < queues.length; bucket++)
        {
            if (!queues[bucket].isEmpty())
            {
                if (!service.putPrimaryIndexEntries(tableId, indexId, queues[bucket], options[bucket]))
                {
                    throw new IndexException("failed to put HyBench primary Index entries in bucket " + bucket);
                }
                queues[bucket].clear();
            }
        }
    }

    private static void registerAllIndexKeyLengths()
    {
        for (int tableOrdinal = 0; tableOrdinal < TABLES.size(); tableOrdinal++)
        {
            RocksDBFactory.registerIndexKeyLength(
                    INDEX_ID_BASE + tableOrdinal, PRIMARY_KEY_BYTES);
        }
    }

    private static SnapshotManifest.TableState tableState(
            TableSpec spec, int tableOrdinal, long tableId, long indexId,
            long schemaVersionId, long rowCount, long rowsPerFile, int pixelStride)
    {
        SnapshotManifest.TableState table = new SnapshotManifest.TableState();
        table.schemaName = "hybench";
        table.schemaId = SCHEMA_ID;
        table.tableName = spec.tableName;
        table.tableId = tableId;
        table.tableType = "user";
        table.storageScheme = "file";
        table.metadataRowCount = rowCount;

        for (int ordinal = 0; ordinal < spec.columnCount; ordinal++)
        {
            SnapshotManifest.ColumnState column = new SnapshotManifest.ColumnState();
            column.ordinal = ordinal;
            column.columnId = tableId * 100L + ordinal + 1L;
            column.name = ordinal == 0 ? spec.primaryKeyName : "column_" + ordinal;
            column.type = ordinal == 0 ? "integer" : "string";
            table.columns.add(column);
        }

        SnapshotManifest.IndexState index = new SnapshotManifest.IndexState();
        index.indexId = indexId;
        index.scheme = "rocksdb";
        index.primary = true;
        index.unique = true;
        index.schemaVersionId = schemaVersionId;
        index.keyColumnIds.add(tableId * 100L + 1L);
        index.keyColumnOrdinals.add(0);
        index.keyColumnNames.add(spec.primaryKeyName);
        index.keyColumnTypes.add("integer");
        index.rocksDbPrefixKeyBytes = PRIMARY_KEY_BYTES;
        index.canonicalKeyBytes = PRIMARY_KEY_BYTES;
        table.indexes.add(index);
        table.primaryIndex = index;

        SnapshotManifest.LayoutState layout = new SnapshotManifest.LayoutState();
        layout.layoutId = tableId * 10L + 1L;
        layout.tableId = tableId;
        layout.schemaVersionId = schemaVersionId;
        layout.version = 1L;
        layout.permission = "rw";
        layout.readable = true;
        layout.writable = false;
        layout.productionSelectedLatestLayout = true;
        SnapshotManifest.PathState path = new SnapshotManifest.PathState();
        path.apiOrdinal = 0;
        path.pathId = tableId * 10L + 2L;
        path.role = "ordered";
        path.uri = "synthetic://hybench/ordered/" + spec.tableName;
        path.productionSelectedByRetina = true;
        layout.paths.add(path);
        table.layouts.add(layout);
        table.productionOrderedPathId = path.pathId;
        table.productionOrderedPathUri = path.uri;
        return table;
    }

    private static List<SnapshotManifest.FileState> tableFiles(
            SnapshotManifest.TableState table, long rowCount, long rowsPerFile, int pixelStride)
    {
        SnapshotManifest.LayoutState layout = table.layouts.get(0);
        SnapshotManifest.PathState path = layout.paths.get(0);
        List<SnapshotManifest.FileState> files = new ArrayList<>();
        long fileCount = (rowCount + rowsPerFile - 1L) / rowsPerFile;
        for (long fileOrdinal = 0; fileOrdinal < fileCount; fileOrdinal++)
        {
            long rowStart = fileOrdinal * rowsPerFile;
            long rows = Math.min(rowsPerFile, rowCount - rowStart);
            SnapshotManifest.FileState file = new SnapshotManifest.FileState();
            file.fileId = fileOrdinal + 1L;
            file.pathId = path.pathId;
            file.layoutId = layout.layoutId;
            file.layoutRole = "ordered";
            file.name = String.format("%s-%08d.pxl", table.tableName, file.fileId);
            file.fullUri = path.uri + "/" + file.name;
            file.type = "ordered";
            file.minRowId = rowStart;
            file.maxRowId = rowStart + rows - 1L;
            file.footerRowCount = rows;
            file.pixelStride = pixelStride;
            file.compressionKind = "synthetic";
            file.fileVersion = "index-only";
            int rowGroups = (int) ((rows + pixelStride - 1L) / pixelStride);
            file.metadataNumRowGroups = rowGroups;
            file.footerRowGroupCount = rowGroups;
            for (int rgId = 0; rgId < rowGroups; rgId++)
            {
                SnapshotManifest.RowGroupState rowGroup = new SnapshotManifest.RowGroupState();
                rowGroup.rgId = rgId;
                long rgStart = (long) rgId * pixelStride;
                rowGroup.recordNum = (int) Math.min(pixelStride, rows - rgStart);
                file.rowGroups.add(rowGroup);
            }
            files.add(file);
        }
        return files;
    }

    private static SnapshotManifest createManifest(
            SnapshotManifest source, String scale, long rowsPerFile,
            List<SnapshotManifest.TableState> tables, Path outputDirectory) throws IOException
    {
        SnapshotManifest manifest = new SnapshotManifest();
        manifest.createdAtUtc = Instant.now().toString();
        manifest.sourceHost = "hybench-index-only";
        manifest.sourceVnodeIds = new ArrayList<>(source.sourceVnodeIds);
        manifest.sourceIndexBucketIds = new ArrayList<>(source.sourceIndexBucketIds);
        manifest.sourceQuiesced = true;
        manifest.consistencyNote = "HyBench real primary keys; Pixels payload files intentionally omitted; "
                + "row locations follow the configured pixel.stride and explicit rows-per-file layout.";
        manifest.snapshotTimestamp = source.snapshotTimestamp;
        manifest.physicalIndexStateIncluded = true;
        manifest.semanticConfig = new LinkedHashMap<>(source.semanticConfig);
        manifest.tables = tables;

        Path stateRoot = outputDirectory.resolve("state/index");
        for (String rootName : Arrays.asList("rocksdb", "sqlite"))
        {
            Path root = stateRoot.resolve(rootName);
            if (!Files.isDirectory(root))
            {
                throw new IOException("Index state directory is missing: " + root);
            }
            try (java.util.stream.Stream<Path> paths = Files.walk(root))
            {
                paths.filter(Files::isRegularFile).forEach(path -> {
                    try
                    {
                        String relative = outputDirectory.relativize(path)
                                .toString().replace(java.io.File.separatorChar, '/');
                        manifest.artifactsSha256.put(relative, SnapshotIO.sha256(path));
                    }
                    catch (IOException e)
                    {
                        throw new ArtifactHashRuntimeException(e);
                    }
                });
            }
            catch (ArtifactHashRuntimeException e)
            {
                throw e.ioException;
            }
        }
        return manifest;
    }

    private static void writeBuildProperties(Path outputDirectory, String scale,
                                             Path dataDirectory, Path sourceSnapshot,
                                             long rowsPerFile, int pixelStride,
                                             int bucketCount,
                                             List<SnapshotManifest.TableState> tables) throws IOException
    {
        Properties properties = new Properties();
        properties.setProperty("format", "pixels-retina-hybench-index-state-v1");
        properties.setProperty("scale", scale);
        properties.setProperty("source.data", dataDirectory.toString());
        properties.setProperty("source.config.snapshot", sourceSnapshot.toString());
        properties.setProperty("rows.per.file", Long.toString(rowsPerFile));
        properties.setProperty("rows.per.row.group", Integer.toString(pixelStride));
        properties.setProperty("index.bucket.num", Integer.toString(bucketCount));
        properties.setProperty("table.count", Integer.toString(tables.size()));
        long total = 0L;
        for (SnapshotManifest.TableState table : tables)
        {
            properties.setProperty("table." + table.tableName + ".rows",
                    Long.toString(table.metadataRowCount));
            total = Math.addExact(total, table.metadataRowCount);
        }
        properties.setProperty("total.rows", Long.toString(total));
        try (java.io.OutputStream output = Files.newOutputStream(
                outputDirectory.resolve("hybench-index-state.properties")))
        {
            properties.store(output, "HyBench Index-only LOAD state");
        }
    }

    private static String firstCsvField(String line)
    {
        int separator = line.indexOf(',');
        String value = separator < 0 ? line : line.substring(0, separator);
        value = value.trim();
        if (value.isEmpty())
        {
            throw new IllegalArgumentException("HyBench primary key is empty in line: " + line);
        }
        return value;
    }

    private static int semanticInt(SnapshotManifest manifest, String key)
    {
        String value = manifest.semanticConfig.get(key);
        if (value == null || value.trim().isEmpty())
        {
            throw new IllegalArgumentException("source Snapshot semanticConfig lacks " + key);
        }
        return Integer.parseInt(value);
    }

    private static void ensureEmptyOutput(Path output) throws IOException
    {
        if (Files.exists(output) && !isEmptyDirectory(output))
        {
            throw new IOException("output directory must be empty: " + output);
        }
        Files.createDirectories(output);
    }

    private static boolean isEmptyDirectory(Path path) throws IOException
    {
        if (!Files.isDirectory(path))
        {
            return false;
        }
        try (java.nio.file.DirectoryStream<Path> entries = Files.newDirectoryStream(path))
        {
            return !entries.iterator().hasNext();
        }
    }

    private static final class TableSpec
    {
        private final String tableName;
        private final String fileName;
        private final int columnCount;
        private final String primaryKeyName;

        private TableSpec(String tableName, String fileName, int columnCount, String primaryKeyName)
        {
            this.tableName = tableName;
            this.fileName = fileName;
            this.columnCount = columnCount;
            this.primaryKeyName = primaryKeyName;
        }
    }

    private static final class ArtifactHashRuntimeException extends RuntimeException
    {
        private final IOException ioException;

        private ArtifactHashRuntimeException(IOException cause)
        {
            super(cause);
            this.ioException = cause;
        }
    }
}
