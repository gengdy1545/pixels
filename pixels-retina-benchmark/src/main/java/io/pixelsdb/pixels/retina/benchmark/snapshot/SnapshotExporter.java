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
package io.pixelsdb.pixels.retina.benchmark.snapshot;

import com.google.common.collect.ImmutableList;
import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.common.utils.IndexUtils;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkConfig;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotManifest.ColumnState;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotManifest.FileState;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotManifest.IndexState;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotManifest.LayoutState;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotManifest.PathState;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotManifest.RowGroupState;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotManifest.TableState;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.DirectoryStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Exports source-audited table, Pixels file/RG topology and physical index state.
 *
 * <p>Metadata/footer export is read-only. Optional physical RocksDB and
 * SQLite copying is rejected unless the operator
 * explicitly declares that every source writer has been quiesced; an online
 * directory copy would not be a consistent snapshot.</p>
 */
public final class SnapshotExporter
{
    private static final String[] SEMANTIC_CONFIG_KEYS = {
            "pixel.stride", "row.group.size", "block.size", "block.replication",
            "compact.factor", "column.chunk.little.endian", "column.chunk.alignment",
            "isnull.bitmap.alignment", "enabled.storage.schemes",
            "node.virtual.num", "index.bucket.num", "enabled.single.point.index.schemes",
            "enabled.main.index.scheme", "index.cache.enabled", "index.cache.capacity",
            "index.cache.expiration.seconds", "index.main.cache.bucket.num",
            "index.rocksdb.multicf", "index.rocksdb.write.buffer.size",
            "index.rocksdb.max.write.buffer.number", "index.rocksdb.max.background.flushes",
            "index.rocksdb.max.background.compactions", "index.rocksdb.max.open.files",
            "index.rocksdb.block.cache.capacity", "index.rocksdb.block.cache.shard.bits",
            "index.rocksdb.block.size", "index.rocksdb.min.write.buffer.number.to.merge",
            "index.rocksdb.level0.file.num.compaction.trigger", "index.rocksdb.max.bytes.for.level.base",
            "index.rocksdb.max.bytes.for.level.multiplier", "index.rocksdb.target.file.size.base",
            "index.rocksdb.target.file.size.multiplier", "index.rocksdb.prefix.length",
            "index.rocksdb.max.subcompactions", "index.rocksdb.compression.type",
            "index.rocksdb.bottommost.compression.type", "index.rocksdb.compaction.style",
            "index.rocksdb.stats.enabled",
            "retina.upsert-mode.enabled", "retina.tile.visibility.capacity",
            "retina.gc.interval", "retina.storage.gc.enabled", "retina.storage.gc.threshold",
            "retina.storage.gc.target.file.size",
            "retina.buffer.memTable.size", "retina.buffer.flush.count",
            "retina.buffer.object.flush.threads", "retina.buffer.flush.interval",
            "retina.buffer.flush.encodingLevel", "retina.buffer.flush.nullsPadding",
            "retina.buffer.object.storage.scheme"
    };
    private static final String[] ROCKSDB_REQUIRED_CONFIG = {
            "index.bucket.num", "index.rocksdb.multicf",
            "index.rocksdb.write.buffer.size", "index.rocksdb.max.write.buffer.number",
            "index.rocksdb.max.background.flushes", "index.rocksdb.max.background.compactions",
            "index.rocksdb.max.open.files", "index.rocksdb.block.cache.capacity",
            "index.rocksdb.block.cache.shard.bits", "index.rocksdb.block.size",
            "index.rocksdb.min.write.buffer.number.to.merge",
            "index.rocksdb.level0.file.num.compaction.trigger",
            "index.rocksdb.max.bytes.for.level.base",
            "index.rocksdb.max.bytes.for.level.multiplier",
            "index.rocksdb.target.file.size.base",
            "index.rocksdb.target.file.size.multiplier", "index.rocksdb.prefix.length",
            "index.rocksdb.max.subcompactions", "index.rocksdb.compression.type",
            "index.rocksdb.bottommost.compression.type", "index.rocksdb.compaction.style",
            "index.rocksdb.stats.enabled"
    };

    private SnapshotExporter()
    {
    }

    public static void run(BenchmarkConfig options) throws Exception
    {
        String pixelsHome = System.getenv("PIXELS_HOME");
        if (pixelsHome == null || pixelsHome.trim().isEmpty())
        {
            throw new IllegalStateException("PIXELS_HOME is not set; point it at the source "
                    + "Pixels deployment before exporting a snapshot");
        }
        java.nio.file.Path output = Paths.get(options.require("snapshot-output")).toAbsolutePath();
        String schemaName = options.require("snapshot-schema");
        List<String> tableNames = splitList(options.require("snapshot-tables"));
        if (tableNames.isEmpty())
        {
            throw new IllegalArgumentException("--snapshot-tables is empty");
        }
        if (Files.exists(output) && !isEmptyDirectory(output))
        {
            throw new IllegalArgumentException("snapshot output must be a new or empty directory: " + output);
        }
        Files.createDirectories(output);

        SnapshotManifest manifest = new SnapshotManifest();
        manifest.createdAtUtc = Instant.now().toString();
        manifest.sourceHost = options.get("snapshot-source-host", hostName());
        if (manifest.sourceHost.trim().isEmpty())
        {
            throw new IllegalArgumentException("--snapshot-source-host must not be empty");
        }
        manifest.sourceVnodeIds.addAll(parseIntList(options.get("snapshot-retina-vnodes", "")));
        manifest.sourceIndexBucketIds.addAll(parseIntList(options.get("snapshot-index-buckets", "")));
        manifest.sourceQuiesced = options.getBoolean("snapshot-source-quiesced", false);
        manifest.consistencyNote = manifest.sourceQuiesced
                ? "operator declared CDC/load/write-buffer/compaction/GC quiesced"
                : "logical metadata/footer export only; physical state must not be copied online";
        if (options.options().containsKey("snapshot-timestamp"))
        {
            throw new IllegalArgumentException("--snapshot-timestamp was removed; physical Index "
                    + "exports read trans_high_watermark from source etcd");
        }
        if (options.options().containsKey("snapshot-visibility-checkpoint"))
        {
            throw new IllegalArgumentException("--snapshot-visibility-checkpoint was removed; "
                    + "Visibility benchmarks always construct a clean baseline from footer topology");
        }
        manifest.snapshotTimestamp = 0L;
        captureSemanticConfig(manifest.semanticConfig);

        MetadataService metadata = MetadataService.Instance();
        PixelsFooterCache footerCache = new PixelsFooterCache();
        for (String tableName : tableNames)
        {
            TableState state = exportTable(metadata, footerCache, schemaName, tableName);
            manifest.tables.add(state);
        }
        copyOptionalPhysicalState(options, output, manifest);
        SnapshotIO.writeManifest(output, manifest);

        System.out.println("snapshot_directory=" + output);
        System.out.println("snapshot_tables=" + manifest.tables.size());
        System.out.println("source_quiesced=" + manifest.sourceQuiesced);
        System.out.println("physical_index_state=" + manifest.physicalIndexStateIncluded);
        System.out.println("snapshot_timestamp=" + manifest.snapshotTimestamp);
        for (TableState table : manifest.tables)
        {
            System.out.println("table." + table.tableName + ".id=" + table.tableId);
            System.out.println("table." + table.tableName + ".files=" + table.files.size());
        }
        System.out.println("status=exported");
    }

    private static TableState exportTable(MetadataService metadata, PixelsFooterCache footerCache,
                                          String schemaName, String tableName) throws Exception
    {
        Table table = metadata.getTable(schemaName, tableName);
        if (table == null)
        {
            throw new IllegalArgumentException("table does not exist: " + schemaName + "." + tableName);
        }
        List<Column> columns = metadata.getColumns(schemaName, tableName, true);
        if (columns.isEmpty())
        {
            columns = metadata.getColumns(schemaName, tableName, false);
        }

        TableState state = new TableState();
        state.schemaName = schemaName;
        state.schemaId = table.getSchemaId();
        state.tableName = tableName;
        state.tableId = table.getId();
        state.tableType = table.getType();
        state.storageScheme = table.getStorageScheme().name();
        state.metadataRowCount = table.getRowCount();
        for (int ordinal = 0; ordinal < columns.size(); ordinal++)
        {
            Column column = columns.get(ordinal);
            ColumnState columnState = new ColumnState();
            columnState.ordinal = ordinal;
            columnState.columnId = column.getId();
            columnState.name = column.getName();
            columnState.type = column.getType();
            columnState.cardinality = column.getCardinality();
            columnState.nullFraction = column.getNullFraction();
            state.columns.add(columnState);
        }

        List<SinglePointIndex> indexes = metadata.getSinglePointIndices(table.getId());
        if (indexes != null)
        {
            for (SinglePointIndex index : indexes)
            {
                IndexState exported = indexState(index, columns);
                state.indexes.add(exported);
                if (exported.primary)
                {
                    if (state.primaryIndex != null)
                    {
                        throw new IllegalStateException("table has more than one primary index: "
                                + schemaName + "." + tableName);
                    }
                    state.primaryIndex = exported;
                }
            }
        }
        SinglePointIndex primary = metadata.getPrimaryIndex(table.getId());
        if (primary != null && state.primaryIndex == null)
        {
            IndexState exported = indexState(primary, columns);
            state.indexes.add(exported);
            state.primaryIndex = exported;
        }

        List<Layout> layouts = metadata.getLayouts(schemaName, tableName);
        Layout latest = metadata.getLatestLayout(schemaName, tableName);
        long latestId = latest == null ? -1L : latest.getId();
        Set<Long> seenFiles = new HashSet<>();
        for (Layout layout : layouts)
        {
            LayoutState layoutState = new LayoutState();
            layoutState.layoutId = layout.getId();
            layoutState.tableId = layout.getTableId();
            layoutState.schemaVersionId = layout.getSchemaVersionId();
            layoutState.version = layout.getVersion();
            layoutState.createAt = layout.getCreateAt();
            layoutState.permission = layout.getPermission().name();
            layoutState.readable = layout.isReadable();
            layoutState.writable = layout.isWritable();
            layoutState.productionSelectedLatestLayout = layout.getId() == latestId;
            exportPaths(metadata, footerCache, state, layoutState, layout.getOrderedPaths(),
                    "ordered", seenFiles);
            exportPaths(metadata, footerCache, state, layoutState, layout.getCompactPaths(),
                    "compact", seenFiles);
            List<Path> projections = new ArrayList<>(layout.getProjectionPaths().values());
            projections.sort(Comparator.comparingLong(Path::getId));
            exportPaths(metadata, footerCache, state, layoutState, projections,
                    "projection", seenFiles);
            state.layouts.add(layoutState);
        }
        captureProductionWritePaths(state, latestId);

        return state;
    }

    private static void exportPaths(MetadataService metadata, PixelsFooterCache footerCache,
                                    TableState table, LayoutState layout, List<Path> paths,
                                    String role, Set<Long> seenFiles) throws Exception
    {
        for (int ordinal = 0; ordinal < paths.size(); ordinal++)
        {
            Path path = paths.get(ordinal);
            PathState pathState = new PathState();
            pathState.apiOrdinal = ordinal;
            pathState.pathId = path.getId();
            pathState.role = role;
            pathState.uri = path.getUri();
            pathState.productionSelectedByRetina = ordinal == 0
                    && ("ordered".equals(role) || "compact".equals(role));
            layout.paths.add(pathState);
            for (File file : metadata.getFiles(path.getId()))
            {
                if (seenFiles.add(file.getId()))
                {
                    table.files.add(readFile(metadata, footerCache, layout.layoutId, role, path, file));
                }
            }
        }
    }

    private static FileState readFile(MetadataService metadata, PixelsFooterCache footerCache,
                                      long layoutId, String role, Path path, File file) throws Exception
    {
        String fullUri = File.getFilePath(path, file);
        Storage storage = StorageFactory.Instance().getStorage(fullUri);
        long beforeLength = storage.getStatus(fullUri).getLength();
        FileState state = new FileState();
        state.fileId = file.getId();
        state.pathId = path.getId();
        state.layoutId = layoutId;
        state.layoutRole = role;
        state.name = file.getName();
        state.fullUri = fullUri;
        state.type = file.getType().name();
        state.metadataNumRowGroups = file.getNumRowGroup();
        state.minRowId = file.getMinRowId();
        state.maxRowId = file.getMaxRowId();
        state.fileSizeBytes = beforeLength;
        try (PixelsReader reader = newReader(storage, fullUri, footerCache))
        {
            state.footerRowCount = reader.getNumberOfRows();
            state.footerRowGroupCount = reader.getRowGroupNum();
            state.pixelStride = reader.getPixelStride();
            state.compressionKind = reader.getCompressionKind().name();
            state.fileVersion = reader.getFileVersion().name();
            List<PixelsProto.RowGroupInformation> infos = reader.getRowGroupInfos();
            for (int rgId = 0; rgId < infos.size(); rgId++)
            {
                PixelsProto.RowGroupInformation info = infos.get(rgId);
                RowGroupState rg = new RowGroupState();
                rg.rgId = rgId;
                rg.recordNum = info.getNumberOfRows();
                rg.dataLength = info.getDataLength();
                rg.footerOffset = info.getFooterOffset();
                rg.footerLength = info.getFooterLength();
                state.rowGroups.add(rg);
            }
        }
        long afterLength = storage.getStatus(fullUri).getLength();
        if (beforeLength != afterLength)
        {
            throw new IllegalStateException("Pixels file changed during snapshot export: " + fullUri);
        }
        if (state.metadataNumRowGroups != state.footerRowGroupCount)
        {
            throw new IllegalStateException("metadata/footer row-group mismatch for " + fullUri
                    + ": " + state.metadataNumRowGroups + " != " + state.footerRowGroupCount);
        }
        long summedRows = 0;
        for (RowGroupState rg : state.rowGroups)
        {
            summedRows += rg.recordNum;
        }
        if (summedRows != state.footerRowCount)
        {
            throw new IllegalStateException("footer row count mismatch for " + fullUri
                    + ": " + summedRows + " != " + state.footerRowCount);
        }
        long lookedUpId = metadata.getFileId(fullUri);
        if (lookedUpId != state.fileId)
        {
            throw new IllegalStateException("metadata file ID mismatch for " + fullUri
                    + ": " + lookedUpId + " != " + state.fileId);
        }
        return state;
    }

    private static IndexState indexState(SinglePointIndex index, List<Column> columns)
    {
        IndexState state = new IndexState();
        state.indexId = index.getId();
        state.scheme = index.getIndexScheme().name();
        state.primary = index.isPrimary();
        state.unique = index.isUnique();
        state.schemaVersionId = index.getSchemaVersionId();
        int keyBytes = 0;
        boolean fixedLength = true;
        int rocksPrefixBytes = 0;
        boolean rocksPrefixFixed = true;
        for (Integer columnId : index.getKeyColumns().getKeyColumnIds())
        {
            int ordinal = -1;
            for (int i = 0; i < columns.size(); i++)
            {
                if (columns.get(i).getId() == columnId)
                {
                    ordinal = i;
                    break;
                }
            }
            if (ordinal < 0)
            {
                throw new IllegalStateException("primary index column ID is absent from table schema: " + columnId);
            }
            Column column = columns.get(ordinal);
            state.keyColumnIds.add(columnId.longValue());
            state.keyColumnOrdinals.add(ordinal);
            state.keyColumnNames.add(column.getName());
            state.keyColumnTypes.add(column.getType());
            TypeDescription columnType = TypeDescription.fromString(column.getType());
            int columnBytes = fixedCanonicalLength(columnType);
            if (columnBytes < 0)
            {
                fixedLength = false;
            }
            else
            {
                keyBytes += columnBytes;
            }
            int prefixBytes = IndexUtils.keyLengthOf(columnType.getCategory().getExternalJavaType());
            if (prefixBytes < 0)
            {
                rocksPrefixFixed = false;
            }
            else
            {
                rocksPrefixBytes += prefixBytes;
            }
        }
        state.canonicalKeyBytes = fixedLength ? keyBytes : -1;
        state.rocksDbPrefixKeyBytes = rocksPrefixFixed ? rocksPrefixBytes : -1;
        return state;
    }

    private static void captureProductionWritePaths(TableState table, long latestId)
    {
        for (LayoutState layout : table.layouts)
        {
            if (layout.layoutId != latestId || !layout.writable)
            {
                continue;
            }
            table.productionWriteLayoutId = layout.layoutId;
            for (PathState path : layout.paths)
            {
                if (!path.productionSelectedByRetina)
                {
                    continue;
                }
                if ("ordered".equals(path.role))
                {
                    table.productionOrderedPathId = path.pathId;
                    table.productionOrderedPathUri = path.uri;
                }
                else if ("compact".equals(path.role))
                {
                    table.productionCompactPathId = path.pathId;
                    table.productionCompactPathUri = path.uri;
                }
            }
            return;
        }
    }

    private static void copyOptionalPhysicalState(BenchmarkConfig options, java.nio.file.Path output,
                                                  SnapshotManifest manifest) throws Exception
    {
        boolean copyIndex = options.getBoolean("snapshot-copy-index-state", false);
        if (copyIndex && !manifest.sourceQuiesced)
        {
            throw new IllegalArgumentException("physical state export requires "
                    + "--snapshot-source-quiesced true after all Retina writers have stopped");
        }
        if (copyIndex)
        {
            manifest.snapshotTimestamp = readSnapshotTimestampFromEtcd();
            requireSemanticConfig(manifest, "physical RocksDB snapshot",
                    ROCKSDB_REQUIRED_CONFIG);
            ConfigFactory config = ConfigFactory.Instance();
            String rocksSource = options.get("snapshot-rocksdb-dir",
                    config.getProperty("index.rocksdb.data.path"));
            String sqliteSource = options.get("snapshot-sqlite-dir",
                    config.getProperty("index.sqlite.path"));
            if (rocksSource == null || sqliteSource == null)
            {
                throw new IllegalArgumentException("RocksDB/SQLite source paths are not configured");
            }
            java.nio.file.Path rocksSourcePath = localDirectory(rocksSource);
            java.nio.file.Path sqliteSourcePath = localDirectory(sqliteSource);
            List<long[]> discoveredColumnFamilies = discoverIndexColumnFamilies(rocksSourcePath);
            validateColumnFamilyDescriptors(manifest, discoveredColumnFamilies);
            for (TableState table : manifest.tables)
            {
                if (table.primaryIndex != null
                        && "rocksdb".equalsIgnoreCase(table.primaryIndex.scheme))
                {
                    java.nio.file.Path mainIndex = sqliteSourcePath.resolve(
                            table.tableId + ".main.index.db");
                    if (!Files.isRegularFile(mainIndex) || Files.isSymbolicLink(mainIndex))
                    {
                        throw new IOException("source SQLite MainIndex is missing for "
                                + table.schemaName + "." + table.tableName + ": " + mainIndex);
                    }
                }
            }
            Set<Integer> discoveredBuckets = new LinkedHashSet<>();
            for (long[] ids : discoveredColumnFamilies)
            {
                discoveredBuckets.add((int) ids[2]);
            }
            if (manifest.sourceIndexBucketIds.isEmpty())
            {
                manifest.sourceIndexBucketIds.addAll(discoveredBuckets);
            }
            else if (!discoveredBuckets.isEmpty()
                    && !discoveredBuckets.containsAll(manifest.sourceIndexBucketIds))
            {
                throw new IllegalArgumentException("--snapshot-index-buckets contains buckets not present "
                        + "in source RocksDB: configured=" + manifest.sourceIndexBucketIds
                        + ", discovered=" + discoveredBuckets);
            }
            java.nio.file.Path rocksTarget = output.resolve("state/index/rocksdb");
            java.nio.file.Path sqliteTarget = output.resolve("state/index/sqlite");
            SnapshotIO.copyDirectory(rocksSourcePath, rocksTarget);
            SnapshotIO.copyDirectory(sqliteSourcePath, sqliteTarget);
            SnapshotIO.addDirectoryChecksums(output, rocksTarget, manifest.artifactsSha256);
            SnapshotIO.addDirectoryChecksums(output, sqliteTarget, manifest.artifactsSha256);
            manifest.physicalIndexStateIncluded = true;
        }
    }

    private static long readSnapshotTimestampFromEtcd()
    {
        KeyValue keyValue = EtcdUtil.Instance().getKeyValue(Constants.TRANS_HIGH_WATERMARK_KEY);
        if (keyValue == null)
        {
            throw new IllegalStateException("source etcd has no "
                    + Constants.TRANS_HIGH_WATERMARK_KEY
                    + "; start Transaction Service and wait for its watermark checkpoint");
        }
        String value = keyValue.getValue().toString(StandardCharsets.UTF_8);
        return snapshotTimestampFromHighWatermark(value);
    }

    static long snapshotTimestampFromHighWatermark(String value)
    {
        final long highWatermark;
        try
        {
            highWatermark = Long.parseLong(value);
        }
        catch (NumberFormatException e)
        {
            throw new IllegalStateException("source etcd contains an invalid "
                    + Constants.TRANS_HIGH_WATERMARK_KEY + ": " + value, e);
        }
        if (highWatermark <= 1L)
        {
            throw new IllegalStateException("source transaction high watermark must be greater than 1 "
                    + "for a physical Index snapshot, found " + highWatermark);
        }
        return highWatermark - 1L;
    }

    private static int fixedCanonicalLength(TypeDescription type)
    {
        switch (type.getCategory())
        {
            case BOOLEAN:
            case BYTE:
                return 1;
            case SHORT:
            case INT:
            case DATE:
            case TIME:
            case FLOAT:
                return 4;
            case LONG:
            case TIMESTAMP:
            case DOUBLE:
                return 8;
            case DECIMAL:
                return type.getPrecision() <= TypeDescription.MAX_SHORT_DECIMAL_PRECISION ? 8 : 16;
            default:
                return -1;
        }
    }

    private static PixelsReader newReader(Storage storage, String uri,
                                          PixelsFooterCache footerCache) throws IOException
    {
        return PixelsReaderImpl.newBuilder().setPath(uri).setStorage(storage).setEnableCache(false)
                .setCacheOrder(ImmutableList.of()).setPixelsCacheReader(null)
                .setPixelsFooterCache(footerCache).build();
    }

    private static void captureSemanticConfig(Map<String, String> target)
    {
        ConfigFactory config = ConfigFactory.Instance();
        for (String key : SEMANTIC_CONFIG_KEYS)
        {
            String value = config.getProperty(key);
            if (value != null)
            {
                target.put(key, value);
            }
        }
    }

    private static void requireSemanticConfig(SnapshotManifest manifest, String purpose,
                                               String... keys)
    {
        List<String> missing = new ArrayList<>();
        for (String key : keys)
        {
            String value = manifest.semanticConfig.get(key);
            if (value == null || value.trim().isEmpty())
            {
                missing.add(key);
            }
        }
        if (!missing.isEmpty())
        {
            throw new IllegalArgumentException(purpose + " requires source configuration keys "
                    + missing + "; point PIXELS_CONFIG at the real source deployment");
        }
    }

    private static List<String> splitList(String value)
    {
        Set<String> values = new LinkedHashSet<>();
        for (String part : value.split(","))
        {
            String trimmed = part.trim();
            if (!trimmed.isEmpty())
            {
                values.add(trimmed);
            }
        }
        return new ArrayList<>(values);
    }

    private static List<Integer> parseIntList(String value)
    {
        List<Integer> result = new ArrayList<>();
        Set<Integer> seen = new LinkedHashSet<>();
        if (value == null || value.trim().isEmpty())
        {
            return result;
        }
        for (String part : value.split(","))
        {
            int vnode = Integer.parseInt(part.trim());
            if (vnode < 0)
            {
                throw new IllegalArgumentException("snapshot vnode IDs must be non-negative");
            }
            if (seen.add(vnode))
            {
                result.add(vnode);
            }
        }
        return result;
    }

    private static boolean isEmptyDirectory(java.nio.file.Path path) throws IOException
    {
        if (!Files.isDirectory(path))
        {
            return false;
        }
        try (DirectoryStream<java.nio.file.Path> entries = Files.newDirectoryStream(path))
        {
            return !entries.iterator().hasNext();
        }
    }

    private static java.nio.file.Path localDirectory(String value)
    {
        if (value.startsWith("file:"))
        {
            return Paths.get(URI.create(value));
        }
        return Paths.get(value);
    }

    private static List<long[]> discoverIndexColumnFamilies(java.nio.file.Path rocksDirectory) throws Exception
    {
        List<long[]> columnFamilies = new ArrayList<>();
        RocksDB.loadLibrary();
        try (Options options = new Options())
        {
            for (byte[] columnFamily : RocksDB.listColumnFamilies(options,
                    rocksDirectory.toAbsolutePath().normalize().toString()))
            {
                long[] ids = IndexUtils.parseTableAndIndexId(columnFamily);
                if (ids != null)
                {
                    if (ids[2] < 0 || ids[2] > Integer.MAX_VALUE)
                    {
                        throw new IllegalStateException("invalid Index bucket in RocksDB column family: "
                                + ids[2]);
                    }
                    columnFamilies.add(ids);
                }
            }
        }
        return columnFamilies;
    }

    private static void validateColumnFamilyDescriptors(SnapshotManifest manifest,
                                                        List<long[]> columnFamilies)
    {
        Map<Long, Long> indexToTable = new HashMap<>();
        for (TableState table : manifest.tables)
        {
            for (IndexState index : table.indexes)
            {
                indexToTable.put(index.indexId, table.tableId);
            }
        }
        for (long[] ids : columnFamilies)
        {
            Long tableId = indexToTable.get(ids[1]);
            if (tableId == null || tableId != ids[0])
            {
                throw new IllegalArgumentException("RocksDB contains t" + ids[0] + "_i" + ids[1]
                        + " but snapshot metadata lacks that index descriptor; include every indexed table "
                        + "from this shared RocksDB in --snapshot-tables");
            }
        }
    }

    private static String hostName()
    {
        try
        {
            return InetAddress.getLocalHost().getHostName();
        }
        catch (Exception e)
        {
            return "unknown";
        }
    }

}
