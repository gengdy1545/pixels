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
import com.google.protobuf.ByteString;
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
import io.pixelsdb.pixels.common.utils.IndexUtils;
import io.pixelsdb.pixels.common.utils.RetinaUtils;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkConfig;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotManifest.ColumnState;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotManifest.FileState;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotManifest.IndexState;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotManifest.LayoutState;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotManifest.PathState;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotManifest.RowGroupState;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotManifest.TableState;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotSamples.IndexSample;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotSamples.RowSample;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.IOException;
import java.io.DataInputStream;
import java.io.BufferedInputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.DirectoryStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Exports source-audited table, Pixels file/RG and representative row/key state.
 *
 * <p>Metadata/footer/sample export is read-only. Optional physical RocksDB,
 * SQLite and Visibility checkpoint copying is rejected unless the operator
 * explicitly declares that every source writer has been quiesced; an online
 * directory copy would not be a consistent snapshot.</p>
 */
public final class SnapshotExporter
{
    private static final String[] CONFIG_KEYS = {
            "metadata.server.host", "metadata.server.port", "node.server.host", "node.server.port",
            "index.server.host", "index.server.port", "etcd.hosts", "etcd.port",
            "pixel.stride", "row.group.size", "block.size", "block.replication",
            "compact.factor", "column.chunk.little.endian", "column.chunk.alignment",
            "isnull.bitmap.alignment", "enabled.storage.schemes",
            "node.virtual.num", "index.bucket.num", "enabled.single.point.index.schemes",
            "enabled.main.index.scheme", "index.cache.enabled", "index.cache.capacity",
            "index.cache.expiration.seconds", "index.main.cache.bucket.num", "index.sqlite.path",
            "index.rocksdb.data.path", "index.rocksdb.multicf", "index.rocksdb.write.buffer.size",
            "index.rocksdb.max.write.buffer.number", "index.rocksdb.max.background.flushes",
            "index.rocksdb.max.background.compactions", "index.rocksdb.max.open.files",
            "index.rocksdb.block.cache.capacity", "index.rocksdb.block.cache.shard.bits",
            "index.rocksdb.block.size", "index.rocksdb.min.write.buffer.number.to.merge",
            "index.rocksdb.level0.file.num.compaction.trigger", "index.rocksdb.max.bytes.for.level.base",
            "index.rocksdb.max.bytes.for.level.multiplier", "index.rocksdb.target.file.size.base",
            "index.rocksdb.target.file.size.multiplier", "index.rocksdb.prefix.length",
            "index.rocksdb.max.subcompactions", "index.rocksdb.compression.type",
            "index.rocksdb.bottommost.compression.type", "index.rocksdb.compaction.style",
            "index.rocksdb.stats.enabled", "index.rocksdb.stats.interval",
            "index.rocksdb.stats.path", "index.rocksdb.log.interval",
            "index.rockset.data.path", "index.rockset.local.data.path",
            "index.rockset.persistent.cache.path", "index.rockset.persistent.cache.size.gb",
            "index.rockset.read.only", "index.rockset.prefix.length", "index.rockset.multicf",
            "retina.upsert-mode.enabled", "retina.tile.visibility.capacity",
            "retina.gc.interval", "retina.storage.gc.enabled", "retina.storage.gc.threshold",
            "retina.storage.gc.target.file.size", "retina.checkpoint.dir", "retina.checkpoint.threads",
            "retina.buffer.memTable.size", "retina.buffer.flush.count",
            "retina.buffer.object.flush.threads", "retina.buffer.flush.interval",
            "retina.buffer.flush.encodingLevel", "retina.buffer.flush.nullsPadding",
            "retina.buffer.object.storage.scheme", "retina.buffer.object.storage.folder",
            "hdfs.config.dir", "s3.connection.timeout.sec",
            "s3.connection.acquisition.timeout.sec", "s3.client.service.threads",
            "s3.max.request.concurrency", "s3.max.pending.requests",
            "s3.enable.async", "s3.use.async.client", "minio.region", "minio.endpoint"
    };
    private static final String[] WRITE_BUFFER_REQUIRED_CONFIG = {
            "retina.buffer.memTable.size", "retina.buffer.flush.count",
            "retina.buffer.object.flush.threads", "retina.buffer.flush.interval",
            "retina.buffer.flush.encodingLevel", "retina.buffer.flush.nullsPadding",
            "retina.buffer.object.storage.scheme", "retina.buffer.object.storage.folder",
            "block.size", "block.replication", "node.virtual.num",
            "index.main.cache.bucket.num", "retina.tile.visibility.capacity",
            "enabled.storage.schemes"
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
        java.nio.file.Path output = Paths.get(options.require("snapshot-output")).toAbsolutePath();
        String schemaName = options.require("snapshot-schema");
        List<String> tableNames = splitList(options.require("snapshot-tables"));
        if (tableNames.isEmpty())
        {
            throw new IllegalArgumentException("--snapshot-tables is empty");
        }
        int indexSampleCount = nonNegativeInt(options, "snapshot-index-samples", 0);
        int rowSampleCount = nonNegativeInt(options, "snapshot-row-samples", 0);
        String sampleLayout = options.get("snapshot-sample-layout", "ordered").toLowerCase();
        if (!"ordered".equals(sampleLayout) && !"compact".equals(sampleLayout))
        {
            throw new IllegalArgumentException("--snapshot-sample-layout must be ordered or compact");
        }
        long selectedLayoutId = options.getLong("snapshot-layout-id", 0L);

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
        manifest.snapshotTimestamp = options.getLong("snapshot-timestamp", 0L);
        if (indexSampleCount > 0 && manifest.snapshotTimestamp <= 0)
        {
            throw new IllegalArgumentException("index samples require an authoritative positive "
                    + "--snapshot-timestamp (the source transaction high watermark)");
        }
        captureEffectiveConfig(manifest.effectiveConfig);
        if (rowSampleCount > 0)
        {
            requireEffectiveConfig(manifest, "WriteBuffer snapshot",
                    WRITE_BUFFER_REQUIRED_CONFIG);
        }
        if (indexSampleCount > 0)
        {
            requireEffectiveConfig(manifest, "Index samples", "index.bucket.num");
        }

        MetadataService metadata = MetadataService.Instance();
        PixelsFooterCache footerCache = new PixelsFooterCache();
        for (String tableName : tableNames)
        {
            TableState state = exportTable(metadata, footerCache, schemaName, tableName,
                    sampleLayout, selectedLayoutId, indexSampleCount, rowSampleCount, output);
            if (state.maxObservedCreateTimestamp > manifest.snapshotTimestamp
                    && indexSampleCount > 0)
            {
                throw new IllegalStateException("sampled create timestamp "
                        + state.maxObservedCreateTimestamp + " exceeds --snapshot-timestamp "
                        + manifest.snapshotTimestamp + " for " + schemaName + "." + tableName);
            }
            manifest.tables.add(state);
            addArtifactChecksum(output, manifest, state.indexSamplesFile);
            addArtifactChecksum(output, manifest, state.rowSamplesFile);
        }
        copyOptionalPhysicalState(options, output, manifest);
        SnapshotIO.writeManifest(output, manifest);

        System.out.println("snapshot_directory=" + output);
        System.out.println("snapshot_tables=" + manifest.tables.size());
        System.out.println("source_quiesced=" + manifest.sourceQuiesced);
        System.out.println("physical_index_state=" + manifest.physicalIndexStateIncluded);
        System.out.println("visibility_checkpoint=" + manifest.visibilityCheckpointIncluded);
        if (manifest.visibilityCheckpointIncluded)
        {
            System.out.println("visibility_checkpoint_type=" + manifest.visibilityCheckpointType);
            System.out.println("visibility_checkpoint_host=" + manifest.visibilityCheckpointHost);
            System.out.println("visibility_checkpoint_timestamp="
                    + manifest.visibilityCheckpointTimestamp);
            System.out.println("visibility_checkpoint_coverage="
                    + manifest.visibilityCheckpointMatchedEntryCount + "/"
                    + manifest.visibilityCheckpointExpectedEntryCount);
        }
        for (TableState table : manifest.tables)
        {
            System.out.println("table." + table.tableName + ".id=" + table.tableId);
            System.out.println("table." + table.tableName + ".files=" + table.files.size());
            System.out.println("table." + table.tableName + ".index_samples=" + table.indexSampleCount);
            System.out.println("table." + table.tableName + ".row_samples=" + table.rowSampleCount);
        }
        System.out.println("status=exported");
    }

    private static TableState exportTable(MetadataService metadata, PixelsFooterCache footerCache,
                                           String schemaName, String tableName, String sampleLayout,
                                           long selectedLayoutId, int requestedIndexSamples,
                                           int requestedRowSamples, java.nio.file.Path output) throws Exception
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
        state.sampleLayout = sampleLayout;
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

        List<FileState> sampleFiles = new ArrayList<>();
        long selectedId = selectedLayoutId > 0 ? selectedLayoutId : latestReadableLayoutId(state);
        state.selectedSampleLayoutId = selectedId;
        boolean selectedReadable = false;
        for (LayoutState layout : state.layouts)
        {
            if (layout.layoutId == selectedId)
            {
                selectedReadable = layout.readable;
                break;
            }
        }
        if ((requestedIndexSamples > 0 || requestedRowSamples > 0) && !selectedReadable)
        {
            throw new IllegalArgumentException("selected sample layout is absent or not readable: "
                    + selectedId + " for " + schemaName + "." + tableName);
        }
        Set<Long> productionSamplePaths = new HashSet<>();
        for (LayoutState layout : state.layouts)
        {
            if (layout.layoutId != selectedId)
            {
                continue;
            }
            for (PathState path : layout.paths)
            {
                if (path.productionSelectedByRetina
                        && sampleLayout.equalsIgnoreCase(path.role))
                {
                    productionSamplePaths.add(path.pathId);
                }
            }
        }
        for (FileState file : state.files)
        {
            if (file.layoutId == selectedId && sampleLayout.equals(file.layoutRole)
                    && productionSamplePaths.contains(file.pathId))
            {
                sampleFiles.add(file);
                state.footerRowsInSelectedSampleLayout += file.footerRowCount;
            }
        }
        sampleFiles.sort(Comparator.comparingLong(file -> file.fileId));
        if ((requestedIndexSamples > 0 || requestedRowSamples > 0) && sampleFiles.isEmpty())
        {
            throw new IllegalStateException("no " + sampleLayout + " files in selected layout "
                    + selectedId + " for " + schemaName + "." + tableName);
        }
        if (requestedIndexSamples > 0 && state.primaryIndex == null)
        {
            throw new IllegalStateException("index samples requested but table has no primary index: "
                    + schemaName + "." + tableName);
        }

        int indexSamples = (int) Math.min((long) requestedIndexSamples,
                state.footerRowsInSelectedSampleLayout);
        int rowSamples = (int) Math.min((long) requestedRowSamples,
                state.footerRowsInSelectedSampleLayout);
        if (indexSamples > 0 || rowSamples > 0)
        {
            SampleResult samples = readSamples(state, sampleFiles, indexSamples, rowSamples);
            java.nio.file.Path tableDir = output.resolve("tables").resolve(
                    safeName(schemaName + "_" + tableName) + "-t" + state.tableId);
            if (indexSamples > 0)
            {
                java.nio.file.Path path = tableDir.resolve("index-samples.bin");
                SnapshotIO.writeIndexSamples(path, samples.indexSamples);
                state.indexSamplesFile = output.relativize(path).toString();
                state.indexSampleCount = samples.indexSamples.size();
            }
            if (rowSamples > 0)
            {
                java.nio.file.Path path = tableDir.resolve("row-samples.bin");
                SnapshotIO.writeRowSamples(path, samples.rowSamples, state.columns.size());
                state.rowSamplesFile = output.relativize(path).toString();
                state.rowSampleCount = samples.rowSamples.size();
            }
            state.maxObservedCreateTimestamp = samples.maxTimestamp;
        }
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

    private static SampleResult readSamples(TableState table, List<FileState> files,
                                            int indexCount, int rowCount) throws Exception
    {
        Map<Location, Target> targets = new LinkedHashMap<>();
        addTargets(files, table.footerRowsInSelectedSampleLayout, indexCount, true, targets);
        addTargets(files, table.footerRowsInSelectedSampleLayout, rowCount, false, targets);
        Map<Long, Map<Integer, List<Target>>> byFile = new HashMap<>();
        for (Target target : targets.values())
        {
            byFile.computeIfAbsent(target.location.fileId, ignored -> new HashMap<>())
                    .computeIfAbsent(target.location.rgId, ignored -> new ArrayList<>()).add(target);
        }
        Map<Long, FileState> fileById = new HashMap<>();
        for (FileState file : files)
        {
            fileById.put(file.fileId, file);
        }

        SampleResult result = new SampleResult();
        String[] names = new String[table.columns.size()];
        List<String> types = new ArrayList<>(table.columns.size());
        for (ColumnState column : table.columns)
        {
            names[column.ordinal] = column.name;
            types.add(column.type);
        }
        TypeDescription schema = TypeDescription.createSchemaFromStrings(Arrays.asList(names), types);
        for (Map.Entry<Long, Map<Integer, List<Target>>> fileTargets : byFile.entrySet())
        {
            FileState file = fileById.get(fileTargets.getKey());
            Storage storage = StorageFactory.Instance().getStorage(file.fullUri);
            try (PixelsReader reader = newReader(storage, file.fullUri, new PixelsFooterCache()))
            {
                for (Map.Entry<Integer, List<Target>> rgTargets : fileTargets.getValue().entrySet())
                {
                    readRowGroupSamples(table, schema, reader, file, rgTargets.getKey(),
                            rgTargets.getValue(), result, names);
                }
            }
        }
        result.indexSamples.sort(Comparator.comparingLong(sample -> sample.fileId * 31L
                + (long) sample.rgId * 17L + sample.rgRowOffset));
        return result;
    }

    private static void readRowGroupSamples(TableState table, TypeDescription schema,
                                            PixelsReader reader, FileState file, int rgId,
                                            List<Target> targets, SampleResult result,
                                            String[] columnNames) throws Exception
    {
        targets.sort(Comparator.comparingInt(target -> target.location.offset));
        Map<Integer, Target> offsets = new HashMap<>();
        for (Target target : targets)
        {
            offsets.put(target.location.offset, target);
        }
        boolean needsIndexTimestamp = false;
        for (Target target : targets)
        {
            needsIndexTimestamp |= target.index;
        }
        PixelsReaderOption option = new PixelsReaderOption().includeCols(columnNames)
                .rgRange(rgId, 1).skipCorruptRecords(false)
                .tolerantSchemaEvolution(false).enableEncodedColumnVector(false)
                .exposeHiddenColumn(needsIndexTimestamp);
        int rowOffset = 0;
        try (PixelsRecordReader recordReader = reader.read(option))
        {
            VectorizedRowBatch batch;
            while ((batch = recordReader.readBatch(true)) != null && batch.size > 0)
            {
                LongColumnVector hidden = batch.getHiddenColumnVector();
                if (needsIndexTimestamp && hidden == null)
                {
                    throw new IllegalStateException("indexed snapshot requires the real hidden commit "
                            + "timestamp column: " + file.fullUri + " RG " + rgId);
                }
                for (int logical = 0; logical < batch.size; logical++, rowOffset++)
                {
                    Target target = offsets.get(rowOffset);
                    if (target == null)
                    {
                        continue;
                    }
                    // PixelsRecordReader returns a dense batch; unlike Hive's
                    // VectorizedRowBatch this class has no selected[] indirection.
                    int row = logical;
                    long timestamp = hidden == null ? 0L
                            : hidden.vector[hidden.isRepeating() ? 0 : row];
                    result.maxTimestamp = Math.max(result.maxTimestamp, timestamp);
                    if (target.index)
                    {
                        ByteString key = buildIndexKey(table, schema, batch, row);
                        int bucket = IndexUtils.getBucketIdFromByteBuffer(key);
                        result.indexSamples.add(new IndexSample(key.toByteArray(), timestamp,
                                file.fileId, rgId, rowOffset, bucket));
                    }
                    if (target.row)
                    {
                        byte[][] values = new byte[table.columns.size()][];
                        for (int column = 0; column < values.length; column++)
                        {
                            ColumnVector vector = batch.cols[column];
                            int vectorRow = vector.isRepeating() ? 0 : row;
                            if (!vector.noNulls && vector.isNull[vectorRow])
                            {
                                values[column] = null;
                            }
                            else
                            {
                                values[column] = schema.getChildren().get(column)
                                        .convertColumnVectorToByte(vector, vectorRow);
                            }
                        }
                        result.rowSamples.add(new RowSample(values));
                    }
                }
                if (batch.endOfFile)
                {
                    break;
                }
            }
        }
        if (rowOffset != file.rowGroups.get(rgId).recordNum)
        {
            throw new IllegalStateException("reader returned " + rowOffset + " rows for "
                    + file.fullUri + " RG " + rgId + ", footer says "
                    + file.rowGroups.get(rgId).recordNum);
        }
    }

    private static ByteString buildIndexKey(TableState table, TypeDescription schema,
                                            VectorizedRowBatch batch, int row)
    {
        int length = 0;
        List<byte[]> parts = new ArrayList<>(table.primaryIndex.keyColumnOrdinals.size());
        for (Integer ordinal : table.primaryIndex.keyColumnOrdinals)
        {
            ColumnVector vector = batch.cols[ordinal];
            int vectorRow = vector.isRepeating() ? 0 : row;
            if (!vector.noNulls && vector.isNull[vectorRow])
            {
                throw new IllegalStateException("primary key column is null at ordinal " + ordinal);
            }
            byte[] value = schema.getChildren().get(ordinal).convertColumnVectorToByte(vector, vectorRow);
            parts.add(value);
            length += value.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(length);
        for (byte[] part : parts)
        {
            buffer.put(part);
        }
        return ByteString.copyFrom((ByteBuffer) buffer.flip());
    }

    private static void addTargets(List<FileState> files, long totalRows, int count,
                                   boolean index, Map<Location, Target> targets)
    {
        if (count <= 0)
        {
            return;
        }
        long[] ordinals = new long[count];
        for (int i = 0; i < count; i++)
        {
            ordinals[i] = ((2L * i + 1L) * totalRows) / (2L * count);
            if (ordinals[i] >= totalRows)
            {
                ordinals[i] = totalRows - 1L;
            }
        }
        int targetIndex = 0;
        long base = 0;
        for (FileState file : files)
        {
            for (RowGroupState rg : file.rowGroups)
            {
                long end = base + rg.recordNum;
                while (targetIndex < ordinals.length && ordinals[targetIndex] < end)
                {
                    int offset = (int) (ordinals[targetIndex] - base);
                    Location location = new Location(file.fileId, rg.rgId, offset);
                    Target target = targets.computeIfAbsent(location, Target::new);
                    if (index)
                    {
                        target.index = true;
                    }
                    else
                    {
                        target.row = true;
                    }
                    targetIndex++;
                }
                base = end;
            }
        }
        if (targetIndex != ordinals.length)
        {
            throw new IllegalStateException("failed to map all systematic sample ordinals");
        }
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
        String checkpoint = options.get("snapshot-visibility-checkpoint", null);
        if ((copyIndex || checkpoint != null) && !manifest.sourceQuiesced)
        {
            throw new IllegalArgumentException("physical state export requires "
                    + "--snapshot-source-quiesced true after all Retina writers have stopped");
        }
        if (copyIndex)
        {
            requireEffectiveConfig(manifest, "physical RocksDB snapshot",
                    ROCKSDB_REQUIRED_CONFIG);
            if (Boolean.parseBoolean(manifest.effectiveConfig.get("index.rocksdb.stats.enabled")))
            {
                requireEffectiveConfig(manifest, "RocksDB statistics",
                        "index.rocksdb.stats.interval", "index.rocksdb.stats.path",
                        "index.rocksdb.log.interval");
            }
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
        if (checkpoint != null)
        {
            requireEffectiveConfig(manifest, "Visibility checkpoint",
                    "retina.tile.visibility.capacity");
            if (manifest.snapshotTimestamp <= 0)
            {
                throw new IllegalArgumentException("Visibility checkpoint export requires its positive "
                        + "--snapshot-timestamp");
            }
            CheckpointIdentity identity = parseCheckpointIdentity(checkpoint);
            if (identity.timestamp != manifest.snapshotTimestamp)
            {
                throw new IllegalArgumentException("Visibility checkpoint filename timestamp "
                        + identity.timestamp + " does not equal --snapshot-timestamp "
                        + manifest.snapshotTimestamp);
            }
            java.nio.file.Path target = output.resolve("state/visibility/gc-checkpoint.bin");
            Files.createDirectories(target.getParent());
            Storage storage = StorageFactory.Instance().getStorage(checkpoint);
            try (DataInputStream input = storage.open(checkpoint);
                 OutputStream out = Files.newOutputStream(target))
            {
                byte[] buffer = new byte[8 * 1024 * 1024];
                int read;
                while ((read = input.read(buffer)) >= 0)
                {
                    if (read > 0)
                    {
                        out.write(buffer, 0, read);
                    }
                }
            }
            String relative = output.relativize(target).toString();
            manifest.artifactsSha256.put(relative, SnapshotIO.sha256(target));
            manifest.visibilityCheckpointIncluded = true;
            manifest.visibilityCheckpointSource = checkpoint;
            manifest.visibilityCheckpointType = identity.type;
            manifest.visibilityCheckpointHost = identity.host;
            manifest.visibilityCheckpointTimestamp = identity.timestamp;
            CheckpointStats stats = inspectVisibilityCheckpoint(target, manifest);
            manifest.visibilityCheckpointEntryCount = stats.total;
            manifest.visibilityCheckpointExpectedEntryCount = stats.expected;
            manifest.visibilityCheckpointMatchedEntryCount = stats.matched;
            manifest.visibilityCheckpointUnmatchedEntryCount = stats.unmatched;
        }
    }

    private static CheckpointStats inspectVisibilityCheckpoint(java.nio.file.Path checkpoint,
                                                               SnapshotManifest manifest)
            throws IOException
    {
        if (Files.size(checkpoint) > Integer.MAX_VALUE)
        {
            throw new IOException("Visibility checkpoint exceeds the current CheckpointFileIO 2 GiB limit: "
                    + checkpoint);
        }
        Map<String, RowGroupState> topology = new HashMap<>();
        for (TableState table : manifest.tables)
        {
            Set<Long> productionVisibilityPaths = new HashSet<>();
            for (LayoutState layout : table.layouts)
            {
                if (!layout.readable)
                {
                    continue;
                }
                for (PathState path : layout.paths)
                {
                    if (path.productionSelectedByRetina
                            && ("ordered".equalsIgnoreCase(path.role)
                            || "compact".equalsIgnoreCase(path.role)))
                    {
                        productionVisibilityPaths.add(path.pathId);
                    }
                }
            }
            for (FileState file : table.files)
            {
                if (!productionVisibilityPaths.contains(file.pathId))
                {
                    continue;
                }
                for (RowGroupState rg : file.rowGroups)
                {
                    String key = file.fileId + "_" + rg.rgId;
                    if (topology.put(key, rg) != null)
                    {
                        throw new IOException("duplicate production Visibility row group " + key);
                    }
                }
            }
        }
        if (topology.isEmpty())
        {
            throw new IOException("snapshot tables contain no production Visibility row groups");
        }
        int tileCapacity = Integer.parseInt(manifest.effectiveConfig.getOrDefault(
                "retina.tile.visibility.capacity", "10240"));
        if (tileCapacity <= 0 || tileCapacity > 65_536
                || tileCapacity % Long.SIZE != 0)
        {
            throw new IOException("invalid source retina.tile.visibility.capacity=" + tileCapacity);
        }

        CheckpointStats stats = new CheckpointStats();
        Set<String> entries = new HashSet<>();
        try (DataInputStream input = new DataInputStream(new BufferedInputStream(
                Files.newInputStream(checkpoint), 8 * 1024 * 1024)))
        {
            int count = input.readInt();
            if (count < 0)
            {
                throw new IOException("negative Visibility checkpoint entry count");
            }
            stats.total = count;
            for (int i = 0; i < count; i++)
            {
                long fileId = input.readLong();
                int rgId = input.readInt();
                int recordNum = input.readInt();
                int bitmapLength = input.readInt();
                if (fileId <= 0 || rgId < 0 || recordNum < 0 || bitmapLength < 0)
                {
                    throw new IOException("invalid Visibility checkpoint entry at ordinal " + i);
                }
                String key = fileId + "_" + rgId;
                if (!entries.add(key))
                {
                    throw new IOException("duplicate Visibility checkpoint entry: " + key);
                }
                RowGroupState expected = topology.get(key);
                if (expected == null)
                {
                    stats.unmatched++;
                }
                else
                {
                    long expectedBitmapLength = ((recordNum + (long) tileCapacity - 1L) / tileCapacity)
                            * (tileCapacity / Long.SIZE);
                    if (recordNum != expected.recordNum || bitmapLength != expectedBitmapLength)
                    {
                        throw new IOException("Visibility checkpoint/manifest mismatch for " + key
                                + ": recordNum=" + recordNum + "/" + expected.recordNum
                                + ", bitmapWords=" + bitmapLength + "/" + expectedBitmapLength);
                    }
                    stats.matched++;
                }
                for (int word = 0; word < bitmapLength; word++)
                {
                    input.readLong();
                }
            }
            if (input.read() != -1)
            {
                throw new IOException("Visibility checkpoint contains trailing bytes: " + checkpoint);
            }
        }
        stats.expected = topology.size();
        if (stats.matched != stats.expected)
        {
            throw new IOException("Visibility checkpoint covers " + stats.matched + " of "
                    + stats.expected + " production row groups from --snapshot-tables; "
                    + "use the checkpoint from the same quiesced Retina host and T_snap");
        }
        return stats;
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

    private static long latestReadableLayoutId(TableState table)
    {
        long id = -1;
        for (LayoutState layout : table.layouts)
        {
            if (layout.readable && layout.layoutId > id)
            {
                id = layout.layoutId;
            }
        }
        return id;
    }

    private static void captureEffectiveConfig(Map<String, String> target)
    {
        ConfigFactory config = ConfigFactory.Instance();
        for (String key : CONFIG_KEYS)
        {
            String value = config.getProperty(key);
            if (value != null)
            {
                target.put(key, value);
            }
        }
    }

    private static void requireEffectiveConfig(SnapshotManifest manifest, String purpose,
                                               String... keys)
    {
        List<String> missing = new ArrayList<>();
        for (String key : keys)
        {
            String value = manifest.effectiveConfig.get(key);
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

    private static void addArtifactChecksum(java.nio.file.Path output, SnapshotManifest manifest,
                                            String relative) throws IOException
    {
        if (relative != null)
        {
            manifest.artifactsSha256.put(relative, SnapshotIO.sha256(output.resolve(relative)));
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

    private static int nonNegativeInt(BenchmarkConfig options, String key, int defaultValue)
    {
        int value = options.getInt(key, defaultValue);
        if (value < 0)
        {
            throw new IllegalArgumentException("--" + key + " must not be negative");
        }
        return value;
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

    private static CheckpointIdentity parseCheckpointIdentity(String checkpoint)
    {
        String withoutQuery = checkpoint;
        int query = withoutQuery.indexOf('?');
        if (query >= 0)
        {
            withoutQuery = withoutQuery.substring(0, query);
        }
        int slash = Math.max(withoutQuery.lastIndexOf('/'), withoutQuery.lastIndexOf('\\'));
        String fileName = slash >= 0 ? withoutQuery.substring(slash + 1) : withoutQuery;
        String type;
        String prefix;
        if (fileName.startsWith(RetinaUtils.CHECKPOINT_PREFIX_GC))
        {
            type = "gc";
            prefix = RetinaUtils.CHECKPOINT_PREFIX_GC;
        }
        else if (fileName.startsWith(RetinaUtils.CHECKPOINT_PREFIX_OFFLOAD))
        {
            type = "offload";
            prefix = RetinaUtils.CHECKPOINT_PREFIX_OFFLOAD;
        }
        else
        {
            throw new IllegalArgumentException("Visibility checkpoint must retain the production "
                    + "vis_gc_<host>_<timestamp>.bin or vis_offload_<host>_<timestamp>.bin filename: "
                    + checkpoint);
        }
        if (!fileName.endsWith(RetinaUtils.CHECKPOINT_SUFFIX))
        {
            throw new IllegalArgumentException("Visibility checkpoint does not end in "
                    + RetinaUtils.CHECKPOINT_SUFFIX + ": " + checkpoint);
        }
        String stem = fileName.substring(0,
                fileName.length() - RetinaUtils.CHECKPOINT_SUFFIX.length());
        int timestampSeparator = stem.lastIndexOf('_');
        if (timestampSeparator < prefix.length()
                || timestampSeparator == stem.length() - 1)
        {
            throw new IllegalArgumentException("invalid Visibility checkpoint filename: " + checkpoint);
        }
        long timestamp;
        try
        {
            timestamp = Long.parseLong(stem.substring(timestampSeparator + 1));
        }
        catch (NumberFormatException e)
        {
            throw new IllegalArgumentException("invalid Visibility checkpoint timestamp in "
                    + checkpoint, e);
        }
        if (timestamp <= 0)
        {
            throw new IllegalArgumentException("Visibility checkpoint timestamp must be positive: "
                    + checkpoint);
        }
        String host = stem.substring(prefix.length(), timestampSeparator);
        if (host.isEmpty())
        {
            throw new IllegalArgumentException("Visibility checkpoint hostname is empty: "
                    + checkpoint);
        }
        return new CheckpointIdentity(type, host, timestamp);
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

    private static String safeName(String value)
    {
        return value.replaceAll("[^A-Za-z0-9_.-]", "_");
    }

    private static final class SampleResult
    {
        private final List<IndexSample> indexSamples = new ArrayList<>();
        private final List<RowSample> rowSamples = new ArrayList<>();
        private long maxTimestamp;
    }

    private static final class CheckpointStats
    {
        private long total;
        private long expected;
        private long matched;
        private long unmatched;
    }

    private static final class CheckpointIdentity
    {
        private final String type;
        private final String host;
        private final long timestamp;

        private CheckpointIdentity(String type, String host, long timestamp)
        {
            this.type = type;
            this.host = host;
            this.timestamp = timestamp;
        }
    }

    private static final class Location
    {
        private final long fileId;
        private final int rgId;
        private final int offset;

        private Location(long fileId, int rgId, int offset)
        {
            this.fileId = fileId;
            this.rgId = rgId;
            this.offset = offset;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other) return true;
            if (!(other instanceof Location)) return false;
            Location that = (Location) other;
            return fileId == that.fileId && rgId == that.rgId && offset == that.offset;
        }

        @Override
        public int hashCode()
        {
            int result = Long.valueOf(fileId).hashCode();
            result = 31 * result + rgId;
            result = 31 * result + offset;
            return result;
        }
    }

    private static final class Target
    {
        private final Location location;
        private boolean index;
        private boolean row;

        private Target(Location location)
        {
            this.location = location;
        }
    }
}
