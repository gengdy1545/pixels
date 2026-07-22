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
import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.MainIndexFactory;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.common.index.SinglePointIndexFactory;
import io.pixelsdb.pixels.common.index.service.IndexService;
import io.pixelsdb.pixels.common.index.service.IndexServiceProvider;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.IndexUtils;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.index.rocksdb.RocksDBFactory;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkConfig;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotIO;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotManifest;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotRuntime;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Builds a persistent RocksDB + SQLite Index state without creating Pixels
 * payload files.
 *
 * <p>The builder uses the same LocalIndexService batch APIs as the production
 * load/Retina paths. It only replaces the payload writer and row allocator with
 * deterministic keys, row ids and opaque RowLocations. The resulting state is
 * intended for Index-component experiments; RowLocations must not be used to
 * read Pixels files.</p>
 */
public final class IndexStateBuilder
{
    private static final int DEFAULT_BATCH_SIZE = 4096;
    private static final long DEFAULT_ROWS_PER_FILE = 1_000_000L;
    private static final int NON_UNIQUE_GROUP_SIZE = 32;
    private static final long KEY_DOMAIN = 0x4958442d53544154L;

    private IndexStateBuilder()
    {
    }

    public static void run(BenchmarkConfig config) throws Exception
    {
        Path snapshotDirectory = Paths.get(config.require("snapshot-dir"))
                .toAbsolutePath().normalize();
        Path outputDirectory = Paths.get(config.require("index-state-dir"))
                .toAbsolutePath().normalize();
        SnapshotManifest manifest = SnapshotIO.readManifest(snapshotDirectory);
        SnapshotManifest.TableState table = SnapshotIO.requireTable(
                manifest, config.require("snapshot-table"));
        long rowCount = config.getLong("index-entry-count", config.dataSize());
        int batchSize = config.getInt("index-build-batch-size", DEFAULT_BATCH_SIZE);
        long rowsPerFile = config.getLong("index-rows-per-file", DEFAULT_ROWS_PER_FILE);
        long rowsPerRowGroup = config.getLong("index-rows-per-row-group",
                semanticLong(manifest, "pixel.stride", 10000L));
        if (rowCount <= 0 || batchSize <= 0 || rowsPerFile <= 0 || rowsPerRowGroup <= 0
                || rowsPerRowGroup > Integer.MAX_VALUE || rowsPerFile < rowsPerRowGroup)
        {
            throw new IllegalArgumentException("invalid Index builder sizing options");
        }

        try (SnapshotRuntime runtime = SnapshotRuntime.open(config))
        {
            runtime.prepareEmptyIndexState(outputDirectory);
            registerIndexKeyLengths(manifest);
            BuildSummary summary = build(table, manifest, rowCount, batchSize,
                    rowsPerFile, (int) rowsPerRowGroup);
            writeStateProperties(outputDirectory, snapshotDirectory, table, summary,
                    manifest.snapshotTimestamp);
            System.out.println("index_state_build_completed=" + outputDirectory);
            System.out.println("index_state_table=" + table.schemaName + "." + table.tableName);
            System.out.println("index_state_entries=" + summary.entries);
            System.out.println("index_state_indexes=" + summary.indexes);
            System.out.println("index_state_files=" + summary.files);
        }
    }

    private static BuildSummary build(SnapshotManifest.TableState table,
                                      SnapshotManifest manifest, long rowCount,
                                      int batchSize, long rowsPerFile,
                                      int rowsPerRowGroup) throws Exception
    {
        if (table.primaryIndex == null || !table.primaryIndex.primary
                || !table.primaryIndex.unique)
        {
            throw new IllegalArgumentException("Index builder requires a unique primary index for "
                    + table.schemaName + "." + table.tableName);
        }
        for (SnapshotManifest.IndexState index : table.indexes)
        {
            if (!"rocksdb".equalsIgnoreCase(index.scheme))
            {
                throw new IllegalArgumentException("Index builder currently requires RocksDB indexes; index "
                        + index.indexId + " uses " + index.scheme);
            }
        }

        ConfigFactory config = ConfigFactory.Instance();
        int bucketCount = positiveConfig(config, "index.bucket.num");
        IndexOption[] options = new IndexOption[bucketCount];
        for (int bucket = 0; bucket < bucketCount; bucket++)
        {
            options[bucket] = IndexOption.builder().vNodeId(bucket).build();
        }

        SinglePointIndexFactory singlePointFactory = SinglePointIndexFactory.Instance();
        if (!singlePointFactory.isSchemeEnabled(SinglePointIndex.Scheme.rocksdb))
        {
            throw new IllegalStateException("Index builder requires RocksDB to be enabled");
        }
        MainIndexFactory mainIndexFactory = MainIndexFactory.Instance();
        if (!mainIndexFactory.isSchemeEnabled(MainIndex.Scheme.sqlite))
        {
            throw new IllegalStateException("Index builder requires SQLite MainIndex");
        }

        List<SnapshotManifest.IndexState> openedIndexes = new ArrayList<>();
        for (SnapshotManifest.IndexState index : table.indexes)
        {
            SinglePointIndexFactory.TableIndex descriptor =
                    new SinglePointIndexFactory.TableIndex(table.tableId, index.indexId,
                            SinglePointIndex.Scheme.rocksdb, index.unique);
            for (int bucket = 0; bucket < bucketCount; bucket++)
            {
                singlePointFactory.getSinglePointIndex(descriptor, options[bucket]);
            }
            openedIndexes.add(index);
        }

        IndexService indexService = IndexServiceProvider.getService(
                IndexServiceProvider.ServiceMode.local);
        long timestamp = Math.addExact(manifest.snapshotTimestamp, 1L);
        BuildSummary summary = new BuildSummary(table.indexes.size());
        try
        {
            long fileCount = (rowCount + rowsPerFile - 1L) / rowsPerFile;
            for (long fileOrdinal = 0; fileOrdinal < fileCount; fileOrdinal++)
            {
                long fileStart = Math.multiplyExact(fileOrdinal, rowsPerFile);
                long fileEnd = Math.min(rowCount, Math.addExact(fileStart, rowsPerFile));
                long cursor = fileStart;
                while (cursor < fileEnd)
                {
                    long batchEnd = Math.min(fileEnd, Math.addExact(cursor, batchSize));
                    List<IndexProto.PrimaryIndexEntry>[] primaryByBucket = primaryBuckets(bucketCount);
                    @SuppressWarnings("unchecked")
                    List<IndexProto.SecondaryIndexEntry>[] secondaryByBucket =
                            (List<IndexProto.SecondaryIndexEntry>[]) new List<?>[bucketCount];
                    for (int bucket = 0; bucket < bucketCount; bucket++)
                    {
                        primaryByBucket[bucket] = new ArrayList<>((int) (batchEnd - cursor));
                        secondaryByBucket[bucket] = new ArrayList<>((int) (batchEnd - cursor));
                    }

                    for (long rowOrdinal = cursor; rowOrdinal < batchEnd; rowOrdinal++)
                    {
                        long rowId = rowOrdinal;
                        IndexProto.RowLocation location = location(rowId, rowsPerFile,
                                rowsPerRowGroup);
                        for (SnapshotManifest.IndexState index : table.indexes)
                        {
                            ByteString key = syntheticKey(index, rowId);
                            IndexProto.IndexKey indexKey = IndexProto.IndexKey.newBuilder()
                                    .setTableId(table.tableId)
                                    .setIndexId(index.indexId)
                                    .setKey(key)
                                    .setTimestamp(timestamp)
                                    .build();
                            int bucket = IndexUtils.getBucketIdFromByteBuffer(key);
                            if (index.primary)
                            {
                                primaryByBucket[bucket].add(IndexProto.PrimaryIndexEntry.newBuilder()
                                        .setIndexKey(indexKey)
                                        .setRowId(rowId)
                                        .setRowLocation(location)
                                        .build());
                            }
                            else
                            {
                                secondaryByBucket[bucket].add(IndexProto.SecondaryIndexEntry.newBuilder()
                                        .setIndexKey(indexKey)
                                        .setRowId(rowId)
                                        .build());
                            }
                        }
                    }

                    for (int bucket = 0; bucket < bucketCount; bucket++)
                    {
                        if (!primaryByBucket[bucket].isEmpty())
                        {
                            if (!indexService.putPrimaryIndexEntries(table.tableId,
                                    table.primaryIndex.indexId, primaryByBucket[bucket], options[bucket]))
                            {
                                throw new IndexException("failed to insert primary Index entries in bucket "
                                        + bucket);
                            }
                        }
                        for (SnapshotManifest.IndexState index : table.indexes)
                        {
                            if (index.primary || secondaryByBucket[bucket].isEmpty())
                            {
                                continue;
                            }
                            List<IndexProto.SecondaryIndexEntry> entries = new ArrayList<>(
                                    secondaryByBucket[bucket].size());
                            for (IndexProto.SecondaryIndexEntry entry : secondaryByBucket[bucket])
                            {
                                if (entry.getIndexKey().getIndexId() == index.indexId)
                                {
                                    entries.add(entry);
                                }
                            }
                            if (!entries.isEmpty()
                                    && !indexService.putSecondaryIndexEntries(table.tableId,
                                    index.indexId, entries, options[bucket]))
                            {
                                throw new IndexException("failed to insert secondary Index entries for index "
                                        + index.indexId + " in bucket " + bucket);
                            }
                        }
                    }
                    summary.entries += batchEnd - cursor;
                    cursor = batchEnd;
                }

                long fileId = Math.addExact(fileOrdinal, 1L);
                if (!indexService.flushIndexEntriesOfFile(table.tableId,
                        table.primaryIndex.indexId, fileId, true, options[0]))
                {
                    throw new IndexException("failed to flush MainIndex file " + fileId);
                }
                summary.files++;
            }
            verifyFirstEntry(indexService, table, options, rowsPerFile, rowsPerRowGroup, timestamp);
        }
        finally
        {
            Exception closeFailure = null;
            for (int i = openedIndexes.size() - 1; i >= 0; i--)
            {
                SnapshotManifest.IndexState index = openedIndexes.get(i);
                try
                {
                    indexService.closeIndex(table.tableId, index.indexId, index.primary, options[0]);
                }
                catch (Exception e)
                {
                    if (closeFailure == null)
                    {
                        closeFailure = e;
                    }
                    else
                    {
                        closeFailure.addSuppressed(e);
                    }
                }
            }
            if (closeFailure != null)
            {
                throw closeFailure;
            }
        }
        return summary;
    }

    private static void verifyFirstEntry(IndexService service,
                                         SnapshotManifest.TableState table,
                                         IndexOption[] options,
                                         long rowsPerFile,
                                         int rowsPerRowGroup,
                                         long timestamp) throws Exception
    {
        long rowId = 0L;
        for (SnapshotManifest.IndexState index : table.indexes)
        {
            ByteString key = syntheticKey(index, rowId);
            IndexProto.IndexKey indexKey = IndexProto.IndexKey.newBuilder()
                    .setTableId(table.tableId).setIndexId(index.indexId)
                    .setKey(key).setTimestamp(timestamp).build();
            int bucket = IndexUtils.getBucketIdFromByteBuffer(key);
            IndexProto.RowLocation expected = location(rowId, rowsPerFile, rowsPerRowGroup);
            if (index.primary || index.unique)
            {
                IndexProto.RowLocation actual = service.lookupUniqueIndex(indexKey, options[bucket]);
                if (!expected.equals(actual))
                {
                    throw new IndexException("built Index verification failed for index " + index.indexId
                            + ": expected=" + expected + ", actual=" + actual);
                }
            }
            else
            {
                List<IndexProto.RowLocation> actual = service.lookupNonUniqueIndex(
                        indexKey, options[bucket]);
                if (actual == null || !actual.contains(expected))
                {
                    throw new IndexException("built secondary Index verification failed for index "
                            + index.indexId);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static List<IndexProto.PrimaryIndexEntry>[] primaryBuckets(int bucketCount)
    {
        return (List<IndexProto.PrimaryIndexEntry>[]) new List<?>[bucketCount];
    }

    private static IndexProto.RowLocation location(long rowId, long rowsPerFile,
                                                   int rowsPerRowGroup)
    {
        long withinFile = rowId % rowsPerFile;
        long rgId = withinFile / rowsPerRowGroup;
        if (rgId > Integer.MAX_VALUE)
        {
            throw new IllegalArgumentException("synthetic RowLocation rgId exceeds int range");
        }
        return IndexProto.RowLocation.newBuilder()
                .setFileId(rowId / rowsPerFile + 1L)
                .setRgId((int) rgId)
                .setRgRowOffset((int) (withinFile % rowsPerRowGroup))
                .build();
    }

    private static ByteString syntheticKey(SnapshotManifest.IndexState index, long rowId)
    {
        int length = index.canonicalKeyBytes > 0
                ? index.canonicalKeyBytes : Math.max(16, index.rocksDbPrefixKeyBytes);
        if (length <= 0)
        {
            length = 16;
        }
        long identity = index.unique ? rowId : rowId / NON_UNIQUE_GROUP_SIZE;
        byte[] bytes = new byte[length];
        long value = mix64(KEY_DOMAIN ^ index.indexId ^ identity);
        for (int offset = 0; offset < bytes.length; offset++)
        {
            if ((offset & 7) == 0)
            {
                value = mix64(value + offset + 0x9e3779b97f4a7c15L);
            }
            bytes[offset] = (byte) (value >>> ((offset & 7) * 8));
        }
        if (length >= Long.BYTES)
        {
            ByteBuffer.wrap(bytes).putLong(0, identity);
        }
        else if (length >= Integer.BYTES)
        {
            ByteBuffer.wrap(bytes).putInt(0, (int) identity);
        }
        else
        {
            bytes[0] = (byte) identity;
        }
        return ByteString.copyFrom(bytes);
    }

    private static long mix64(long value)
    {
        value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
        return value ^ (value >>> 31);
    }

    private static void registerIndexKeyLengths(SnapshotManifest manifest)
    {
        boolean multiCf = Boolean.parseBoolean(requiredSemanticConfig(manifest,
                "index.rocksdb.multicf"));
        int configuredPrefix = Integer.parseInt(requiredSemanticConfig(manifest,
                "index.rocksdb.prefix.length"));
        for (SnapshotManifest.TableState table : manifest.tables)
        {
            for (SnapshotManifest.IndexState index : table.indexes)
            {
                if (!"rocksdb".equalsIgnoreCase(index.scheme))
                {
                    continue;
                }
                int logicalPrefix = index.rocksDbPrefixKeyBytes;
                if (logicalPrefix <= 0)
                {
                    logicalPrefix = multiCf ? configuredPrefix : configuredPrefix - Long.BYTES;
                }
                if (logicalPrefix <= 0)
                {
                    throw new IllegalArgumentException("invalid RocksDB prefix length for index "
                            + index.indexId);
                }
                RocksDBFactory.registerIndexKeyLength(index.indexId, logicalPrefix);
            }
        }
    }

    private static String requiredSemanticConfig(SnapshotManifest manifest, String key)
    {
        String value = manifest.semanticConfig.get(key);
        if (value == null || value.trim().isEmpty())
        {
            throw new IllegalArgumentException("snapshot semanticConfig lacks " + key);
        }
        return value;
    }

    private static long semanticLong(SnapshotManifest manifest, String key, long fallback)
    {
        String value = manifest.semanticConfig.get(key);
        return value == null || value.trim().isEmpty() ? fallback : Long.parseLong(value);
    }

    private static int positiveConfig(ConfigFactory config, String key)
    {
        String value = config.getProperty(key);
        if (value == null)
        {
            throw new IllegalArgumentException("snapshot semanticConfig lacks " + key);
        }
        int result = Integer.parseInt(value);
        if (result <= 0)
        {
            throw new IllegalArgumentException(key + " must be positive");
        }
        return result;
    }

    private static void writeStateProperties(Path outputDirectory, Path snapshotDirectory,
                                             SnapshotManifest.TableState table,
                                             BuildSummary summary, long timestamp)
            throws IOException
    {
        Properties properties = new Properties();
        properties.setProperty("format", "pixels-retina-index-state-v1");
        properties.setProperty("source.snapshot", snapshotDirectory.toString());
        properties.setProperty("schema", table.schemaName);
        properties.setProperty("table", table.tableName);
        properties.setProperty("table.id", Long.toString(table.tableId));
        properties.setProperty("snapshot.timestamp", Long.toString(timestamp - 1L));
        properties.setProperty("entry.count", Long.toString(summary.entries));
        properties.setProperty("index.count", Integer.toString(summary.indexes));
        properties.setProperty("file.count", Long.toString(summary.files));
        Path metadata = outputDirectory.resolve("index-state.properties");
        try (OutputStream output = Files.newOutputStream(metadata))
        {
            properties.store(output, "Pixels Retina benchmark Index state");
        }
    }

    private static final class BuildSummary
    {
        private final int indexes;
        private long entries;
        private long files;

        private BuildSummary(int indexes)
        {
            this.indexes = indexes;
        }
    }
}
