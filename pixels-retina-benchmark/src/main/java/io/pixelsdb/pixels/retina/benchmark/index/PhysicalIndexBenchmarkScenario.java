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
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkPhase;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkResult;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkScenario;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkWorker;
import io.pixelsdb.pixels.retina.benchmark.common.OperationRange;
import io.pixelsdb.pixels.retina.benchmark.common.OperationResult;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotManifest;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotRuntime;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Benchmarks one physical LocalIndexService batch operation against a disposable
 * RocksDB + SQLite copy restored from a Snapshot v2 manifest.
 */
public final class PhysicalIndexBenchmarkScenario implements BenchmarkScenario
{
    private static final int TIMESTAMP_SLOTS_PER_OPERATION = 3;
    private static final int MAX_KEY_ATTEMPTS = 1_000_000;

    private final IndexOperation operation;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicReference<String> firstError = new AtomicReference<>();
    private final AtomicLong verifiedSamples = new AtomicLong();
    private final AtomicLong persistedFixtureRanges = new AtomicLong();
    private final AtomicLong warmedFixtureRanges = new AtomicLong();
    private final Map<Long, boolean[]> openedBuckets = new ConcurrentHashMap<>();
    private final Map<Long, Set<ByteString>> reservedKeys = new ConcurrentHashMap<>();

    private BenchmarkConfig config;
    private SnapshotRuntime runtime;
    private SnapshotManifest manifest;
    private SnapshotManifest.TableState table;
    private SnapshotManifest.IndexState primaryIndex;
    private SnapshotManifest.IndexState targetIndex;
    private SinglePointIndexFactory indexFactory;
    private IndexService indexService;
    private IndexOption[] indexOptions;
    private int[] usableBuckets;
    private int bucketCount;
    private int rowsPerRowGroup;
    private long snapshotTimestamp;
    private SyntheticRowLayout rowLayout;
    private MainIndexState mainIndexState;
    private int syntheticKeyBytes;

    public PhysicalIndexBenchmarkScenario(IndexOperation operation)
    {
        if (operation == null)
        {
            throw new IllegalArgumentException("index operation is null");
        }
        this.operation = operation;
    }

    @Override
    public String name()
    {
        return operation.commandName();
    }

    @Override
    public void setup(BenchmarkConfig benchmarkConfig) throws Exception
    {
        if (config != null)
        {
            throw new IllegalStateException("physical index scenario is already set up");
        }
        this.config = benchmarkConfig;
        this.mainIndexState = MainIndexState.parse(
                benchmarkConfig.get("main-index-state", MainIndexState.WARM_CACHE.optionValue()));
        /* Both options are deliberately required for every physical Index command. */
        benchmarkConfig.require("snapshot-dir");
        String requestedTable = benchmarkConfig.require("snapshot-table");

        this.runtime = SnapshotRuntime.open(benchmarkConfig);
        this.manifest = runtime.manifest();
        this.table = runtime.requireTable(requestedTable);
        validateSnapshot();
        this.primaryIndex = table.primaryIndex;
        this.targetIndex = operation.isPrimary()
                ? primaryIndex : selectSecondaryIndex(benchmarkConfig.require("snapshot-secondary-index"));
        validateIndexDescriptor(targetIndex);

        /* The immutable source is copied before any RocksDB/SQLite singleton opens. */
        runtime.prepareIndexState(table);
        registerSnapshotIndexKeyLengths();

        this.bucketCount = positiveConfig("index.bucket.num");
        this.usableBuckets = usableBuckets();
        this.indexOptions = new IndexOption[bucketCount];
        for (int bucket = 0; bucket < bucketCount; bucket++)
        {
            indexOptions[bucket] = IndexOption.builder().vNodeId(bucket).build();
        }

        Path sqlite = runtime.workDirectory().resolve("index/sqlite")
                .resolve(table.tableId + ".main.index.db");
        this.rowsPerRowGroup = representativeRowsPerRowGroup();
        this.rowLayout = new SyntheticRowLayout(
                Math.max(readNextRowId(sqlite), nextTopologyRowId()),
                Math.addExact(maximumFileId(), 1L),
                benchmarkConfig.warmupOperations(),
                benchmarkConfig.dataSize(),
                rowsPerRowGroup);
        validateGeneratedDomains();

        this.indexFactory = SinglePointIndexFactory.Instance();
        if (!indexFactory.isSchemeEnabled(SinglePointIndex.Scheme.rocksdb))
        {
            throw new IllegalStateException("physical index benchmark requires a fresh JVM with RocksDB enabled");
        }
        MainIndexFactory mainFactory = MainIndexFactory.Instance();
        if (!mainFactory.isSchemeEnabled(MainIndex.Scheme.sqlite))
        {
            throw new IllegalStateException("physical index benchmark requires SQLite MainIndex");
        }
        this.indexService = IndexServiceProvider.getService(IndexServiceProvider.ServiceMode.local);
    }

    @Override
    public void preparePhase(BenchmarkPhase phase, long totalOperations)
    {
        ensureReady();
        long expected = phase == BenchmarkPhase.WARMUP
                ? config.warmupOperations() : config.dataSize();
        if (totalOperations != expected)
        {
            throw new IllegalArgumentException("unexpected " + phase + " operation count: "
                    + totalOperations + ", expected " + expected);
        }
    }

    @Override
    public void completePreparation(BenchmarkPhase phase) throws Exception
    {
        ensureReady();
        if (!usesTimedMainIndexFixture() || mainIndexState == MainIndexState.HOT_BUFFER)
        {
            return;
        }

        long fixtureFileId = rowLayout.fileId(phase, SyntheticRowLayout.Role.OLD);
        boolean flushed = indexService.flushIndexEntriesOfFile(
                table.tableId, primaryIndex.indexId, fixtureFileId, true,
                indexOptions[usableBuckets[0]]);
        if (!flushed)
        {
            throw new IllegalStateException("failed to flush MainIndex fixture file " + fixtureFileId);
        }
        persistedFixtureRanges.addAndGet(rowLayout.rangeCount(phase));

        if (mainIndexState == MainIndexState.COLD_START)
        {
            MainIndexFactory.Instance().closeIndex(table.tableId, false);
            MainIndex reopened = MainIndexFactory.Instance().getMainIndex(table.tableId);
            if (reopened == null)
            {
                throw new IllegalStateException("failed to reopen cold MainIndex");
            }
            return;
        }

        MainIndex mainIndex = MainIndexFactory.Instance().getMainIndex(table.tableId);
        if (mainIndex == null)
        {
            throw new IllegalStateException("MainIndex is unavailable while warming fixture ranges");
        }
        long ranges = rowLayout.rangeCount(phase);
        for (long range = 0; range < ranges; range++)
        {
            long operationId = Math.multiplyExact(range, (long) rowLayout.rowsPerRowGroup());
            long rowId = rowLayout.rowId(phase, operationId, SyntheticRowLayout.Role.OLD);
            IndexProto.RowLocation expected = rowLayout.location(
                    phase, operationId, SyntheticRowLayout.Role.OLD);
            IndexProto.RowLocation actual = mainIndex.getLocation(rowId);
            if (!expected.equals(actual))
            {
                throw new IllegalStateException("failed to warm MainIndex fixture row " + rowId);
            }
        }
        warmedFixtureRanges.addAndGet(ranges);
    }

    @Override
    public BenchmarkWorker createWorker(BenchmarkPhase phase, int workerId, int clientId,
                                        OperationRange operationRange)
    {
        ensureReady();
        return new PhysicalIndexWorker(phase, workerId);
    }

    @Override
    public void completePhase(BenchmarkPhase phase, BenchmarkResult result)
    {
        // Warmup and measurement intentionally share the restored working copy
        // but use disjoint deterministic key, row-id, and timestamp namespaces.
    }

    @Override
    public Map<String, String> details()
    {
        Map<String, String> details = new LinkedHashMap<>();
        details.put("operation", operation.name());
        details.put("call", localCallName());
        details.put("snapshotTable", table == null ? "not-initialized"
                : table.schemaName + "." + table.tableName);
        details.put("tableId", table == null ? "0" : Long.toString(table.tableId));
        details.put("indexId", targetIndex == null ? "0" : Long.toString(targetIndex.indexId));
        details.put("indexDescriptor", targetIndex == null ? "not-initialized"
                : descriptorName(targetIndex));
        details.put("indexUnique", targetIndex == null ? "false"
                : Boolean.toString(targetIndex.unique));
        details.put("snapshotTimestamp", Long.toString(snapshotTimestamp));
        details.put("singlePointIndex", "restored RocksDB working copy");
        details.put("mainIndex", "restored SQLite working copy");
        details.put("mainIndexState", mainIndexState == null ? "not-initialized"
                : mainIndexState.optionValue());
        details.put("effectiveMainIndexState", usesTimedMainIndexFixture()
                ? mainIndexState.optionValue() : "not-applicable");
        details.put("syntheticRowIdLayout", "contiguous old/new domains; disjoint warmup/measurement");
        details.put("syntheticRowLocationLayout", "distinct old/new and warmup/measurement file IDs");
        details.put("persistedFixtureRanges", Long.toString(persistedFixtureRanges.get()));
        details.put("warmedFixtureRanges", Long.toString(warmedFixtureRanges.get()));
        details.put("workingCopy", runtime == null ? "not-initialized"
                : runtime.workDirectory().toString());
        details.put("bucketCount", Integer.toString(bucketCount));
        details.put("usableBuckets", bucketList());
        details.put("bucketFunction", "IndexUtils.getBucketIdFromByteBuffer (SHA-256)");
        details.put("batching", "one same-bucket LocalIndexService batch API call per execute group");
        details.put("keyBytes", targetIndex == null ? "0"
                : targetIndex.canonicalKeyBytes > 0
                ? Integer.toString(targetIndex.canonicalKeyBytes)
                : "variable/" + syntheticKeyBytes);
        details.put("fixture", fixtureDescription());
        details.put("warmupMeasurementNamespaces", "disjoint");
        details.put("verifiedSamples", Long.toString(verifiedSamples.get()));
        String error = firstError.get();
        if (error != null)
        {
            details.put("firstError", error);
        }
        return details;
    }

    @Override
    public void close() throws Exception
    {
        if (!closed.compareAndSet(false, true))
        {
            return;
        }
        Exception first = null;
        if (indexService != null && targetIndex != null && targetIndex.indexId != primaryIndex.indexId)
        {
            first = closeIndex(targetIndex, false, first);
        }
        if (indexService != null && primaryIndex != null)
        {
            first = closeIndex(primaryIndex, true, first);
        }
        if (runtime != null)
        {
            try
            {
                runtime.close();
            }
            catch (Exception e)
            {
                first = append(first, e);
            }
        }
        if (first != null)
        {
            throw first;
        }
    }

    private void validateSnapshot()
    {
        if (!manifest.sourceQuiesced || !manifest.physicalIndexStateIncluded)
        {
            throw new IllegalArgumentException("physical Index benchmark requires a quiesced Snapshot v2 "
                    + "with RocksDB and SQLite state");
        }
        if (manifest.snapshotTimestamp <= 0)
        {
            throw new IllegalArgumentException("snapshotTimestamp must be positive");
        }
        this.snapshotTimestamp = manifest.snapshotTimestamp;
        if (table.primaryIndex == null || !table.primaryIndex.primary || !table.primaryIndex.unique)
        {
            throw new IllegalArgumentException("snapshot table requires a unique primary index");
        }
        validateIndexDescriptor(table.primaryIndex);
        if (table.files == null || table.files.isEmpty())
        {
            throw new IllegalArgumentException("snapshot table has no physical file/RG topology");
        }
    }

    private static void validateIndexDescriptor(SnapshotManifest.IndexState index)
    {
        if (index == null || index.indexId <= 0 || !"rocksdb".equalsIgnoreCase(index.scheme))
        {
            throw new IllegalArgumentException("physical Index benchmark requires RocksDB index descriptors");
        }
        if (index.canonicalKeyBytes == 0 || index.canonicalKeyBytes < -1
                || index.rocksDbPrefixKeyBytes == 0 || index.rocksDbPrefixKeyBytes < -1)
        {
            throw new IllegalArgumentException("invalid key length descriptor for index " + index.indexId);
        }
    }

    private SnapshotManifest.IndexState selectSecondaryIndex(String selector)
    {
        List<SnapshotManifest.IndexState> matches = new ArrayList<>();
        Long requestedId = parseLong(selector);
        for (SnapshotManifest.IndexState index : table.indexes)
        {
            if (index.primary)
            {
                continue;
            }
            if ((requestedId != null && index.indexId == requestedId)
                    || descriptorMatches(index, selector))
            {
                matches.add(index);
            }
        }
        if (matches.size() != 1)
        {
            throw new IllegalArgumentException("--snapshot-secondary-index must identify exactly one "
                    + "secondary index by ID or key-column name; selector=" + selector
                    + ", matches=" + matches.size());
        }
        return matches.get(0);
    }

    private static boolean descriptorMatches(SnapshotManifest.IndexState index, String selector)
    {
        String normalized = selector.trim().toLowerCase(Locale.ROOT);
        if (descriptorName(index).toLowerCase(Locale.ROOT).equals(normalized))
        {
            return true;
        }
        return index.keyColumnNames.size() == 1
                && index.keyColumnNames.get(0).equalsIgnoreCase(selector.trim());
    }

    private static String descriptorName(SnapshotManifest.IndexState index)
    {
        return String.join(",", index.keyColumnNames);
    }

    private static Long parseLong(String value)
    {
        try
        {
            return Long.valueOf(value);
        }
        catch (NumberFormatException ignored)
        {
            return null;
        }
    }

    private void registerSnapshotIndexKeyLengths()
    {
        boolean multiCf = Boolean.parseBoolean(requiredSemanticConfig("index.rocksdb.multicf"));
        int configuredPrefix = Integer.parseInt(requiredSemanticConfig("index.rocksdb.prefix.length"));
        Set<Long> registered = new HashSet<>();
        for (SnapshotManifest.TableState manifestTable : manifest.tables)
        {
            for (SnapshotManifest.IndexState index : manifestTable.indexes)
            {
                if (!"rocksdb".equalsIgnoreCase(index.scheme) || !registered.add(index.indexId))
                {
                    continue;
                }
                int logicalPrefix = index.rocksDbPrefixKeyBytes;
                if (logicalPrefix <= 0)
                {
                    /*
                     * Variable-width source indexes used the configured default
                     * extractor. Register an equivalent logical length so the
                     * factory never falls back to Metadata Service.
                     */
                    logicalPrefix = multiCf ? configuredPrefix : configuredPrefix - Long.BYTES;
                }
                if (logicalPrefix <= 0)
                {
                    throw new IllegalArgumentException("cannot reproduce RocksDB prefix extractor for index "
                            + index.indexId + " from index.rocksdb.prefix.length=" + configuredPrefix);
                }
                RocksDBFactory.registerIndexKeyLength(index.indexId, logicalPrefix);
            }
        }
        int selectedLength = targetIndex.canonicalKeyBytes;
        if (selectedLength <= 0)
        {
            int logicalPrefix = targetIndex.rocksDbPrefixKeyBytes > 0
                    ? targetIndex.rocksDbPrefixKeyBytes
                    : multiCf ? configuredPrefix : configuredPrefix - Long.BYTES;
            this.syntheticKeyBytes = Math.max(16, logicalPrefix);
        }
        else
        {
            this.syntheticKeyBytes = selectedLength;
        }
    }

    private String requiredSemanticConfig(String key)
    {
        String value = manifest.semanticConfig.get(key);
        if (value == null || value.trim().isEmpty())
        {
            throw new IllegalArgumentException("snapshot semanticConfig lacks " + key);
        }
        return value;
    }

    private int positiveConfig(String key)
    {
        String value = ConfigFactory.Instance().getProperty(key);
        if (value == null)
        {
            throw new IllegalArgumentException("snapshot semanticConfig lacks " + key);
        }
        int parsed = Integer.parseInt(value);
        if (parsed <= 0)
        {
            throw new IllegalArgumentException(key + " must be positive");
        }
        return parsed;
    }

    private int[] usableBuckets()
    {
        if (manifest.sourceIndexBucketIds == null || manifest.sourceIndexBucketIds.isEmpty())
        {
            int[] all = new int[bucketCount];
            for (int i = 0; i < all.length; i++)
            {
                all[i] = i;
            }
            return all;
        }
        List<Integer> buckets = new ArrayList<>(manifest.sourceIndexBucketIds);
        Collections.sort(buckets);
        int[] result = new int[buckets.size()];
        for (int i = 0; i < buckets.size(); i++)
        {
            int bucket = buckets.get(i);
            if (bucket < 0 || bucket >= bucketCount)
            {
                throw new IllegalArgumentException("snapshot source Index bucket " + bucket
                        + " is outside index.bucket.num=" + bucketCount);
            }
            result[i] = bucket;
        }
        return result;
    }

    private long maximumFileId()
    {
        long maximum = 0L;
        for (SnapshotManifest.FileState file : table.files)
        {
            maximum = Math.max(maximum, file.fileId);
        }
        if (maximum <= 0)
        {
            throw new IllegalArgumentException("snapshot file topology has no positive file ID");
        }
        return maximum;
    }

    private long nextTopologyRowId()
    {
        long maximum = -1L;
        for (SnapshotManifest.FileState file : table.files)
        {
            maximum = Math.max(maximum, file.maxRowId);
        }
        return maximum < 0 ? 0L : Math.addExact(maximum, 1L);
    }

    private int representativeRowsPerRowGroup()
    {
        List<Integer> counts = new ArrayList<>();
        for (SnapshotManifest.FileState file : table.files)
        {
            for (SnapshotManifest.RowGroupState rowGroup : file.rowGroups)
            {
                if (rowGroup.rgId < 0 || rowGroup.recordNum < 0)
                {
                    throw new IllegalArgumentException("snapshot contains invalid row-group topology");
                }
                if (rowGroup.recordNum > 0)
                {
                    counts.add(rowGroup.recordNum);
                }
            }
        }
        if (counts.isEmpty())
        {
            throw new IllegalArgumentException("snapshot contains no non-empty row group");
        }
        counts.sort(Comparator.naturalOrder());
        return counts.get(counts.size() / 2);
    }

    private void validateGeneratedDomains()
    {
        long operations = Math.addExact(config.warmupOperations(), config.dataSize());
        long timestampSlots = Math.multiplyExact(operations, TIMESTAMP_SLOTS_PER_OPERATION);
        Math.addExact(snapshotTimestamp, Math.addExact(timestampSlots, 2L));
    }

    private static long readNextRowId(Path sqlitePath) throws Exception
    {
        Class.forName("org.sqlite.JDBC");
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + sqlitePath);
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT MAX(row_id_end) FROM row_id_ranges"))
        {
            if (!result.next())
            {
                return 0L;
            }
            long next = result.getLong(1);
            return result.wasNull() ? 0L : next;
        }
    }

    private void ensureIndexOpen(SnapshotManifest.IndexState index, int bucket) throws Exception
    {
        boolean[] states = openedBuckets.computeIfAbsent(index.indexId,
                ignored -> new boolean[bucketCount]);
        if (states[bucket])
        {
            return;
        }
        synchronized (states)
        {
            if (!states[bucket])
            {
                SinglePointIndexFactory.TableIndex descriptor =
                        new SinglePointIndexFactory.TableIndex(table.tableId, index.indexId,
                                SinglePointIndex.Scheme.rocksdb, index.unique);
                indexFactory.getSinglePointIndex(descriptor, indexOptions[bucket]);
                states[bucket] = true;
            }
        }
    }

    private ByteString newAbsentKey(SnapshotManifest.IndexState index, int bucket,
                                    long identity, long queryTimestamp) throws Exception
    {
        ensureIndexOpen(index, bucket);
        Set<ByteString> reserved = reservedKeys.computeIfAbsent(index.indexId,
                ignored -> Collections.newSetFromMap(new ConcurrentHashMap<ByteString, Boolean>()));
        int length = index.canonicalKeyBytes > 0 ? index.canonicalKeyBytes
                : variableSyntheticKeyBytes(index);
        for (int salt = 0; salt < MAX_KEY_ATTEMPTS; salt++)
        {
            ByteString candidate = syntheticKey(length, index.indexId, identity, salt);
            if (IndexUtils.getBucketIdFromByteBuffer(candidate) != bucket || !reserved.add(candidate))
            {
                continue;
            }
            IndexProto.IndexKey lookup = indexKey(index, candidate, queryTimestamp);
            boolean absent = index.unique
                    ? indexService.lookupUniqueIndex(lookup, indexOptions[bucket]) == null
                    : indexService.lookupNonUniqueIndex(lookup, indexOptions[bucket]) == null;
            if (absent)
            {
                return candidate;
            }
        }
        throw new IllegalArgumentException("unable to synthesize a unique absent "
                + length + "-byte key in bucket " + bucket + " for index " + index.indexId);
    }

    private int variableSyntheticKeyBytes(SnapshotManifest.IndexState index)
    {
        if (index == targetIndex)
        {
            return syntheticKeyBytes;
        }
        boolean multiCf = Boolean.parseBoolean(requiredSemanticConfig("index.rocksdb.multicf"));
        int prefix = index.rocksDbPrefixKeyBytes;
        if (prefix <= 0)
        {
            int configured = Integer.parseInt(requiredSemanticConfig("index.rocksdb.prefix.length"));
            prefix = multiCf ? configured : configured - Long.BYTES;
        }
        return Math.max(16, prefix);
    }

    private static ByteString syntheticKey(int length, long indexId, long identity, int salt)
    {
        byte[] bytes = new byte[length];
        long value = mix64(identity ^ Long.rotateLeft(indexId, 17)
                ^ ((long) salt * 0x9e3779b97f4a7c15L));
        for (int offset = 0; offset < bytes.length; offset++)
        {
            if ((offset & 7) == 0)
            {
                value = mix64(value + offset + 0x9e3779b97f4a7c15L);
            }
            bytes[offset] = (byte) (value >>> ((offset & 7) * 8));
        }
        return ByteString.copyFrom(bytes);
    }

    private static long mix64(long value)
    {
        value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
        return value ^ (value >>> 31);
    }

    private IndexProto.IndexKey indexKey(SnapshotManifest.IndexState index, ByteString key,
                                         long timestamp)
    {
        return IndexProto.IndexKey.newBuilder()
                .setTableId(table.tableId)
                .setIndexId(index.indexId)
                .setKey(key)
                .setTimestamp(timestamp)
                .build();
    }

    private long globalOperation(BenchmarkPhase phase, long operationId)
    {
        return rowLayout.globalOperation(phase, operationId);
    }

    private long fixtureTimestamp(long globalOperation)
    {
        return Math.addExact(snapshotTimestamp, Math.addExact(
                Math.multiplyExact(globalOperation, TIMESTAMP_SLOTS_PER_OPERATION), 1L));
    }

    private long timedTimestamp(long globalOperation)
    {
        return Math.addExact(fixtureTimestamp(globalOperation), 1L);
    }

    private long keyIdentity(long globalOperation, int role)
    {
        return Math.addExact(Math.multiplyExact(globalOperation, 16L), role);
    }

    private IndexProto.PrimaryIndexEntry primaryEntry(ByteString key, long timestamp,
                                                      long rowId, IndexProto.RowLocation location)
    {
        return IndexProto.PrimaryIndexEntry.newBuilder()
                .setIndexKey(indexKey(primaryIndex, key, timestamp))
                .setRowId(rowId)
                .setRowLocation(location)
                .build();
    }

    private IndexProto.SecondaryIndexEntry secondaryEntry(ByteString key, long timestamp,
                                                          long rowId)
    {
        return IndexProto.SecondaryIndexEntry.newBuilder()
                .setIndexKey(indexKey(targetIndex, key, timestamp))
                .setRowId(rowId)
                .build();
    }

    private PreparedInvocation prepareInvocation(BenchmarkPhase phase, long firstOperation,
                                                 int count, int bucket) throws Exception
    {
        ensureIndexOpen(primaryIndex, bucket);
        ensureIndexOpen(targetIndex, bucket);
        PreparedInvocation invocation = new PreparedInvocation(firstOperation, count, bucket);
        List<IndexProto.PrimaryIndexEntry> primaryFixtures = new ArrayList<>();
        List<IndexProto.SecondaryIndexEntry> secondaryFixtures = new ArrayList<>();

        for (int i = 0; i < count; i++)
        {
            long operationId = firstOperation + i;
            long global = globalOperation(phase, operationId);
            long fixtureTs = fixtureTimestamp(global);
            long timedTs = timedTimestamp(global);
            ByteString targetKey = newAbsentKey(targetIndex, bucket,
                    keyIdentity(global, 1), timedTs);

            if (operation.isPrimary())
            {
                long oldRowId = rowLayout.rowId(
                        phase, operationId, SyntheticRowLayout.Role.OLD);
                IndexProto.RowLocation oldLocation = rowLayout.location(
                        phase, operationId, SyntheticRowLayout.Role.OLD);
                if (operation.requiresExistingKey())
                {
                    primaryFixtures.add(primaryEntry(targetKey, fixtureTs, oldRowId, oldLocation));
                    invocation.expectedPreviousLocations.add(oldLocation);
                }
                if (operation == IndexOperation.PUT_PRIMARY)
                {
                    invocation.primaryEntries.add(primaryEntry(targetKey, timedTs,
                            rowLayout.rowId(phase, operationId, SyntheticRowLayout.Role.NEW),
                            rowLayout.location(phase, operationId, SyntheticRowLayout.Role.NEW)));
                }
                else if (operation == IndexOperation.UPDATE_PRIMARY)
                {
                    invocation.primaryEntries.add(primaryEntry(targetKey, timedTs,
                            rowLayout.rowId(phase, operationId, SyntheticRowLayout.Role.NEW),
                            rowLayout.location(phase, operationId, SyntheticRowLayout.Role.NEW)));
                }
                else
                {
                    invocation.keys.add(indexKey(primaryIndex, targetKey, timedTs));
                }
            }
            else
            {
                long oldRowId = rowLayout.rowId(
                        phase, operationId, SyntheticRowLayout.Role.OLD);
                long newRowId = operation == IndexOperation.UPDATE_SECONDARY
                        ? rowLayout.rowId(phase, operationId, SyntheticRowLayout.Role.NEW)
                        : oldRowId;
                ByteString oldPrimaryKey = newAbsentKey(primaryIndex, bucket,
                        keyIdentity(global, 2), fixtureTs);
                primaryFixtures.add(primaryEntry(oldPrimaryKey, fixtureTs,
                        oldRowId, rowLayout.location(
                                phase, operationId, SyntheticRowLayout.Role.OLD)));
                if (newRowId != oldRowId)
                {
                    ByteString newPrimaryKey = newAbsentKey(primaryIndex, bucket,
                            keyIdentity(global, 3), timedTs);
                    primaryFixtures.add(primaryEntry(newPrimaryKey, fixtureTs,
                            newRowId, rowLayout.location(
                                    phase, operationId, SyntheticRowLayout.Role.NEW)));
                }
                if (operation.requiresExistingKey())
                {
                    secondaryFixtures.add(secondaryEntry(targetKey, fixtureTs, oldRowId));
                    invocation.expectedPreviousRowIds.add(oldRowId);
                }
                if (operation == IndexOperation.DELETE_SECONDARY)
                {
                    invocation.keys.add(indexKey(targetIndex, targetKey, timedTs));
                }
                else
                {
                    invocation.secondaryEntries.add(secondaryEntry(targetKey, timedTs, newRowId));
                }
            }
        }

        if (!primaryFixtures.isEmpty())
        {
            boolean put = indexService.putPrimaryIndexEntries(table.tableId, primaryIndex.indexId,
                    primaryFixtures, indexOptions[bucket]);
            if (!put)
            {
                throw new IllegalStateException("failed to prepare primary fixture in bucket " + bucket);
            }
        }
        if (!secondaryFixtures.isEmpty())
        {
            boolean put = indexService.putSecondaryIndexEntries(table.tableId, targetIndex.indexId,
                    secondaryFixtures, indexOptions[bucket]);
            if (!put)
            {
                throw new IllegalStateException("failed to prepare secondary fixture in bucket " + bucket);
            }
        }
        return invocation;
    }

    private void verifySample(PreparedInvocation invocation) throws Exception
    {
        if (invocation.returnedLocations != null
                && !invocation.expectedPreviousLocations.equals(invocation.returnedLocations))
        {
            throw new IllegalStateException("sampled previous RowLocation mismatch: expected="
                    + invocation.expectedPreviousLocations + ", actual=" + invocation.returnedLocations);
        }
        if (invocation.returnedRowIds != null
                && !invocation.expectedPreviousRowIds.equals(invocation.returnedRowIds))
        {
            throw new IllegalStateException("sampled previous row-id mismatch: expected="
                    + invocation.expectedPreviousRowIds + ", actual=" + invocation.returnedRowIds);
        }

        int sample = 0;
        if (operation.isPrimary())
        {
            IndexProto.IndexKey key = operation == IndexOperation.DELETE_PRIMARY
                    ? invocation.keys.get(sample)
                    : invocation.primaryEntries.get(sample).getIndexKey();
            IndexProto.RowLocation actual = indexService.lookupUniqueIndex(
                    key, indexOptions[invocation.bucket]);
            if (operation == IndexOperation.DELETE_PRIMARY)
            {
                if (actual != null)
                {
                    throw new IllegalStateException("sampled primary key remains visible after delete");
                }
            }
            else
            {
                IndexProto.RowLocation expected = invocation.primaryEntries.get(sample).getRowLocation();
                if (!expected.equals(actual))
                {
                    throw new IllegalStateException("sampled primary target state mismatch: expected="
                            + expected + ", actual=" + actual);
                }
            }
        }
        else
        {
            IndexProto.IndexKey key = operation == IndexOperation.DELETE_SECONDARY
                    ? invocation.keys.get(sample)
                    : invocation.secondaryEntries.get(sample).getIndexKey();
            List<IndexProto.RowLocation> actual = secondaryLocations(key, invocation.bucket);
            if (operation == IndexOperation.DELETE_SECONDARY)
            {
                if (actual != null && !actual.isEmpty())
                {
                    throw new IllegalStateException("sampled secondary key remains visible after delete");
                }
            }
            else
            {
                IndexProto.SecondaryIndexEntry entry = invocation.secondaryEntries.get(sample);
                IndexProto.RowLocation expected = rowLayout.locationForRowId(entry.getRowId());
                if (actual == null || !actual.contains(expected))
                {
                    throw new IllegalStateException("sampled secondary target state lacks expected location "
                            + expected + ": actual=" + actual);
                }
                if (targetIndex.unique && actual.size() != 1)
                {
                    throw new IllegalStateException("sampled unique secondary state has "
                            + actual.size() + " locations");
                }
            }
        }
        verifiedSamples.incrementAndGet();
    }

    private List<IndexProto.RowLocation> secondaryLocations(IndexProto.IndexKey key, int bucket)
            throws Exception
    {
        if (targetIndex.unique)
        {
            IndexProto.RowLocation location = indexService.lookupUniqueIndex(key, indexOptions[bucket]);
            return location == null ? null : Collections.singletonList(location);
        }
        return indexService.lookupNonUniqueIndex(key, indexOptions[bucket]);
    }

    private void ensureReady()
    {
        if (config == null || indexService == null)
        {
            throw new IllegalStateException("physical index scenario is not set up");
        }
        if (closed.get())
        {
            throw new IllegalStateException("physical index scenario is closed");
        }
    }

    private String localCallName()
    {
        switch (operation)
        {
            case PUT_PRIMARY:
                return "LocalIndexService.putPrimaryIndexEntries";
            case UPDATE_PRIMARY:
                return "LocalIndexService.updatePrimaryIndexEntries";
            case DELETE_PRIMARY:
                return "LocalIndexService.deletePrimaryIndexEntries";
            case PUT_SECONDARY:
                return "LocalIndexService.putSecondaryIndexEntries";
            case UPDATE_SECONDARY:
                return "LocalIndexService.updateSecondaryIndexEntries";
            case DELETE_SECONDARY:
                return "LocalIndexService.deleteSecondaryIndexEntries";
            default:
                throw new AssertionError(operation);
        }
    }

    private String fixtureDescription()
    {
        if (operation.isPrimary())
        {
            return operation.requiresExistingKey()
                    ? "real putPrimaryIndexEntries prefill outside timing; MainIndex "
                    + mainIndexState.optionValue()
                    : "timed keys checked absent outside timing";
        }
        return operation.requiresExistingKey()
                ? "valid primary rows plus real secondary put prefill outside timing"
                : "valid primary rows outside timing; timed secondary keys checked absent";
    }

    private boolean usesTimedMainIndexFixture()
    {
        return operation.isPrimary() && operation.requiresExistingKey();
    }

    private String bucketList()
    {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < usableBuckets.length; i++)
        {
            if (i > 0)
            {
                builder.append(',');
            }
            builder.append(usableBuckets[i]);
        }
        return builder.toString();
    }

    private Exception closeIndex(SnapshotManifest.IndexState index, boolean primary,
                                 Exception first)
    {
        boolean[] buckets = openedBuckets.get(index.indexId);
        if (buckets == null)
        {
            return first;
        }
        int opened = -1;
        for (int bucket = 0; bucket < buckets.length; bucket++)
        {
            if (buckets[bucket])
            {
                opened = bucket;
                break;
            }
        }
        if (opened < 0)
        {
            return first;
        }
        try
        {
            indexService.closeIndex(table.tableId, index.indexId, primary, indexOptions[opened]);
        }
        catch (Exception e)
        {
            return append(first, e);
        }
        return first;
    }

    private static Exception append(Exception first, Exception next)
    {
        if (first == null)
        {
            return next;
        }
        first.addSuppressed(next);
        return first;
    }

    private final class PhysicalIndexWorker implements BenchmarkWorker
    {
        private final BenchmarkPhase phase;
        private final int workerId;
        private List<PreparedInvocation> invocations;
        private long preparedFirst;
        private long preparedCount;
        private int preparedBatchSize;
        private PreparedInvocation sampled;

        private PhysicalIndexWorker(BenchmarkPhase phase, int workerId)
        {
            this.phase = phase;
            this.workerId = workerId;
        }

        @Override
        public void prepare(long firstOperation, long operationCount, int batchSize) throws Exception
        {
            if (invocations != null)
            {
                throw new IllegalStateException("physical index worker was prepared twice");
            }
            long batchCount = (operationCount + batchSize - 1L) / batchSize;
            if (batchCount > Integer.MAX_VALUE)
            {
                throw new IllegalArgumentException("too many prepared Index batches for one worker");
            }
            this.preparedFirst = firstOperation;
            this.preparedCount = operationCount;
            this.preparedBatchSize = batchSize;
            this.invocations = new ArrayList<>((int) batchCount);

            long cursor = firstOperation;
            long end = Math.addExact(firstOperation, operationCount);
            long ordinal = 0L;
            while (cursor < end)
            {
                int count = (int) Math.min((long) batchSize, end - cursor);
                int bucketSlot = (int) Math.floorMod(
                        workerId + ordinal * (long) config.threads(), usableBuckets.length);
                int bucket = usableBuckets[bucketSlot];
                invocations.add(prepareInvocation(phase, cursor, count, bucket));
                cursor += count;
                ordinal++;
            }
        }

        @Override
        public OperationResult execute(long firstOperation, int logicalOperationCount)
        {
            PreparedInvocation invocation = invocation(firstOperation, logicalOperationCount);
            if (!invocation.executed.compareAndSet(false, true))
            {
                return OperationResult.failure(logicalOperationCount, 0L);
            }
            try
            {
                boolean success;
                List<IndexProto.RowLocation> returnedLocations = null;
                List<Long> returnedRowIds = null;
                switch (operation)
                {
                    case PUT_PRIMARY:
                        success = indexService.putPrimaryIndexEntries(table.tableId, targetIndex.indexId,
                                invocation.primaryEntries, indexOptions[invocation.bucket]);
                        break;
                    case UPDATE_PRIMARY:
                        returnedLocations = indexService.updatePrimaryIndexEntries(
                                table.tableId, targetIndex.indexId, invocation.primaryEntries,
                                indexOptions[invocation.bucket]);
                        success = returnedLocations != null
                                && returnedLocations.size() == logicalOperationCount;
                        break;
                    case DELETE_PRIMARY:
                        returnedLocations = indexService.deletePrimaryIndexEntries(
                                table.tableId, targetIndex.indexId, invocation.keys,
                                indexOptions[invocation.bucket]);
                        success = returnedLocations != null
                                && returnedLocations.size() == logicalOperationCount;
                        break;
                    case PUT_SECONDARY:
                        success = indexService.putSecondaryIndexEntries(table.tableId, targetIndex.indexId,
                                invocation.secondaryEntries, indexOptions[invocation.bucket]);
                        break;
                    case UPDATE_SECONDARY:
                        returnedRowIds = indexService.updateSecondaryIndexEntries(
                                table.tableId, targetIndex.indexId, invocation.secondaryEntries,
                                indexOptions[invocation.bucket]);
                        success = returnedRowIds != null
                                && returnedRowIds.size() == logicalOperationCount;
                        break;
                    case DELETE_SECONDARY:
                        returnedRowIds = indexService.deleteSecondaryIndexEntries(
                                table.tableId, targetIndex.indexId, invocation.keys,
                                indexOptions[invocation.bucket]);
                        success = returnedRowIds != null
                                && returnedRowIds.size() == logicalOperationCount;
                        break;
                    default:
                        throw new AssertionError(operation);
                }
                if (!success)
                {
                    firstError.compareAndSet(null, "LocalIndexService returned an invalid batch result");
                    return OperationResult.failure(logicalOperationCount, 1L);
                }
                if (sampled == null)
                {
                    invocation.returnedLocations = returnedLocations;
                    invocation.returnedRowIds = returnedRowIds;
                    sampled = invocation;
                }
                return new OperationResult(logicalOperationCount, logicalOperationCount, 0L, 1L);
            }
            catch (Exception e)
            {
                firstError.compareAndSet(null, e.getClass().getName() + ": "
                        + String.valueOf(e.getMessage()));
                return OperationResult.failure(logicalOperationCount, 1L);
            }
        }

        private PreparedInvocation invocation(long firstOperation, int logicalOperationCount)
        {
            if (invocations == null)
            {
                throw new IllegalStateException("physical index worker has not been prepared");
            }
            long relative = firstOperation - preparedFirst;
            if (relative < 0 || relative >= preparedCount || relative % preparedBatchSize != 0)
            {
                throw new IllegalArgumentException("unexpected Index operation offset " + firstOperation);
            }
            PreparedInvocation invocation = invocations.get((int) (relative / preparedBatchSize));
            if (invocation.firstOperation != firstOperation
                    || invocation.logicalCount != logicalOperationCount)
            {
                throw new IllegalArgumentException("execute range does not match prepared Index batch");
            }
            return invocation;
        }

        @Override
        public void close() throws Exception
        {
            if (sampled == null)
            {
                return;
            }
            try
            {
                verifySample(sampled);
            }
            catch (Exception e)
            {
                firstError.compareAndSet(null, e.getClass().getName() + ": "
                        + String.valueOf(e.getMessage()));
                throw e;
            }
        }
    }

    private static final class PreparedInvocation
    {
        private final long firstOperation;
        private final int logicalCount;
        private final int bucket;
        private final List<IndexProto.PrimaryIndexEntry> primaryEntries = new ArrayList<>();
        private final List<IndexProto.SecondaryIndexEntry> secondaryEntries = new ArrayList<>();
        private final List<IndexProto.IndexKey> keys = new ArrayList<>();
        private final List<IndexProto.RowLocation> expectedPreviousLocations = new ArrayList<>();
        private final List<Long> expectedPreviousRowIds = new ArrayList<>();
        private final AtomicBoolean executed = new AtomicBoolean(false);
        private List<IndexProto.RowLocation> returnedLocations;
        private List<Long> returnedRowIds;

        private PreparedInvocation(long firstOperation, int logicalCount, int bucket)
        {
            this.firstOperation = firstOperation;
            this.logicalCount = logicalCount;
            this.bucket = bucket;
        }
    }
}
