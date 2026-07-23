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
    private int configuredBucketCount;
    private int bucketCount;
    private int rowsPerRowGroup;
    private long snapshotTimestamp;
    private SyntheticRowLayout rowLayout;
    private MainIndexState mainIndexState;
    private int syntheticKeyBytes;
    private ExistingPrimaryKeyPool existingKeyPool;
    private boolean useExistingSnapshotKeys;
    private long effectiveWarmupOperations;
    private long effectiveMeasurementOperations;
    private long snapshotRowCount;

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
                benchmarkConfig.get("main-index-state", MainIndexState.NATURAL.optionValue()));
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

        this.configuredBucketCount = positiveConfig("index.bucket.num");
        /*
         * Keep IndexUtils hashing on semanticConfig index.bucket.num. HyBench packages
         * may copy a parent sourceIndexBucketIds list that is wider than the hash space
         * used when keys were written; rewriting bucket.num would make lookups miss.
         */
        this.bucketCount = configuredBucketCount;
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

        this.snapshotRowCount = snapshotOrderedRowCount(table);
        this.useExistingSnapshotKeys = mainIndexState == MainIndexState.NATURAL
                && operation.isPrimary()
                && operation.requiresExistingKey()
                && primaryIndex.canonicalKeyBytes == Integer.BYTES;
        this.effectiveWarmupOperations = benchmarkConfig.warmupOperations();
        this.effectiveMeasurementOperations = benchmarkConfig.dataSize();
        if (useExistingSnapshotKeys)
        {
            prepareExistingSnapshotKeyPool();
        }
    }

    @Override
    public long warmupOperations(BenchmarkConfig ignored)
    {
        return effectiveWarmupOperations;
    }

    @Override
    public long measurementOperations(BenchmarkConfig ignored)
    {
        return effectiveMeasurementOperations;
    }

    @Override
    public void preparePhase(BenchmarkPhase phase, long totalOperations)
    {
        ensureReady();
        long expected = phase == BenchmarkPhase.WARMUP
                ? effectiveWarmupOperations : effectiveMeasurementOperations;
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
        if (!usesTimedMainIndexFixture()
                || mainIndexState == MainIndexState.HOT_BUFFER
                || mainIndexState == MainIndexState.NATURAL)
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
        details.put("effectiveMainIndexState", operation.isPrimary() && operation.requiresExistingKey()
                ? (mainIndexState == null ? "not-initialized" : mainIndexState.optionValue())
                : "not-applicable");
        details.put("keySource", useExistingSnapshotKeys
                ? "snapshot-existing-hybench-int-keys"
                : "synthetic-absent-keys");
        details.put("snapshotRowCount", Long.toString(snapshotRowCount));
        if (existingKeyPool != null)
        {
            details.put("existingKeyRange",
                    "[" + existingKeyPool.firstKey() + "," + existingKeyPool.lastKey() + "]");
            details.put("existingKeyPoolSize", Long.toString(existingKeyPool.totalKeys()));
        }
        else
        {
            details.put("existingKeyRange", "n/a");
            details.put("existingKeyPoolSize", "0");
        }
        details.put("effectiveWarmupOperations", Long.toString(effectiveWarmupOperations));
        details.put("effectiveMeasurementOperations", Long.toString(effectiveMeasurementOperations));
        details.put("syntheticRowIdLayout", useExistingSnapshotKeys
                ? "new domains only for update targets; existing snapshot keys for lookups"
                : "contiguous old/new domains; disjoint warmup/measurement");
        details.put("syntheticRowLocationLayout", "distinct old/new and warmup/measurement file IDs");
        details.put("persistedFixtureRanges", Long.toString(persistedFixtureRanges.get()));
        details.put("warmedFixtureRanges", Long.toString(warmedFixtureRanges.get()));
        details.put("workingCopy", runtime == null ? "not-initialized"
                : runtime.workDirectory().toString());
        details.put("configuredBucketCount", Integer.toString(configuredBucketCount));
        details.put("bucketCount", Integer.toString(bucketCount));
        details.put("bucketCountAdjustedBySourceState",
                Boolean.toString(bucketCount != configuredBucketCount));
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
            return configuredBucketRange();
        }
        List<Integer> buckets = new ArrayList<>();
        for (int bucket : manifest.sourceIndexBucketIds)
        {
            if (bucket >= 0 && bucket < configuredBucketCount && !buckets.contains(bucket))
            {
                buckets.add(bucket);
            }
        }
        Collections.sort(buckets);
        if (buckets.isEmpty())
        {
            return configuredBucketRange();
        }
        int[] result = new int[buckets.size()];
        for (int i = 0; i < buckets.size(); i++)
        {
            result[i] = buckets.get(i);
        }
        return result;
    }

    private int[] configuredBucketRange()
    {
        int[] all = new int[configuredBucketCount];
        for (int i = 0; i < all.length; i++)
        {
            all[i] = i;
        }
        return all;
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
            ByteString candidate = length == Integer.BYTES
                    ? hybenchCompatibleAbsentIntKey(index.indexId, identity, salt)
                    : syntheticKey(length, index.indexId, identity, salt);
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

    private ByteString hybenchCompatibleAbsentIntKey(long indexId, long identity, int salt)
    {
        // Stay in the same 4-byte int encoding space as HyBench CSV primary keys.
        long[] range = hybenchPrimaryKeyRange();
        long base = range[1] + 1L;
        long mixed = mix64(identity ^ Long.rotateLeft(indexId, 17)
                ^ ((long) salt * 0x9e3779b97f4a7c15L));
        long value = base + Math.floorMod(mixed, Integer.MAX_VALUE - 1L);
        if (value > Integer.MAX_VALUE)
        {
            value = Integer.MAX_VALUE - Math.floorMod(mixed, 1_000_000L);
        }
        return ExistingPrimaryKeyPool.intKey((int) value);
    }

    /**
     * Closed HyBench primary-key interval for the selected table.
     * Most tables use {@code [1, rowCount]}; company continues after customer.
     */
    private long[] hybenchPrimaryKeyRange()
    {
        if (snapshotRowCount <= 0)
        {
            throw new IllegalArgumentException("snapshot table has no ordered rows: " + table.tableName);
        }
        if (!"company".equalsIgnoreCase(table.tableName))
        {
            return new long[] {1L, snapshotRowCount};
        }
        long customerRows = tableMetadataRowCount("customer");
        if (customerRows <= 0 && snapshotRowCount % 2000L == 0L)
        {
            // HyBench SF formula: company = 2000 * SF, customer = 300000 * SF.
            customerRows = 300000L * (snapshotRowCount / 2000L);
        }
        if (customerRows <= 0)
        {
            throw new IllegalStateException("cannot derive HyBench company key base; "
                    + "customer metadataRowCount is missing and company rowCount="
                    + snapshotRowCount + " is not a multiple of 2000");
        }
        long firstKey = customerRows + 1L;
        long lastKey = Math.addExact(customerRows, snapshotRowCount);
        return new long[] {firstKey, lastKey};
    }

    private long tableMetadataRowCount(String tableName)
    {
        if (manifest.tables == null)
        {
            return 0L;
        }
        for (SnapshotManifest.TableState candidate : manifest.tables)
        {
            if (candidate != null && tableName.equalsIgnoreCase(candidate.tableName))
            {
                long ordered = snapshotOrderedRowCount(candidate);
                return ordered > 0 ? ordered : Math.max(0L, candidate.metadataRowCount);
            }
        }
        return 0L;
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
                                                 int count, int bucket, int workerId,
                                                 boolean prepareFixtures,
                                                 boolean prepareTimedPayloads,
                                                 ByteString[] preparedKeys,
                                                 int keyOffset,
                                                 boolean recordPreparedKeys) throws Exception
    {
        ensureIndexOpen(primaryIndex, bucket);
        ensureIndexOpen(targetIndex, bucket);
        if (preparedKeys == null || keyOffset < 0 || keyOffset + count > preparedKeys.length)
        {
            throw new IllegalArgumentException("invalid prepared key window for invocation");
        }
        PreparedInvocation invocation = new PreparedInvocation(bucket);
        List<IndexProto.PrimaryIndexEntry> primaryFixtures = prepareFixtures
                ? new ArrayList<>() : null;
        List<IndexProto.SecondaryIndexEntry> secondaryFixtures = prepareFixtures
                ? new ArrayList<>() : null;

        for (int i = 0; i < count; i++)
        {
            long operationId = firstOperation + i;
            long global = globalOperation(phase, operationId);
            long fixtureTs = fixtureTimestamp(global);
            long timedTs = timedTimestamp(global);
            ByteString targetKey;
            if (recordPreparedKeys)
            {
                if (useExistingSnapshotKeys)
                {
                    // Delete claims unique keys. Update samples a worker-exclusive stripe so
                    // concurrent threads cannot race on the same primary key / MainIndex row id.
                    targetKey = operation == IndexOperation.DELETE_PRIMARY
                            ? existingKeyPool.claim(bucket)
                            : existingKeyPool.sampleForWorker(bucket, workerId, config.threads());
                }
                else
                {
                    targetKey = newAbsentKey(targetIndex, bucket, keyIdentity(global, 1), timedTs);
                }
                preparedKeys[keyOffset + i] = targetKey;
            }
            else
            {
                targetKey = preparedKeys[keyOffset + i];
                if (targetKey == null)
                {
                    throw new IllegalStateException("prepared Index key is missing at offset "
                            + (keyOffset + i));
                }
            }

            if (operation.isPrimary())
            {
                long oldRowId = rowLayout.rowId(
                        phase, operationId, SyntheticRowLayout.Role.OLD);
                IndexProto.RowLocation oldLocation = rowLayout.location(
                        phase, operationId, SyntheticRowLayout.Role.OLD);
                if (operation.requiresExistingKey())
                {
                    if (prepareFixtures)
                    {
                        primaryFixtures.add(primaryEntry(targetKey, fixtureTs, oldRowId, oldLocation));
                    }
                    if (!useExistingSnapshotKeys)
                    {
                        invocation.expectedPreviousLocations.add(oldLocation);
                    }
                }
                if (prepareTimedPayloads && operation == IndexOperation.PUT_PRIMARY)
                {
                    invocation.primaryEntries.add(primaryEntry(targetKey, timedTs,
                            rowLayout.rowId(phase, operationId, SyntheticRowLayout.Role.NEW),
                            rowLayout.location(phase, operationId, SyntheticRowLayout.Role.NEW)));
                }
                else if (prepareTimedPayloads && operation == IndexOperation.UPDATE_PRIMARY)
                {
                    invocation.primaryEntries.add(primaryEntry(targetKey, timedTs,
                            rowLayout.rowId(phase, operationId, SyntheticRowLayout.Role.NEW),
                            rowLayout.location(phase, operationId, SyntheticRowLayout.Role.NEW)));
                }
                else if (prepareTimedPayloads)
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
                if (prepareFixtures)
                {
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
                }
                if (operation.requiresExistingKey())
                {
                    if (prepareFixtures)
                    {
                        secondaryFixtures.add(secondaryEntry(targetKey, fixtureTs, oldRowId));
                    }
                    invocation.expectedPreviousRowIds.add(oldRowId);
                }
                if (prepareTimedPayloads && operation == IndexOperation.DELETE_SECONDARY)
                {
                    invocation.keys.add(indexKey(targetIndex, targetKey, timedTs));
                }
                else if (prepareTimedPayloads)
                {
                    invocation.secondaryEntries.add(secondaryEntry(targetKey, timedTs, newRowId));
                }
            }
        }

        if (prepareFixtures && !primaryFixtures.isEmpty())
        {
            boolean put = indexService.putPrimaryIndexEntries(table.tableId, primaryIndex.indexId,
                    primaryFixtures, indexOptions[bucket]);
            if (!put)
            {
                throw new IllegalStateException("failed to prepare primary fixture in bucket " + bucket);
            }
        }
        if (prepareFixtures && !secondaryFixtures.isEmpty())
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
        if (!useExistingSnapshotKeys
                && invocation.returnedLocations != null
                && !invocation.expectedPreviousLocations.equals(invocation.returnedLocations))
        {
            throw new IllegalStateException("sampled previous RowLocation mismatch: expected="
                    + invocation.expectedPreviousLocations + ", actual=" + invocation.returnedLocations);
        }
        if (!useExistingSnapshotKeys
                && invocation.returnedRowIds != null
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
            if (useExistingSnapshotKeys)
            {
                return "existing HyBench snapshot primary keys; MainIndex "
                        + mainIndexState.optionValue() + " (no fixture prefill/warm)";
            }
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
        return operation.isPrimary()
                && operation.requiresExistingKey()
                && mainIndexState != MainIndexState.NATURAL;
    }

    private void prepareExistingSnapshotKeyPool() throws Exception
    {
        if (snapshotRowCount <= 0)
        {
            throw new IllegalArgumentException("snapshot table has no ordered rows for existing-key mode: "
                    + table.tableName);
        }
        boolean uniqueDelete = operation == IndexOperation.DELETE_PRIMARY;
        long requested = Math.addExact(config.warmupOperations(), config.dataSize());
        long[] keyRange = hybenchPrimaryKeyRange();
        long keySpaceSize = keyRange[1] - keyRange[0] + 1L;
        long keysNeeded = uniqueDelete
                ? requested
                : Math.min(requested, Math.max(100_000L, Math.min(keySpaceSize, 1_000_000L)));
        this.existingKeyPool = ExistingPrimaryKeyPool.build(
                keyRange[0], keyRange[1], usableBuckets, bucketCount, keysNeeded);

        if (operation == IndexOperation.UPDATE_PRIMARY)
        {
            validateNaturalUpdateKeyStripes(keyRange);
        }

        if (uniqueDelete)
        {
            long available = maxNaturalDeleteBudget();
            if (available <= 0)
            {
                throw new IllegalStateException("existing key pool cannot schedule any natural delete; table="
                        + table.tableName + " totalKeys=" + existingKeyPool.totalKeys()
                        + " threads=" + config.threads() + " batchSize=" + config.batchSize());
            }
            long warmup = Math.min(config.warmupOperations(), available);
            long remaining = available - warmup;
            if (remaining <= 0)
            {
                warmup = 0L;
                remaining = available;
            }
            this.effectiveWarmupOperations = warmup;
            this.effectiveMeasurementOperations = Math.min(config.dataSize(), remaining);
            if (effectiveMeasurementOperations <= 0)
            {
                throw new IllegalStateException("not enough existing keys for delete measurement; available="
                        + available + " totalKeys=" + existingKeyPool.totalKeys());
            }
            if (!naturalDeleteDemandFits(effectiveWarmupOperations, effectiveMeasurementOperations))
            {
                throw new IllegalStateException("natural delete schedule exceeds per-bucket key pools; warmup="
                        + effectiveWarmupOperations + " measurement=" + effectiveMeasurementOperations);
            }
        }

        // Spot-check that materialized keys are actually present in the restored index.
        int probes = Math.min(32, usableBuckets.length * 2);
        for (int i = 0; i < probes; i++)
        {
            int bucket = usableBuckets[i % usableBuckets.length];
            ByteString key = existingKeyPool.sample(bucket);
            ensureIndexOpen(primaryIndex, bucket);
            IndexProto.RowLocation location = indexService.lookupUniqueIndex(
                    indexKey(primaryIndex, key, snapshotTimestamp + 1L), indexOptions[bucket]);
            if (location == null)
            {
                throw new IllegalStateException("existing-key probe missed for table="
                        + table.tableName + " bucket=" + bucket
                        + "; HyBench key space [" + keyRange[0] + "," + keyRange[1]
                        + "] may not match this package");
            }
        }
    }

    /**
     * Worker-striped update sampling needs every worker that can be scheduled onto a
     * bucket to own at least one pool index in that bucket ({@code index % threads == workerId}).
     */
    private void validateNaturalUpdateKeyStripes(long[] keyRange)
    {
        int threads = config.threads();
        for (int bucket : usableBuckets)
        {
            int keys = existingKeyPool.availableInBucket(bucket);
            for (int workerId = 0; workerId < threads; workerId++)
            {
                if (!workerCanUseBucket(workerId, bucket))
                {
                    continue;
                }
                // For workerId < threads, stripe membership requires workerId < keys.
                if (workerId >= keys)
                {
                    throw new IllegalStateException("existing key pool too small for natural update: bucket="
                            + bucket + " keys=" + keys + " worker=" + workerId
                            + " threads=" + threads + " table=" + table.tableName
                            + " keyRange=[" + keyRange[0] + "," + keyRange[1] + "]");
                }
            }
        }
    }

    /**
     * Whether the round-robin bucket schedule can assign {@code workerId} to {@code bucket}
     * for some batch ordinal. When {@code threads % usableBuckets == 0}, workers are pinned.
     */
    private boolean workerCanUseBucket(int workerId, int bucket)
    {
        int bucketCount = usableBuckets.length;
        if (bucketCount <= 0)
        {
            return false;
        }
        int slot = -1;
        for (int i = 0; i < bucketCount; i++)
        {
            if (usableBuckets[i] == bucket)
            {
                slot = i;
                break;
            }
        }
        if (slot < 0)
        {
            return false;
        }
        if (config.threads() % bucketCount == 0)
        {
            return Math.floorMod(workerId, bucketCount) == slot;
        }
        // Otherwise ordinal * threads eventually hits every residue class mod bucketCount.
        return true;
    }

    /**
     * Largest natural-delete budget such that the warmup/measurement split fits the
     * exact per-bucket claim schedule used by {@link PhysicalIndexWorker#prepare}.
     */
    private long maxNaturalDeleteBudget()
    {
        long lo = 0L;
        long hi = existingKeyPool.totalKeys();
        while (lo < hi)
        {
            long mid = (lo + hi + 1L) >>> 1;
            long warmup = Math.min(config.warmupOperations(), mid);
            long remaining = mid - warmup;
            if (remaining <= 0L)
            {
                warmup = 0L;
                remaining = mid;
            }
            long measurement = Math.min(config.dataSize(), remaining);
            if (naturalDeleteDemandFits(warmup, measurement))
            {
                lo = mid;
            }
            else
            {
                hi = mid - 1L;
            }
        }
        return lo;
    }

    private boolean naturalDeleteDemandFits(long warmupOps, long measurementOps)
    {
        long[] demand = new long[usableBuckets.length];
        addNaturalDeleteDemand(demand, warmupOps);
        addNaturalDeleteDemand(demand, measurementOps);
        for (int slot = 0; slot < usableBuckets.length; slot++)
        {
            int available = existingKeyPool.availableInBucket(usableBuckets[slot]);
            if (demand[slot] > available)
            {
                return false;
            }
        }
        return true;
    }

    private void addNaturalDeleteDemand(long[] demand, long totalOps)
    {
        if (totalOps <= 0L)
        {
            return;
        }
        int threads = config.threads();
        int batchSize = config.batchSize();
        int bucketCount = usableBuckets.length;
        for (int workerId = 0; workerId < threads; workerId++)
        {
            long length = OperationRange.partition(totalOps, threads, workerId).size();
            long ordinal = 0L;
            long left = length;
            while (left > 0L)
            {
                int count = (int) Math.min((long) batchSize, left);
                int bucketSlot = (int) Math.floorMod(
                        workerId + ordinal * (long) threads, bucketCount);
                demand[bucketSlot] += count;
                left -= count;
                ordinal++;
            }
        }
    }

    private static long snapshotOrderedRowCount(SnapshotManifest.TableState tableState)
    {
        long rows = 0L;
        if (tableState.files != null)
        {
            for (SnapshotManifest.FileState file : tableState.files)
            {
                if (file.rowGroups != null && !file.rowGroups.isEmpty())
                {
                    for (SnapshotManifest.RowGroupState rowGroup : file.rowGroups)
                    {
                        rows = Math.addExact(rows, Math.max(0, rowGroup.recordNum));
                    }
                }
                else
                {
                    rows = Math.addExact(rows, Math.max(0L, file.footerRowCount));
                }
            }
        }
        if (rows <= 0 && tableState.metadataRowCount > 0)
        {
            rows = tableState.metadataRowCount;
        }
        return rows;
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
        private long preparedFirst;
        private long preparedCount;
        private int preparedBatchSize;
        private long nextOperation;
        private ByteString[] preparedKeys;
        private PreparedInvocation sampled;

        private PhysicalIndexWorker(BenchmarkPhase phase, int workerId)
        {
            this.phase = phase;
            this.workerId = workerId;
        }

        @Override
        public void prepare(long firstOperation, long operationCount, int batchSize) throws Exception
        {
            if (preparedBatchSize != 0)
            {
                throw new IllegalStateException("physical index worker was prepared twice");
            }
            this.preparedFirst = firstOperation;
            this.preparedCount = operationCount;
            this.preparedBatchSize = batchSize;
            this.nextOperation = firstOperation;
            if (operationCount > Integer.MAX_VALUE)
            {
                throw new IllegalArgumentException("too many Index operations to materialize keys");
            }
            this.preparedKeys = new ByteString[(int) operationCount];

            long cursor = firstOperation;
            long end = Math.addExact(firstOperation, operationCount);
            long ordinal = 0L;
            while (cursor < end)
            {
                int count = (int) Math.min((long) batchSize, end - cursor);
                int bucketSlot = (int) Math.floorMod(
                        workerId + ordinal * (long) config.threads(), usableBuckets.length);
                int bucket = usableBuckets[bucketSlot];
                int keyOffset = Math.toIntExact(cursor - preparedFirst);
                boolean prepareFixtures = operation.requiresExistingKey() && !useExistingSnapshotKeys;
                prepareInvocation(phase, cursor, count, bucket, workerId,
                        prepareFixtures, false, preparedKeys, keyOffset, true);
                cursor += count;
                ordinal++;
            }
        }

        @Override
        public OperationResult execute(long firstOperation, int logicalOperationCount)
        {
            if (firstOperation != nextOperation || logicalOperationCount <= 0
                    || firstOperation + logicalOperationCount > preparedFirst + preparedCount)
            {
                throw new IllegalArgumentException("unexpected Index operation range "
                        + firstOperation + "+" + logicalOperationCount
                        + ", expected next=" + nextOperation);
            }
            int bucket = invocationBucket(firstOperation, logicalOperationCount);
            PreparedInvocation invocation;
            nextOperation += logicalOperationCount;
            try
            {
                int keyOffset = Math.toIntExact(firstOperation - preparedFirst);
                invocation = prepareInvocation(phase, firstOperation, logicalOperationCount,
                        bucket, workerId, false, true, preparedKeys, keyOffset, false);
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

        private int invocationBucket(long firstOperation, int logicalOperationCount)
        {
            if (preparedBatchSize <= 0)
            {
                throw new IllegalStateException("physical index worker has not been prepared");
            }
            long relative = firstOperation - preparedFirst;
            if (relative < 0 || relative >= preparedCount || relative % preparedBatchSize != 0)
            {
                throw new IllegalArgumentException("unexpected Index operation offset " + firstOperation);
            }
            long ordinal = relative / preparedBatchSize;
            int expected = (int) Math.min((long) preparedBatchSize,
                    preparedFirst + preparedCount - firstOperation);
            if (logicalOperationCount != expected)
            {
                throw new IllegalArgumentException("execute range does not match prepared Index batch");
            }
            int bucketSlot = (int) Math.floorMod(
                    workerId + ordinal * (long) config.threads(), usableBuckets.length);
            return usableBuckets[bucketSlot];
        }

        @Override
        public void close() throws Exception
        {
            preparedKeys = null;
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
        private final int bucket;
        private final List<IndexProto.PrimaryIndexEntry> primaryEntries = new ArrayList<>();
        private final List<IndexProto.SecondaryIndexEntry> secondaryEntries = new ArrayList<>();
        private final List<IndexProto.IndexKey> keys = new ArrayList<>();
        private final List<IndexProto.RowLocation> expectedPreviousLocations = new ArrayList<>();
        private final List<Long> expectedPreviousRowIds = new ArrayList<>();
        private List<IndexProto.RowLocation> returnedLocations;
        private List<Long> returnedRowIds;

        private PreparedInvocation(int bucket)
        {
            this.bucket = bucket;
        }
    }
}
