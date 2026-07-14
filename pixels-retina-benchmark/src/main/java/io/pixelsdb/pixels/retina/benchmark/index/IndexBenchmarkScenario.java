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
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkConfig;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkPhase;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkResult;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkScenario;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkWorker;
import io.pixelsdb.pixels.retina.benchmark.common.OperationRange;
import io.pixelsdb.pixels.retina.benchmark.common.OperationResult;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Throughput scenario for the primary-index part of a Retina update.
 *
 * <p>This deliberately calls the production local API used by Retina:</p>
 *
 * <pre>
 * RetinaServerImpl.updateRecord
 *   -&gt; IndexServiceProvider.getService(ServiceMode.local)
 *   -&gt; LocalIndexService.updatePrimaryIndexEntries(...)
 *   -&gt; SinglePointIndex.updatePrimaryEntries(...)
 *   -&gt; MainIndex.getLocations(previousRowIds)
 *   -&gt; MainIndex.putEntries(newEntries)
 * </pre>
 *
 * <p>The source anchors are {@code RetinaServerImpl}'s update branch and
 * {@code LocalIndexService#updatePrimaryIndexEntries}.  The timed method below
 * does not call transaction, visibility, or write-buffer code.  Like
 * {@code RetinaServerImpl#executeParallelByBucket}, every local API call gets
 * entries from exactly one SHA-256 index bucket and the corresponding
 * {@link IndexOption} vnode.  The server's outer striped lock is not part of
 * the IndexService module call: it protects the compound index-plus-visibility
 * update.  It is unnecessary here because every prepared primary key is
 * unique and Visibility is intentionally outside this benchmark.</p>
 *
 * <p>Preparation uses the real
 * {@link LocalIndexService#putPrimaryIndexEntries} path to install an older
 * version for every measured key.  Consequently an update must return one old
 * {@link IndexProto.RowLocation}; upsert-on-miss is not being measured.  The
 * public {@link SinglePointIndexFactory.TableIndex} overload registers the
 * benchmark's metadata-free table descriptor before the ID-based production
 * call is made.  This avoids inventing an index implementation while also
 * avoiding a Metadata Service dependency that the isolated local benchmark
 * does not otherwise need.</p>
 */
public final class IndexBenchmarkScenario implements BenchmarkScenario
{
    private static final int DEFAULT_BUCKET_COUNT = 128;
    private static final int DEFAULT_ROWS_PER_ROW_GROUP = 100_000;
    private static final long MAX_SAFE_OPERATION_ID = (Long.MAX_VALUE - 4L) / 2L;

    private final Map<BenchmarkPhase, PhaseState> states = new ConcurrentHashMap<>();
    private final Map<BenchmarkPhase, Long> phaseOperationCounts = new ConcurrentHashMap<>();
    private final Set<IndexWorker> workers = Collections.newSetFromMap(
            new ConcurrentHashMap<IndexWorker, Boolean>());

    private BenchmarkConfig benchmarkConfig;
    private IndexService indexService;
    private SinglePointIndexFactory singlePointIndexFactory;
    private IndexOption[] indexOptions;
    private int bucketCount;
    private int rowsPerRowGroup;
    private long idBase;
    private Path sqliteDirectory;
    private boolean ownsSqliteDirectory;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicReference<String> firstUpdateError = new AtomicReference<>();

    @Override
    public String name()
    {
        return "index";
    }

    @Override
    public void setup(BenchmarkConfig config) throws Exception
    {
        if (this.benchmarkConfig != null)
        {
            throw new IllegalStateException("index scenario is already set up");
        }
        this.benchmarkConfig = config;
        this.bucketCount = positiveIntOption(config, "index-buckets", DEFAULT_BUCKET_COUNT);
        this.rowsPerRowGroup = positiveIntOption(config, "index-rows-per-rg", DEFAULT_ROWS_PER_ROW_GROUP);

        String configuredPath = option(config, "index-sqlite-path", null);
        if (configuredPath == null || configuredPath.trim().isEmpty())
        {
            this.sqliteDirectory = Paths.get(System.getProperty("java.io.tmpdir"),
                    "pixels-retina-index-benchmark-" + Long.toUnsignedString(System.nanoTime()));
            this.ownsSqliteDirectory = true;
        }
        else
        {
            this.sqliteDirectory = Paths.get(configuredPath).toAbsolutePath();
            this.ownsSqliteDirectory = false;
        }
        Files.createDirectories(this.sqliteDirectory);

        /*
         * These must be installed before any of the singleton factories are
         * initialized.  The fat JAR contains the production memory SPI and
         * SQLite MainIndex providers; ServiceLoader still creates them.
         */
        ConfigFactory pixelsConfig = ConfigFactory.Instance();
        pixelsConfig.addProperty("enabled.single.point.index.schemes", "memory");
        pixelsConfig.addProperty("enabled.main.index.scheme", "sqlite");
        pixelsConfig.addProperty("index.sqlite.path", this.sqliteDirectory.toString());
        pixelsConfig.addProperty("index.bucket.num", Integer.toString(this.bucketCount));
        pixelsConfig.addProperty("retina.upsert-mode.enabled", "false");
        if (pixelsConfig.getProperty("index.main.cache.bucket.num") == null)
        {
            pixelsConfig.addProperty("index.main.cache.bucket.num", "7");
        }

        this.singlePointIndexFactory = SinglePointIndexFactory.Instance();
        if (!this.singlePointIndexFactory.isSchemeEnabled(SinglePointIndex.Scheme.memory))
        {
            throw new IllegalStateException("SinglePointIndexFactory was initialized before the benchmark "
                    + "could select the memory provider; run each benchmark in a fresh JVM");
        }
        MainIndexFactory mainFactory = MainIndexFactory.Instance();
        if (!mainFactory.isSchemeEnabled(MainIndex.Scheme.sqlite))
        {
            throw new IllegalStateException("index benchmark requires the production SQLite MainIndex provider");
        }

        this.indexService = IndexServiceProvider.getService(IndexServiceProvider.ServiceMode.local);
        this.indexOptions = new IndexOption[this.bucketCount];
        for (int bucket = 0; bucket < this.bucketCount; bucket++)
        {
            this.indexOptions[bucket] = IndexOption.builder().vNodeId(bucket).build();
        }

        /* Keep generated uint64 IDs positive and comfortably below Long.MAX_VALUE. */
        long runBits = (System.currentTimeMillis() & 0x0fffffffL) * 100_000L;
        this.idBase = 10_000_000L + runBits + (System.nanoTime() & 0xffffL);
    }

    @Override
    public void preparePhase(BenchmarkPhase phase, long totalOperations)
    {
        ensureSetUp();
        if (totalOperations < 0 || totalOperations > MAX_SAFE_OPERATION_ID)
        {
            throw new IllegalArgumentException("unsupported index operation count: " + totalOperations);
        }
        phaseOperationCounts.put(phase, totalOperations);
        state(phase);
    }

    @Override
    public BenchmarkWorker createWorker(BenchmarkPhase phase, int workerId, int clientId,
                                        OperationRange operationRange)
    {
        ensureSetUp();
        if (clientId < 0 || clientId >= benchmarkConfig.clients())
        {
            throw new IllegalArgumentException("invalid local index client shard: " + clientId);
        }
        PhaseState state = state(phase);
        IndexWorker worker = new IndexWorker(phase, workerId, clientId, state);
        workers.add(worker);
        return worker;
    }

    @Override
    public void completePhase(BenchmarkPhase phase, BenchmarkResult result) throws Exception
    {
        /*
         * Warmup has independent table/index IDs.  Close it before measurement
         * so warmup versions cannot inflate or warm the measured index state.
         */
        if (phase == BenchmarkPhase.WARMUP)
        {
            closePhase(phase);
        }
    }

    @Override
    public Map<String, String> details()
    {
        Map<String, String> details = new LinkedHashMap<>();
        details.put("call", "LocalIndexService.updatePrimaryIndexEntries (local)");
        details.put("singlePointIndex", "memory");
        details.put("mainIndex", "sqlite");
        details.put("bucketCount", Integer.toString(bucketCount));
        PhaseState measurement = states.get(BenchmarkPhase.MEASUREMENT);
        details.put("activeMeasurementBuckets", Integer.toString(
                measurement == null ? 0 : initializedBucketCount(measurement)));
        details.put("bucketDistribution",
                "same-bucket per API call; successive worker batches rotate across vnodes");
        details.put("bucketFunction", "IndexUtils.getBucketIdFromByteBuffer (SHA-256)");
        details.put("sqlitePath", sqliteDirectory == null ? "not-initialized" : sqliteDirectory.toString());
        details.put("logicalOperation", "one PrimaryIndexEntry; apiCalls count same-bucket local batches");
        details.put("clients", "key-namespace generator groups only; local IndexService has no client connection");
        details.put("metadata", "no Metadata Service RPC; TableIndex descriptor is registered during prepare");
        details.put("outerUpdateLock", "excluded: it coordinates the compound index+visibility update; keys are unique");
        details.put("state", "one prefilled version plus one new version per executed key");
        String error = firstUpdateError.get();
        if (error != null)
        {
            details.put("firstUpdateError", error);
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
        for (BenchmarkPhase phase : BenchmarkPhase.values())
        {
            try
            {
                closePhase(phase);
            }
            catch (Exception e)
            {
                if (first == null)
                {
                    first = e;
                }
            }
        }
        workers.clear();

        if (ownsSqliteDirectory && sqliteDirectory != null)
        {
            try
            {
                deleteRecursively(sqliteDirectory.toFile());
            }
            catch (IOException e)
            {
                if (first == null)
                {
                    first = e;
                }
            }
        }
        if (first != null)
        {
            throw first;
        }
    }

    private PhaseState state(BenchmarkPhase phase)
    {
        PhaseState existing = states.get(phase);
        if (existing != null)
        {
            return existing;
        }

        int ordinal = phase.ordinal();
        Long totalOperations = phaseOperationCounts.get(phase);
        if (totalOperations == null)
        {
            throw new IllegalStateException("preparePhase must run before index workers are created");
        }
        long tableId = idBase + ordinal * 2L + 1L;
        long indexId = idBase + 1_000_000L + ordinal * 2L + 1L;
        long fileBase = idBase + 2_000_000L + (long) ordinal * (bucketCount * 2L + 2L);
        PhaseState created = new PhaseState(phase, tableId, indexId, fileBase, totalOperations);
        PhaseState raced = states.putIfAbsent(phase, created);
        return raced == null ? created : raced;
    }

    private void closePhase(BenchmarkPhase phase) throws Exception
    {
        Exception first = null;
        PhaseState state = states.get(phase);
        if (state != null && state.closed.compareAndSet(false, true))
        {
            try
            {
                /* One close closes every vnode registered for this index. */
                indexService.closeIndex(state.tableId, state.indexId, true, indexOptions[0]);
            }
            catch (Exception e)
            {
                first = e;
            }
        }
        states.remove(phase);
        phaseOperationCounts.remove(phase);
        if (first != null)
        {
            throw first;
        }
    }

    private void ensureBucketInitialized(PhaseState state, int bucket) throws Exception
    {
        if (state.initializedBuckets[bucket])
        {
            return;
        }
        synchronized (state.initializedBuckets)
        {
            if (!state.initializedBuckets[bucket])
            {
                SinglePointIndexFactory.TableIndex descriptor =
                        new SinglePointIndexFactory.TableIndex(state.tableId, state.indexId,
                                SinglePointIndex.Scheme.memory, true);
                singlePointIndexFactory.getSinglePointIndex(descriptor, indexOptions[bucket]);
                state.initializedBuckets[bucket] = true;
            }
        }
    }

    private static int initializedBucketCount(PhaseState state)
    {
        int count = 0;
        synchronized (state.initializedBuckets)
        {
            for (boolean initialized : state.initializedBuckets)
            {
                if (initialized)
                {
                    count++;
                }
            }
        }
        return count;
    }

    private IndexProto.PrimaryIndexEntry entry(PhaseState state, long operationId,
                                                ByteString key, int bucket, boolean updated,
                                                long transactionTimestamp)
    {
        /* Every new version uses an ID above the complete prefill ID domain. */
        long rowId = updated ? state.totalOperations + operationId : operationId;
        long fileId = state.fileBase + bucket * 2L + (updated ? 1L : 0L);
        long rgLong = operationId / rowsPerRowGroup;
        if (rgLong > Integer.MAX_VALUE)
        {
            throw new IllegalArgumentException("row-group id exceeds protobuf uint32 Java representation for "
                    + "operation " + operationId + "; increase --index-rows-per-rg");
        }
        int rowGroup = (int) rgLong;
        int rowOffset = (int) (operationId % rowsPerRowGroup);

        IndexProto.IndexKey indexKey = IndexProto.IndexKey.newBuilder()
                .setTableId(state.tableId)
                .setIndexId(state.indexId)
                .setKey(key)
                .setTimestamp(transactionTimestamp)
                .build();
        IndexProto.RowLocation location = IndexProto.RowLocation.newBuilder()
                .setFileId(fileId)
                .setRgId(rowGroup)
                .setRgRowOffset(rowOffset)
                .build();
        return IndexProto.PrimaryIndexEntry.newBuilder()
                .setIndexKey(indexKey)
                .setRowId(rowId)
                .setRowLocation(location)
                .build();
    }

    private ByteString keyForBucket(long operationId, int clientId, int targetBucket)
    {
        int salt = 0;
        while (true)
        {
            ByteBuffer buffer = ByteBuffer.allocate(16);
            buffer.putInt(clientId);
            buffer.putInt(salt);
            buffer.putLong(operationId);
            ByteString key = ByteString.copyFrom(buffer.array());
            int bucket = IndexUtils.getBucketIdFromByteBuffer(key);
            if (bucket == targetBucket)
            {
                return key;
            }
            /* A negative bucket is IndexUtils' Math.abs(Integer.MIN_VALUE) edge. */
            salt++;
        }
    }

    private void ensureSetUp()
    {
        if (benchmarkConfig == null)
        {
            throw new IllegalStateException("index scenario has not been set up");
        }
        if (closed.get())
        {
            throw new IllegalStateException("index scenario is closed");
        }
    }

    private static String option(BenchmarkConfig config, String key, String defaultValue)
    {
        String value = config.options().get(key);
        return value == null ? defaultValue : value;
    }

    private static int positiveIntOption(BenchmarkConfig config, String key, int defaultValue)
    {
        String value = option(config, key, Integer.toString(defaultValue));
        int parsed = Integer.parseInt(value);
        if (parsed <= 0)
        {
            throw new IllegalArgumentException("--" + key + " must be positive");
        }
        return parsed;
    }

    private static void deleteRecursively(File file) throws IOException
    {
        if (!file.exists())
        {
            return;
        }
        File[] children = file.listFiles();
        if (children != null)
        {
            for (File child : children)
            {
                deleteRecursively(child);
            }
        }
        Files.deleteIfExists(file.toPath());
    }

    private final class IndexWorker implements BenchmarkWorker
    {
        private final BenchmarkPhase phase;
        private final int workerId;
        private final int clientId;
        private final PhaseState state;
        private final AtomicBoolean workerClosed = new AtomicBoolean(false);
        private List<PreparedInvocation> invocations;
        private long preparedFirst;
        private long preparedCount;
        private int preparedBatchSize;
        private PreparedInvocation sampledInvocation;
        private List<IndexProto.RowLocation> sampledPrevious;

        private IndexWorker(BenchmarkPhase phase, int workerId, int clientId, PhaseState state)
        {
            this.phase = phase;
            this.workerId = workerId;
            this.clientId = clientId;
            this.state = state;
        }

        @Override
        public void prepare(long firstOperation, long operationCount, int batchSize) throws Exception
        {
            if (invocations != null)
            {
                throw new IllegalStateException("index worker was prepared twice");
            }
            if (firstOperation < 0 || operationCount < 0
                    || firstOperation > MAX_SAFE_OPERATION_ID - operationCount)
            {
                throw new IllegalArgumentException("invalid index operation range");
            }
            if (batchSize <= 0)
            {
                throw new IllegalArgumentException("index batch size must be positive");
            }
            long batchCountLong = (operationCount + batchSize - 1L) / batchSize;
            if (batchCountLong > Integer.MAX_VALUE)
            {
                throw new IllegalArgumentException("too many prepared index batches for one worker: "
                        + batchCountLong);
            }

            this.preparedFirst = firstOperation;
            this.preparedCount = operationCount;
            this.preparedBatchSize = batchSize;
            this.invocations = new ArrayList<>((int) batchCountLong);

            long cursor = firstOperation;
            long end = firstOperation + operationCount;
            long batchOrdinal = 0L;
            while (cursor < end)
            {
                int targetBucket = (int) Math.floorMod(
                        (long) workerId + (batchOrdinal % bucketCount)
                                * ((long) benchmarkConfig.threads() % bucketCount),
                        (long) bucketCount);
                ensureBucketInitialized(state, targetBucket);
                int count = (int) Math.min((long) batchSize, end - cursor);
                long updateTimestamp = cursor + 2L;
                List<IndexProto.PrimaryIndexEntry> oldEntries = new ArrayList<>(count);
                List<IndexProto.PrimaryIndexEntry> newEntries = new ArrayList<>(count);

                for (int i = 0; i < count; i++)
                {
                    long operationId = cursor + i;
                    ByteString key = keyForBucket(operationId, clientId, targetBucket);
                    oldEntries.add(entry(state, operationId, key, targetBucket, false, 1L));
                    newEntries.add(entry(state, operationId, key, targetBucket, true,
                            updateTimestamp));
                }

                /* Real older versions are installed outside the timed interval. */
                boolean put = indexService.putPrimaryIndexEntries(
                        state.tableId, state.indexId, oldEntries, indexOptions[targetBucket]);
                if (!put)
                {
                    throw new IllegalStateException("failed to prefill primary index bucket " + targetBucket);
                }
                invocations.add(new PreparedInvocation(cursor, count,
                        new PreparedBucketCall(targetBucket,
                                Collections.unmodifiableList(newEntries))));
                cursor += count;
                batchOrdinal++;
            }
        }

        @Override
        public OperationResult execute(long firstOperation, int logicalOperationCount)
        {
            if (workerClosed.get())
            {
                return new OperationResult(logicalOperationCount, 0, logicalOperationCount, 0);
            }
            PreparedInvocation invocation = invocation(firstOperation, logicalOperationCount);
            if (!invocation.executed.compareAndSet(false, true))
            {
                /* Reusing the same rowId/timestamp would not be a valid Retina update. */
                return new OperationResult(logicalOperationCount, 0, logicalOperationCount, 0);
            }

            PreparedBucketCall call = invocation.call;
            try
            {
                List<IndexProto.RowLocation> previous = indexService.updatePrimaryIndexEntries(
                        state.tableId, state.indexId, call.entries, indexOptions[call.bucket]);
                if (previous == null || previous.size() != call.entries.size())
                {
                    firstUpdateError.compareAndSet(null, "previous-location count mismatch: expected "
                            + call.entries.size() + ", got "
                            + (previous == null ? "null" : previous.size()));
                    return new OperationResult(logicalOperationCount, 0, logicalOperationCount, 1);
                }
                if (sampledPrevious == null)
                {
                    /* Keep one correctness sample; exact value comparison runs
                       in close(), after the measured finish timestamp. */
                    sampledInvocation = invocation;
                    sampledPrevious = previous;
                }
                return new OperationResult(logicalOperationCount, logicalOperationCount, 0, 1);
            }
            catch (Exception e)
            {
                firstUpdateError.compareAndSet(null, e.getClass().getName() + ": "
                        + String.valueOf(e.getMessage()));
                return new OperationResult(logicalOperationCount, 0, logicalOperationCount, 1);
            }
        }

        private PreparedInvocation invocation(long firstOperation, int logicalOperationCount)
        {
            if (invocations == null)
            {
                throw new IllegalStateException("index worker has not been prepared");
            }
            long relative = firstOperation - preparedFirst;
            if (relative < 0 || relative >= preparedCount || relative % preparedBatchSize != 0)
            {
                throw new IllegalArgumentException("unexpected index operation offset " + firstOperation);
            }
            int index = (int) (relative / preparedBatchSize);
            PreparedInvocation invocation = invocations.get(index);
            if (invocation.firstOperation != firstOperation || invocation.logicalCount != logicalOperationCount)
            {
                throw new IllegalArgumentException("execute range does not match prepared index batch");
            }
            return invocation;
        }

        @Override
        public void close()
        {
            workerClosed.set(true);
            workers.remove(this);
            if (sampledPrevious != null)
            {
                for (int i = 0; i < sampledPrevious.size(); i++)
                {
                    IndexProto.PrimaryIndexEntry updated = sampledInvocation.call.entries.get(i);
                    IndexProto.RowLocation expected = entry(state,
                            sampledInvocation.firstOperation + i,
                            updated.getIndexKey().getKey(), sampledInvocation.call.bucket,
                            false, 1L).getRowLocation();
                    if (!expected.equals(sampledPrevious.get(i)))
                    {
                        String message = "previous-location value mismatch in post-timing sample: expected "
                                + expected + ", got " + sampledPrevious.get(i);
                        firstUpdateError.compareAndSet(null, message);
                        throw new IllegalStateException(message);
                    }
                }
            }
        }
    }

    private final class PhaseState
    {
        private final BenchmarkPhase phase;
        private final long tableId;
        private final long indexId;
        private final long fileBase;
        private final long totalOperations;
        private final boolean[] initializedBuckets = new boolean[bucketCount];
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private PhaseState(BenchmarkPhase phase, long tableId, long indexId, long fileBase,
                           long totalOperations)
        {
            this.phase = phase;
            this.tableId = tableId;
            this.indexId = indexId;
            this.fileBase = fileBase;
            this.totalOperations = totalOperations;
        }
    }

    private static final class PreparedBucketCall
    {
        private final int bucket;
        private final List<IndexProto.PrimaryIndexEntry> entries;

        private PreparedBucketCall(int bucket, List<IndexProto.PrimaryIndexEntry> entries)
        {
            this.bucket = bucket;
            this.entries = entries;
        }
    }

    private static final class PreparedInvocation
    {
        private final long firstOperation;
        private final int logicalCount;
        private final PreparedBucketCall call;
        private final AtomicBoolean executed = new AtomicBoolean(false);

        private PreparedInvocation(long firstOperation, int logicalCount,
                                   PreparedBucketCall call)
        {
            this.firstOperation = firstOperation;
            this.logicalCount = logicalCount;
            this.call = call;
        }
    }
}
