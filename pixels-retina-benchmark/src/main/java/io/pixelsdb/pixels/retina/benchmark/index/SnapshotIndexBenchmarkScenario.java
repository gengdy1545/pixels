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
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkScenario;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkWorker;
import io.pixelsdb.pixels.retina.benchmark.common.OperationRange;
import io.pixelsdb.pixels.retina.benchmark.common.OperationResult;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotIO;
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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Primary-index update benchmark backed by an offline SF100 physical snapshot.
 *
 * <p>The immutable snapshot is first copied by {@link SnapshotRuntime} into a
 * disposable RocksDB + SQLite working directory.  This scenario keeps the
 * source table ID, primary-index ID, canonical key bytes and SHA-256 index
 * bucket from the manifest.  The timed call is the same local production call
 * used by Retina:</p>
 *
 * <pre>
 * LocalIndexService.updatePrimaryIndexEntries
 *   -&gt; RocksDBIndex.updatePrimaryEntries
 *   -&gt; SqliteMainIndex.getLocations/putEntries
 * </pre>
 *
 * <p>Warm-up and measurement consume disjoint primary keys.  Each local API
 * call contains entries from one real index bucket.  A harness execute group
 * may contain more than one API call only when its range crosses bucket
 * boundaries in the exported sample stream.  Transaction, Visibility and
 * WriteBuffer code are deliberately excluded.</p>
 */
public final class SnapshotIndexBenchmarkScenario implements BenchmarkScenario
{
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicReference<String> firstError = new AtomicReference<>();

    private BenchmarkConfig config;
    private SnapshotRuntime snapshotRuntime;
    private SnapshotManifest manifest;
    private SnapshotManifest.TableState table;
    private IndexService indexService;
    private SinglePointIndexFactory singlePointIndexFactory;
    private SinglePointIndexFactory.TableIndex tableIndex;
    private IndexOption[] indexOptions;
    private boolean[] openedBuckets;
    private List<SelectedSample> selectedSamples;
    private long warmupSampleCount;
    private long sourceTimestamp;
    private long nextRowId;
    private long newFileIdBase;
    private int targetFileCount;
    private int rowsPerRowGroup;
    private int bucketCount;
    private int activeBucketCount;
    private long duplicateSamples;
    private long inactiveSamples;
    private long relocatedSamples;
    private Set<Integer> sourceIndexBucketIds = Collections.emptySet();

    @Override
    public String name()
    {
        return "index";
    }

    @Override
    public void setup(BenchmarkConfig config) throws Exception
    {
        if (this.config != null)
        {
            throw new IllegalStateException("snapshot index scenario is already set up");
        }
        this.config = config;
        this.snapshotRuntime = SnapshotRuntime.open(config);
        this.manifest = snapshotRuntime.manifest();
        this.table = snapshotRuntime.requireTable(config.require("snapshot-table"));
        validateSnapshotDescription();

        /* Copy the immutable source before any RocksDB/SQLite singleton opens. */
        snapshotRuntime.prepareIndexState(table);

        /*
         * RocksDBFactory normally obtains fixed prefix lengths from Metadata
         * Service while opening every existing CF.  A standalone benchmark has
         * no Metadata Service, so register the exact fixed-prefix lengths that
         * the source RocksDBFactory derived via IndexUtils before opening the DB.
         */
        for (SnapshotManifest.TableState manifestTable : manifest.tables)
        {
            for (SnapshotManifest.IndexState index : manifestTable.indexes)
            {
                if ("rocksdb".equalsIgnoreCase(index.scheme)
                        && index.rocksDbPrefixKeyBytes > 0)
                {
                    RocksDBFactory.registerIndexKeyLength(index.indexId,
                            index.rocksDbPrefixKeyBytes);
                }
            }
        }

        this.bucketCount = positiveConfiguredInt("index.bucket.num");
        this.sourceIndexBucketIds = validateSourceIndexBucketIds();
        this.indexOptions = new IndexOption[bucketCount];
        this.openedBuckets = new boolean[bucketCount];
        for (int bucket = 0; bucket < bucketCount; bucket++)
        {
            indexOptions[bucket] = IndexOption.builder().vNodeId(bucket).build();
        }

        this.singlePointIndexFactory = SinglePointIndexFactory.Instance();
        if (!singlePointIndexFactory.isSchemeEnabled(SinglePointIndex.Scheme.rocksdb))
        {
            throw new IllegalStateException("snapshot index benchmark requires a fresh JVM with RocksDB enabled");
        }
        MainIndexFactory mainIndexFactory = MainIndexFactory.Instance();
        if (!mainIndexFactory.isSchemeEnabled(MainIndex.Scheme.sqlite))
        {
            throw new IllegalStateException("snapshot index benchmark requires SQLite MainIndex");
        }
        this.tableIndex = new SinglePointIndexFactory.TableIndex(
                table.tableId, table.primaryIndex.indexId, SinglePointIndex.Scheme.rocksdb, true);
        this.indexService = IndexServiceProvider.getService(IndexServiceProvider.ServiceMode.local);

        this.warmupSampleCount = config.warmupSeconds() > 0 && config.warmupOperations() > 0
                ? config.warmupOperations() : 0L;
        long required = Math.addExact(warmupSampleCount, config.dataSize());
        if (required > Integer.MAX_VALUE)
        {
            throw new IllegalArgumentException("snapshot sample-backed benchmark supports at most "
                    + Integer.MAX_VALUE + " warmup + measurement operations");
        }
        if (sourceTimestamp > Long.MAX_VALUE - required - 1L)
        {
            throw new IllegalArgumentException("snapshot timestamp leaves no room for benchmark versions");
        }

        Path samplePath = snapshotRuntime.resolveArtifact(table.indexSamplesFile);
        long validatedSamples = SnapshotIO.validateIndexSamples(samplePath);
        if (validatedSamples != table.indexSampleCount)
        {
            throw new IllegalArgumentException("index sample count does not match manifest: "
                    + validatedSamples + " != " + table.indexSampleCount);
        }
        this.selectedSamples = selectActiveSamples(samplePath, (int) required);

        Path sqlite = snapshotRuntime.workDirectory().resolve("index/sqlite")
                .resolve(table.tableId + ".main.index.db");
        this.nextRowId = readNextRowId(sqlite);
        if (nextRowId > Long.MAX_VALUE - required)
        {
            throw new IllegalArgumentException("snapshot MainIndex leaves no room for benchmark row IDs");
        }
        long maxFileId = maximumFileId(table);
        this.newFileIdBase = Math.addExact(maxFileId, 1L);
        this.targetFileCount = config.getInt("snapshot-index-target-files",
                Math.max(1, config.clients()));
        if (targetFileCount <= 0)
        {
            throw new IllegalArgumentException("--snapshot-index-target-files must be positive");
        }
        Math.addExact(newFileIdBase, targetFileCount - 1L);
        this.rowsPerRowGroup = config.getInt("snapshot-index-rows-per-rg",
                representativeRowsPerRowGroup(table));
        if (rowsPerRowGroup <= 0)
        {
            throw new IllegalArgumentException("--snapshot-index-rows-per-rg must be positive");
        }

        this.activeBucketCount = countOpenedBuckets();
    }

    @Override
    public void preparePhase(BenchmarkPhase phase, long totalOperations)
    {
        ensureSetUp();
        long expected = phase == BenchmarkPhase.WARMUP ? warmupSampleCount : config.dataSize();
        if (totalOperations != expected)
        {
            throw new IllegalArgumentException("unexpected " + phase + " operation count: "
                    + totalOperations + ", expected " + expected);
        }
    }

    @Override
    public BenchmarkWorker createWorker(BenchmarkPhase phase, int workerId, int clientId,
                                        OperationRange range)
    {
        ensureSetUp();
        return new SnapshotIndexWorker(phase, workerId, clientId);
    }

    @Override
    public Map<String, String> details()
    {
        Map<String, String> details = new LinkedHashMap<>();
        details.put("call", "LocalIndexService.updatePrimaryIndexEntries (local snapshot restore)");
        details.put("singlePointIndex", "rocksdb");
        details.put("mainIndex", "sqlite");
        details.put("snapshotSourceHost", manifest == null ? "not-initialized" : manifest.sourceHost);
        details.put("snapshotTable", table == null ? "not-initialized"
                : table.schemaName + "." + table.tableName);
        details.put("snapshotTableId", table == null ? "0" : Long.toString(table.tableId));
        details.put("snapshotIndexId", table == null || table.primaryIndex == null ? "0"
                : Long.toString(table.primaryIndex.indexId));
        details.put("snapshotTimestamp", Long.toString(sourceTimestamp));
        details.put("snapshotSamplesSelected", selectedSamples == null ? "0"
                : Integer.toString(selectedSamples.size()));
        details.put("snapshotDuplicateKeysSkipped", Long.toString(duplicateSamples));
        details.put("snapshotInactiveKeysSkipped", Long.toString(inactiveSamples));
        details.put("snapshotRelocatedKeys", Long.toString(relocatedSamples));
        details.put("snapshotLookupValidation", "outside timed phase; current RowLocation replaces file-sample location");
        details.put("activeIndexBuckets", Integer.toString(activeBucketCount));
        details.put("sourceIndexBucketFilter", sourceIndexBucketIds.isEmpty() ? "all/shared-CF"
                : sourceIndexBucketIds.toString());
        details.put("sourceRetinaVnodes", manifest == null || manifest.sourceVnodeIds == null
                ? "unknown" : manifest.sourceVnodeIds.toString());
        details.put("bucketCount", Integer.toString(bucketCount));
        details.put("targetUpdateFiles", Integer.toString(targetFileCount));
        details.put("targetRowsPerRG", Integer.toString(rowsPerRowGroup));
        details.put("warmupMeasurementKeys", "disjoint");
        details.put("snapshotWorkingCopy", snapshotRuntime == null ? "not-initialized"
                : snapshotRuntime.workDirectory().toString());
        details.put("state", "source snapshot is immutable; updates grow only the disposable working copy");
        details.put("batching", "every LocalIndexService API call contains one original SHA-256 bucket");
        String error = firstError.get();
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
        if (indexService != null && table != null && activeBucketCount > 0)
        {
            try
            {
                /* closeIndex flushes SQLite but never removes the shared root. */
                indexService.closeIndex(table.tableId, table.primaryIndex.indexId,
                        true, indexOptions[firstOpenedBucket()]);
            }
            catch (Exception e)
            {
                first = e;
            }
        }
        if (snapshotRuntime != null)
        {
            try
            {
                snapshotRuntime.close();
            }
            catch (Exception e)
            {
                if (first == null) first = e;
                else first.addSuppressed(e);
            }
        }
        if (first != null)
        {
            throw first;
        }
    }

    private void validateSnapshotDescription()
    {
        if (!manifest.sourceQuiesced)
        {
            throw new IllegalArgumentException("physical index restore requires sourceQuiesced=true in snapshot.json");
        }
        if (manifest.snapshotTimestamp <= 0)
        {
            throw new IllegalArgumentException("physical index restore requires a positive snapshotTimestamp");
        }
        this.sourceTimestamp = manifest.snapshotTimestamp;
        if (table.primaryIndex == null || !table.primaryIndex.primary || !table.primaryIndex.unique)
        {
            throw new IllegalArgumentException("snapshot table requires a unique primary index");
        }
        if (!"rocksdb".equalsIgnoreCase(table.primaryIndex.scheme))
        {
            throw new IllegalArgumentException("snapshot index benchmark supports RocksDB, found "
                    + table.primaryIndex.scheme);
        }
        if (table.primaryIndex.canonicalKeyBytes <= 0)
        {
            throw new IllegalArgumentException("snapshot primary index must have a fixed canonical key length");
        }
        if (table.indexSamplesFile == null || table.indexSampleCount <= 0)
        {
            throw new IllegalArgumentException("snapshot table has no index samples");
        }
        if (table.maxObservedCreateTimestamp > sourceTimestamp)
        {
            throw new IllegalArgumentException("snapshotTimestamp precedes sampled row timestamp "
                    + table.maxObservedCreateTimestamp);
        }
    }

    private List<SelectedSample> selectActiveSamples(Path samplePath, int required)
            throws Exception
    {
        List<SelectedSample> active = new ArrayList<>(required);
        Set<ByteString> keys = new LinkedHashSet<>();
        SnapshotIO.scanIndexSamples(samplePath, sample ->
        {
            if (sample.key == null || sample.key.length != table.primaryIndex.canonicalKeyBytes)
            {
                throw new IllegalArgumentException("snapshot sample key length does not match manifest: "
                        + (sample.key == null ? -1 : sample.key.length) + " != "
                        + table.primaryIndex.canonicalKeyBytes);
            }
            if (sample.createTimestamp < 0 || sample.createTimestamp > sourceTimestamp)
            {
                throw new IllegalArgumentException("snapshot sample timestamp is outside [0,T_snap]");
            }
            ByteString key = ByteString.copyFrom(sample.key);
            int bucket = IndexUtils.getBucketIdFromByteBuffer(key);
            if (bucket < 0 || bucket >= bucketCount || bucket != sample.bucketId)
            {
                throw new IllegalArgumentException("snapshot sample bucket mismatch for table "
                        + table.tableName + ": stored=" + sample.bucketId + ", computed=" + bucket);
            }
            if (sample.fileId <= 0 || sample.rgId < 0 || sample.rgRowOffset < 0)
            {
                throw new IllegalArgumentException("snapshot sample contains an invalid RowLocation");
            }
            /* A node snapshot may contain only a subset of Index bucket CFs. */
            if (!sourceIndexBucketIds.isEmpty() && !sourceIndexBucketIds.contains(bucket))
            {
                return true;
            }
            if (!keys.add(key))
            {
                duplicateSamples++;
                return true;
            }
            ensureBucketOpen(bucket);
            IndexProto.IndexKey lookupKey = IndexProto.IndexKey.newBuilder()
                    .setTableId(table.tableId).setIndexId(table.primaryIndex.indexId)
                    .setKey(key).setTimestamp(sourceTimestamp).build();
            IndexProto.RowLocation current = indexService.lookupUniqueIndex(
                    lookupKey, indexOptions[bucket]);
            if (current == null)
            {
                inactiveSamples++;
                return true;
            }
            IndexProto.RowLocation sampled = IndexProto.RowLocation.newBuilder()
                    .setFileId(sample.fileId).setRgId(sample.rgId)
                    .setRgRowOffset(sample.rgRowOffset).build();
            if (!current.equals(sampled))
            {
                relocatedSamples++;
            }
            active.add(new SelectedSample(key, bucket, current));
            return active.size() < required;
        });
        if (active.size() < required)
        {
            throw new IllegalArgumentException("snapshot contains only " + active.size()
                    + " active unique index keys for source Index buckets "
                    + (sourceIndexBucketIds.isEmpty() ? "all/shared-CF" : sourceIndexBucketIds)
                    + ", but warmup + measurement require " + required
                    + "; export more total --snapshot-index-samples to cover deleted/stale samples");
        }

        /* Keep bucket runs together so almost every harness group maps to one API call. */
        int warmupEnd = (int) warmupSampleCount;
        sortByBucket(active.subList(0, warmupEnd));
        sortByBucket(active.subList(warmupEnd, active.size()));
        return Collections.unmodifiableList(active);
    }

    private Set<Integer> validateSourceIndexBucketIds()
    {
        if (manifest.sourceIndexBucketIds == null || manifest.sourceIndexBucketIds.isEmpty())
        {
            return Collections.emptySet();
        }
        Set<Integer> validated = new LinkedHashSet<>();
        for (Integer bucket : manifest.sourceIndexBucketIds)
        {
            if (bucket == null || bucket < 0 || bucket >= bucketCount)
            {
                throw new IllegalArgumentException("snapshot sourceIndexBucketIds contains invalid bucket "
                        + bucket + " for index.bucket.num=" + bucketCount);
            }
            if (!validated.add(bucket))
            {
                throw new IllegalArgumentException("snapshot sourceIndexBucketIds contains duplicate bucket "
                        + bucket);
            }
        }
        return Collections.unmodifiableSet(validated);
    }

    private static void sortByBucket(List<SelectedSample> samples)
    {
        samples.sort(Comparator.comparingInt(sample -> sample.bucket));
    }

    private void ensureBucketOpen(int bucket) throws Exception
    {
        if (!openedBuckets[bucket])
        {
            synchronized (openedBuckets)
            {
                if (!openedBuckets[bucket])
                {
                    singlePointIndexFactory.getSinglePointIndex(tableIndex, indexOptions[bucket]);
                    openedBuckets[bucket] = true;
                }
            }
        }
    }

    private int countOpenedBuckets()
    {
        int count = 0;
        for (boolean opened : openedBuckets)
        {
            if (opened) count++;
        }
        return count;
    }

    private int firstOpenedBucket()
    {
        for (int bucket = 0; bucket < openedBuckets.length; bucket++)
        {
            if (openedBuckets[bucket]) return bucket;
        }
        throw new IllegalStateException("no snapshot index bucket was opened");
    }

    private int positiveConfiguredInt(String key)
    {
        String value = ConfigFactory.Instance().getProperty(key);
        if (value == null)
        {
            throw new IllegalArgumentException("snapshot effectiveConfig lacks " + key);
        }
        int parsed = Integer.parseInt(value);
        if (parsed <= 0)
        {
            throw new IllegalArgumentException(key + " must be positive");
        }
        return parsed;
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

    private static long maximumFileId(SnapshotManifest.TableState table)
    {
        long maximum = 0L;
        for (SnapshotManifest.FileState file : table.files)
        {
            maximum = Math.max(maximum, file.fileId);
        }
        return maximum;
    }

    private static int representativeRowsPerRowGroup(SnapshotManifest.TableState table)
    {
        List<Integer> counts = new ArrayList<>();
        for (SnapshotManifest.FileState file : table.files)
        {
            if (table.sampleLayout == null || table.sampleLayout.equalsIgnoreCase(file.layoutRole))
            {
                for (SnapshotManifest.RowGroupState rowGroup : file.rowGroups)
                {
                    if (rowGroup.recordNum > 0) counts.add(rowGroup.recordNum);
                }
            }
        }
        if (counts.isEmpty())
        {
            return 100_000;
        }
        Collections.sort(counts);
        return counts.get(counts.size() / 2);
    }

    private long phaseOffset(BenchmarkPhase phase)
    {
        return phase == BenchmarkPhase.WARMUP ? 0L : warmupSampleCount;
    }

    private SelectedSample selected(BenchmarkPhase phase, long phaseOperation)
    {
        long index = Math.addExact(phaseOffset(phase), phaseOperation);
        return selectedSamples.get(Math.toIntExact(index));
    }

    private IndexProto.PrimaryIndexEntry updatedEntry(BenchmarkPhase phase, long phaseOperation,
                                                       SelectedSample sample)
    {
        long global = Math.addExact(phaseOffset(phase), phaseOperation);
        long timestamp = Math.addExact(sourceTimestamp, global + 1L);
        long rowId = Math.addExact(nextRowId, global);
        int fileSlot = (int) (global % targetFileCount);
        long sequence = global / targetFileCount;
        long rg = sequence / rowsPerRowGroup;
        if (rg > Integer.MAX_VALUE)
        {
            throw new IllegalArgumentException("generated row group exceeds uint32 Java representation");
        }
        IndexProto.IndexKey key = IndexProto.IndexKey.newBuilder()
                .setTableId(table.tableId).setIndexId(table.primaryIndex.indexId)
                .setKey(sample.key).setTimestamp(timestamp).build();
        IndexProto.RowLocation location = IndexProto.RowLocation.newBuilder()
                .setFileId(newFileIdBase + fileSlot).setRgId((int) rg)
                .setRgRowOffset((int) (sequence % rowsPerRowGroup)).build();
        return IndexProto.PrimaryIndexEntry.newBuilder().setIndexKey(key)
                .setRowId(rowId).setRowLocation(location).build();
    }

    private void ensureSetUp()
    {
        if (config == null || indexService == null || selectedSamples == null)
        {
            throw new IllegalStateException("snapshot index scenario is not set up");
        }
        if (closed.get())
        {
            throw new IllegalStateException("snapshot index scenario is closed");
        }
    }

    private final class SnapshotIndexWorker implements BenchmarkWorker
    {
        private final BenchmarkPhase phase;
        private final int workerId;
        private final int clientId;
        private List<PreparedInvocation> invocations;
        private long preparedFirst;
        private long preparedCount;
        private int preparedBatchSize;

        private SnapshotIndexWorker(BenchmarkPhase phase, int workerId, int clientId)
        {
            this.phase = phase;
            this.workerId = workerId;
            this.clientId = clientId;
        }

        @Override
        public void prepare(long firstOperation, long operationCount, int batchSize)
        {
            if (invocations != null)
            {
                throw new IllegalStateException("snapshot index worker was prepared twice");
            }
            long batchCount = (operationCount + batchSize - 1L) / batchSize;
            if (batchCount > Integer.MAX_VALUE)
            {
                throw new IllegalArgumentException("too many snapshot index batches for one worker");
            }
            this.preparedFirst = firstOperation;
            this.preparedCount = operationCount;
            this.preparedBatchSize = batchSize;
            this.invocations = new ArrayList<>((int) batchCount);

            long cursor = firstOperation;
            long end = Math.addExact(firstOperation, operationCount);
            while (cursor < end)
            {
                int logicalCount = (int) Math.min((long) batchSize, end - cursor);
                Map<Integer, PreparedBucketCall> calls = new LinkedHashMap<>();
                for (int i = 0; i < logicalCount; i++)
                {
                    long operation = cursor + i;
                    SelectedSample sample = selected(phase, operation);
                    PreparedBucketCall call = calls.computeIfAbsent(sample.bucket,
                            bucket -> new PreparedBucketCall(bucket));
                    call.entries.add(updatedEntry(phase, operation, sample));
                    call.expectedPrevious.add(sample.oldLocation);
                }
                invocations.add(new PreparedInvocation(cursor, logicalCount,
                        new ArrayList<>(calls.values())));
                cursor += logicalCount;
            }
        }

        @Override
        public OperationResult execute(long firstOperation, int logicalOperationCount)
        {
            PreparedInvocation invocation = invocation(firstOperation, logicalOperationCount);
            if (!invocation.executed.compareAndSet(false, true))
            {
                firstError.compareAndSet(null, "snapshot invocation was executed twice");
                return OperationResult.failure(logicalOperationCount, 0L);
            }

            int successful = 0;
            long apiCalls = 0L;
            for (PreparedBucketCall call : invocation.calls)
            {
                apiCalls++;
                try
                {
                    List<IndexProto.RowLocation> previous = indexService.updatePrimaryIndexEntries(
                            table.tableId, table.primaryIndex.indexId,
                            call.entries, indexOptions[call.bucket]);
                    call.actualPrevious = previous;
                    if (previous == null || previous.size() != call.entries.size())
                    {
                        firstError.compareAndSet(null, "previous-location count mismatch in bucket "
                                + call.bucket + ": expected " + call.entries.size() + ", got "
                                + (previous == null ? "null" : previous.size()));
                        break;
                    }
                    successful += call.entries.size();
                }
                catch (Exception e)
                {
                    firstError.compareAndSet(null, e.getClass().getName() + ": "
                            + String.valueOf(e.getMessage()));
                    break;
                }
            }
            return new OperationResult(logicalOperationCount, successful,
                    logicalOperationCount - successful, apiCalls);
        }

        private PreparedInvocation invocation(long firstOperation, int logicalOperationCount)
        {
            if (invocations == null)
            {
                throw new IllegalStateException("snapshot index worker has not been prepared");
            }
            long relative = firstOperation - preparedFirst;
            if (relative < 0 || relative >= preparedCount || relative % preparedBatchSize != 0)
            {
                throw new IllegalArgumentException("unexpected snapshot index operation offset "
                        + firstOperation + " for worker " + workerId + "/client " + clientId);
            }
            PreparedInvocation invocation = invocations.get((int) (relative / preparedBatchSize));
            if (invocation.firstOperation != firstOperation
                    || invocation.logicalCount != logicalOperationCount)
            {
                throw new IllegalArgumentException("execute range does not match prepared snapshot batch");
            }
            return invocation;
        }

        @Override
        public void close()
        {
            if (invocations == null)
            {
                return;
            }
            for (PreparedInvocation invocation : invocations)
            {
                for (PreparedBucketCall call : invocation.calls)
                {
                    if (call.actualPrevious != null
                            && !call.expectedPrevious.equals(call.actualPrevious))
                    {
                        String message = "snapshot previous-location mismatch outside timed interval"
                                + " in bucket " + call.bucket + ": expected="
                                + call.expectedPrevious + ", actual=" + call.actualPrevious;
                        firstError.compareAndSet(null, message);
                        throw new IllegalStateException(message);
                    }
                }
            }
        }
    }

    private static final class SelectedSample
    {
        private final ByteString key;
        private final int bucket;
        private final IndexProto.RowLocation oldLocation;

        private SelectedSample(ByteString key, int bucket, IndexProto.RowLocation oldLocation)
        {
            this.key = key;
            this.bucket = bucket;
            this.oldLocation = oldLocation;
        }
    }

    private static final class PreparedInvocation
    {
        private final long firstOperation;
        private final int logicalCount;
        private final List<PreparedBucketCall> calls;
        private final AtomicBoolean executed = new AtomicBoolean(false);

        private PreparedInvocation(long firstOperation, int logicalCount,
                                   List<PreparedBucketCall> calls)
        {
            this.firstOperation = firstOperation;
            this.logicalCount = logicalCount;
            this.calls = calls;
        }
    }

    private static final class PreparedBucketCall
    {
        private final int bucket;
        private final List<IndexProto.PrimaryIndexEntry> entries = new ArrayList<>();
        private final List<IndexProto.RowLocation> expectedPrevious = new ArrayList<>();
        private List<IndexProto.RowLocation> actualPrevious;

        private PreparedBucketCall(int bucket)
        {
            this.bucket = bucket;
        }
    }
}
