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
package io.pixelsdb.pixels.retina.benchmark.visibility;

import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.retina.RetinaResourceManager;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkConfig;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkPhase;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkResult;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkScenario;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkWorker;
import io.pixelsdb.pixels.retina.benchmark.common.OperationRange;
import io.pixelsdb.pixels.retina.benchmark.common.OperationResult;
import io.pixelsdb.pixels.retina.benchmark.runtime.NativeRuntime;

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
 * Throughput scenario for Retina's local visibility deletion operation.
 *
 * <p>The measured call is the same call made by Retina after an old primary
 * index version has been located:</p>
 *
 * <pre>
 * RetinaServerImpl.updateRecord/deleteRecord
 *   -&gt; RetinaResourceManager.deleteRecord(RowLocation, transactionTimestamp)
 *   -&gt; RetinaResourceManager.deleteRecord(fileId, rgId, offset, timestamp)
 *   -&gt; checkRGVisibility(...)
 *   -&gt; RGVisibility.deleteRecord(...)
 *   -&gt; JNI RGVisibility::deleteRGRecord
 *   -&gt; TileVisibility::deleteTileRecord
 * </pre>
 *
 * <p>No QueryVisibility RPC is involved in an update, so using that RPC here
 * would measure the analytical read path rather than the update path.  This
 * scenario therefore invokes the production local method directly.  It does
 * not call transaction, index, or write-buffer code.</p>
 *
 * <p>Every measured row group is initialized outside the timed interval with
 * the same API used when Retina creates a new write-buffer file:
 * {@code addVisibility(fileId, rgId, recordNum, 0, null, false)}.  A physical
 * row is deleted exactly once.  Each worker exclusively owns its files and row
 * groups, which preserves increasing timestamps within every native tile even
 * when many workers run concurrently.  Warmup uses distinct file IDs and is
 * reclaimed before measurement.</p>
 */
public final class VisibilityBenchmarkScenario implements BenchmarkScenario
{
    /** TileVisibility stores only the low 48 bits of a timestamp. */
    private static final long MAX_NATIVE_TIMESTAMP = (1L << 48) - 1L;
    private static final long MEASUREMENT_TIMESTAMP_BASE = 1L << 46;
    private static final int DEFAULT_ROWS_PER_ROW_GROUP = 65_536;

    private final Set<VisibilityWorker> workers = Collections.newSetFromMap(
            new ConcurrentHashMap<VisibilityWorker, Boolean>());
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicReference<String> firstDeleteError = new AtomicReference<>();

    private BenchmarkConfig benchmarkConfig;
    private RetinaResourceManager resourceManager;
    private int rowsPerRowGroup;
    private long fileIdBase;

    @Override
    public String name()
    {
        return "visibility";
    }

    @Override
    public void setup(BenchmarkConfig config) throws Exception
    {
        if (this.benchmarkConfig != null)
        {
            throw new IllegalStateException("visibility scenario is already set up");
        }
        String osName = System.getProperty("os.name", "").toLowerCase();
        if (!osName.contains("linux"))
        {
            throw new IllegalStateException("the real Retina visibility JNI library is Linux-only; found "
                    + System.getProperty("os.name"));
        }

        this.benchmarkConfig = config;
        this.rowsPerRowGroup = positiveIntOption(config, "visibility-rows-per-rg",
                DEFAULT_ROWS_PER_ROW_GROUP);

        /*
         * These values must be set before RetinaResourceManager.Instance().
         * A scheduled GC would mutate the native deletion chains during the
         * run, and storage GC can install dual-write redirections that make one
         * delete touch more than the requested RG.
         */
        ConfigFactory pixelsConfig = ConfigFactory.Instance();
        pixelsConfig.addProperty("retina.gc.interval", "0");
        pixelsConfig.addProperty("retina.storage.gc.enabled", "false");
        if (pixelsConfig.getProperty("retina.checkpoint.threads") == null)
        {
            pixelsConfig.addProperty("retina.checkpoint.threads", "1");
        }
        if (pixelsConfig.getProperty("retina.checkpoint.dir") == null)
        {
            pixelsConfig.addProperty("retina.checkpoint.dir",
                    "file:///tmp/pixels-retina-visibility-benchmark-checkpoints");
        }
        if (pixelsConfig.getProperty("node.virtual.num") == null)
        {
            pixelsConfig.addProperty("node.virtual.num", "1");
        }
        if (!"0".equals(pixelsConfig.getProperty("retina.gc.interval"))
                || Boolean.parseBoolean(pixelsConfig.getProperty("retina.storage.gc.enabled")))
        {
            throw new IllegalStateException("visibility benchmark requires both Retina GC mechanisms disabled");
        }

        /* Extract/load the fat-JAR JNI runtime before RGVisibility initializes. */
        NativeRuntime.prepareRetinaLibrary();
        this.resourceManager = RetinaResourceManager.Instance();
        long runBits = (System.currentTimeMillis() & 0x0fffffffL) << 20;
        this.fileIdBase = 1_000_000L + runBits + (System.nanoTime() & 0xfffffL);
    }

    @Override
    public void preparePhase(BenchmarkPhase phase, long totalOperations)
    {
        ensureSetUp();
        if (totalOperations < 0)
        {
            throw new IllegalArgumentException("visibility operation count must not be negative");
        }
        long timestampBase = timestampBase(phase);
        if (totalOperations > MAX_NATIVE_TIMESTAMP - timestampBase)
        {
            throw new IllegalArgumentException("visibility timestamps exceed the native 48-bit representation; "
                    + "reduce the phase operation count");
        }
    }

    @Override
    public BenchmarkWorker createWorker(BenchmarkPhase phase, int workerId, int clientId,
                                        OperationRange operationRange)
    {
        ensureSetUp();
        if (clientId < 0 || clientId >= benchmarkConfig.clients())
        {
            throw new IllegalArgumentException("invalid local visibility client shard: " + clientId);
        }
        long phaseOffset = phase == BenchmarkPhase.WARMUP ? 0L : benchmarkConfig.threads() + 1L;
        long fileId = fileIdBase + phaseOffset + workerId + 1L;
        VisibilityWorker worker = new VisibilityWorker(phase, workerId, clientId, fileId);
        workers.add(worker);
        return worker;
    }

    @Override
    public void completePhase(BenchmarkPhase phase, BenchmarkResult result) throws Exception
    {
        /* Warmup JNI objects are not reused by measurement. */
        closeWorkers(phase);
    }

    @Override
    public Map<String, String> details()
    {
        Map<String, String> details = new LinkedHashMap<>();
        details.put("call", "RetinaResourceManager.deleteRecord(RowLocation,timestamp) (local JNI)");
        details.put("initialization", "addVisibility(fileId,rgId,recordNum,0,null,false)");
        details.put("rowsPerRowGroup", Integer.toString(rowsPerRowGroup));
        details.put("configuredNativeTileCapacity",
                ConfigFactory.Instance().getProperty("retina.tile.visibility.capacity"));
        details.put("logicalOperation", "one unique physical-row deletion; one local API/JNI call");
        details.put("clients", "local load-generator groups; each worker exclusively owns its file/RGs");
        details.put("warmup", "separate file IDs; reclaimed with reclaimVisibility before measurement");
        details.put("gc", "retina.gc.interval=0, retina.storage.gc.enabled=false");
        details.put("outerUpdateLock", "excluded: benchmark starts at RetinaResourceManager module boundary");
        details.put("state", "finite unique rows; deletion chains grow until phase cleanup and are never reused");
        String error = firstDeleteError.get();
        if (error != null)
        {
            details.put("firstDeleteError", error);
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
                closeWorkers(phase);
            }
            catch (Exception e)
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

    private void closeWorkers(BenchmarkPhase phase) throws Exception
    {
        Exception first = null;
        List<VisibilityWorker> snapshot = new ArrayList<>(workers);
        for (VisibilityWorker worker : snapshot)
        {
            if (worker.phase != phase)
            {
                continue;
            }
            try
            {
                worker.close();
            }
            catch (Exception e)
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

    private void ensureSetUp()
    {
        if (benchmarkConfig == null)
        {
            throw new IllegalStateException("visibility scenario has not been set up");
        }
        if (closed.get())
        {
            throw new IllegalStateException("visibility scenario is closed");
        }
    }

    private static long timestampBase(BenchmarkPhase phase)
    {
        return phase == BenchmarkPhase.WARMUP ? 1L : MEASUREMENT_TIMESTAMP_BASE;
    }

    private static String option(BenchmarkConfig config, String key, String defaultValue)
    {
        String value = config.options().get(key);
        return value == null ? defaultValue : value;
    }

    private static int positiveIntOption(BenchmarkConfig config, String key, int defaultValue)
    {
        int parsed = Integer.parseInt(option(config, key, Integer.toString(defaultValue)));
        if (parsed <= 0)
        {
            throw new IllegalArgumentException("--" + key + " must be positive");
        }
        return parsed;
    }

    private final class VisibilityWorker implements BenchmarkWorker
    {
        private final BenchmarkPhase phase;
        private final int workerId;
        private final int clientId;
        private final long fileId;
        private final AtomicBoolean workerClosed = new AtomicBoolean(false);
        private long preparedFirst;
        private long preparedCount;
        private PreparedDelete[] deletes;
        private int rowGroupCount;
        private long nextOperation;
        private int firstSuccessfulDelete = -1;
        private int lastSuccessfulDelete = -1;

        private VisibilityWorker(BenchmarkPhase phase, int workerId, int clientId, long fileId)
        {
            this.phase = phase;
            this.workerId = workerId;
            this.clientId = clientId;
            this.fileId = fileId;
        }

        @Override
        public void prepare(long firstOperation, long operationCount, int batchSize)
        {
            if (deletes != null)
            {
                throw new IllegalStateException("visibility worker was prepared twice");
            }
            if (firstOperation < 0 || operationCount < 0
                    || firstOperation > MAX_NATIVE_TIMESTAMP - timestampBase(phase) - operationCount)
            {
                throw new IllegalArgumentException("invalid visibility operation range");
            }
            if (operationCount > Integer.MAX_VALUE)
            {
                throw new IllegalArgumentException("one visibility worker cannot prepare more than "
                        + Integer.MAX_VALUE + " physical rows");
            }
            if (batchSize <= 0)
            {
                throw new IllegalArgumentException("visibility batch size must be positive");
            }

            this.preparedFirst = firstOperation;
            this.preparedCount = operationCount;
            this.nextOperation = firstOperation;
            this.deletes = new PreparedDelete[(int) operationCount];
            this.rowGroupCount = (int) ((operationCount + rowsPerRowGroup - 1L) / rowsPerRowGroup);

            for (int rgId = 0; rgId < rowGroupCount; rgId++)
            {
                long remaining = operationCount - (long) rgId * rowsPerRowGroup;
                int recordCount = (int) Math.min((long) rowsPerRowGroup, remaining);
                /* Exact FileWriterManager initialization shape, never overwrite. */
                resourceManager.addVisibility(fileId, rgId, recordCount, 0L, null, false);
            }

            long base = timestampBase(phase);
            for (int i = 0; i < deletes.length; i++)
            {
                int rgId = i / rowsPerRowGroup;
                int rgRowOffset = i % rowsPerRowGroup;
                IndexProto.RowLocation location = IndexProto.RowLocation.newBuilder()
                        .setFileId(fileId)
                        .setRgId(rgId)
                        .setRgRowOffset(rgRowOffset)
                        .build();
                /* Global operation IDs make timestamps unique across workers. */
                deletes[i] = new PreparedDelete(location, base + firstOperation + i);
            }
        }

        @Override
        public OperationResult execute(long firstOperation, int logicalOperationCount)
        {
            if (workerClosed.get())
            {
                return new OperationResult(logicalOperationCount, 0, logicalOperationCount,
                        logicalOperationCount);
            }
            if (deletes == null)
            {
                throw new IllegalStateException("visibility worker has not been prepared");
            }
            if (firstOperation != nextOperation)
            {
                throw new IllegalArgumentException("visibility operations must execute once in increasing order; "
                        + "expected " + nextOperation + " but got " + firstOperation);
            }
            long relative = firstOperation - preparedFirst;
            if (relative < 0 || relative + logicalOperationCount > preparedCount)
            {
                throw new IllegalArgumentException("visibility execute range is outside prepared rows");
            }

            long successful = 0;
            long errors = 0;
            int start = (int) relative;
            for (int i = 0; i < logicalOperationCount; i++)
            {
                PreparedDelete delete = deletes[start + i];
                try
                {
                    /* This is the complete timed target call; no other module is invoked. */
                    resourceManager.deleteRecord(delete.location, delete.timestamp);
                    if (firstSuccessfulDelete < 0)
                    {
                        firstSuccessfulDelete = start + i;
                    }
                    lastSuccessfulDelete = start + i;
                    successful++;
                }
                catch (Exception e)
                {
                    firstDeleteError.compareAndSet(null, e.getClass().getName() + ": "
                            + String.valueOf(e.getMessage()));
                    errors++;
                }
            }
            nextOperation += logicalOperationCount;
            return new OperationResult(logicalOperationCount, successful, errors,
                    logicalOperationCount);
        }

        @Override
        public long expectedApiCalls(int logicalOperationCount)
        {
            return logicalOperationCount;
        }

        @Override
        public void close() throws Exception
        {
            if (!workerClosed.compareAndSet(false, true))
            {
                return;
            }
            Exception first = null;
            try
            {
                /* Validate the void JNI mutation after timing and before native-state reclaim. */
                if (firstSuccessfulDelete >= 0)
                {
                    assertDeleted(deletes[firstSuccessfulDelete]);
                    if (lastSuccessfulDelete != firstSuccessfulDelete)
                    {
                        assertDeleted(deletes[lastSuccessfulDelete]);
                    }
                }
            }
            catch (Exception e)
            {
                first = e;
            }
            for (int rgId = 0; rgId < rowGroupCount; rgId++)
            {
                try
                {
                    resourceManager.reclaimVisibility(fileId, rgId, 0L);
                }
                catch (RetinaException e)
                {
                    if (first == null)
                    {
                        first = e;
                    }
                }
            }
            workers.remove(this);
            deletes = null;
            if (first != null)
            {
                throw first;
            }
        }

        private void assertDeleted(PreparedDelete delete) throws RetinaException
        {
            IndexProto.RowLocation location = delete.location;
            long[] bitmap = resourceManager.queryVisibility(location.getFileId(), location.getRgId(),
                    MAX_NATIVE_TIMESTAMP);
            int offset = location.getRgRowOffset();
            int word = offset >>> 6;
            long bit = 1L << (offset & 63);
            if (word >= bitmap.length || (bitmap[word] & bit) == 0)
            {
                throw new IllegalStateException("visibility validation failed for fileId="
                        + location.getFileId() + ", rgId=" + location.getRgId()
                        + ", rgRowOffset=" + offset);
            }
        }
    }

    private static final class PreparedDelete
    {
        private final IndexProto.RowLocation location;
        private final long timestamp;

        private PreparedDelete(IndexProto.RowLocation location, long timestamp)
        {
            this.location = location;
            this.timestamp = timestamp;
        }
    }
}
