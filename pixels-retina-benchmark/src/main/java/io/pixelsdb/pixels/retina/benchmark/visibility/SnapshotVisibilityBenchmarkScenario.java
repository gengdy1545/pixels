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
import io.pixelsdb.pixels.common.utils.CheckpointFileIO;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.retina.RGVisibility;
import io.pixelsdb.pixels.retina.RetinaResourceManager;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkConfig;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkPhase;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkResult;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkScenario;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkWorker;
import io.pixelsdb.pixels.retina.benchmark.common.OperationRange;
import io.pixelsdb.pixels.retina.benchmark.common.OperationResult;
import io.pixelsdb.pixels.retina.benchmark.runtime.NativeRuntime;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotIO;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotManifest;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotRuntime;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Visibility-delete throughput benchmark restored from a Retina snapshot.
 *
 * <p>The measured operation begins at the same local module boundary used by
 * Retina after an old primary-index version supplies a physical location:</p>
 *
 * <pre>
 * RetinaServerImpl.updateRecord/deleteRecord
 *   -&gt; RetinaResourceManager.deleteRecord(RowLocation, transactionTimestamp)
 *   -&gt; RetinaResourceManager.deleteRecord(fileId, rgId, offset, timestamp)
 *   -&gt; RGVisibility.deleteRecord(offset, timestamp)
 *   -&gt; JNI RGVisibility::deleteRGRecord
 *   -&gt; TileVisibility::deleteTileRecord
 * </pre>
 *
 * <p>Visibility is not updated through an RPC in the production update path,
 * so the timed loop invokes this real local call directly.  Transaction,
 * Index and WriteBuffer operations are not executed.</p>
 *
 * <p>When {@code snapshot.json} declares a Visibility checkpoint, every phase
 * restores it through the production checkpoint reader:</p>
 *
 * <pre>
 * CheckpointFileIO.readCheckpointParallel
 *   -&gt; RetinaResourceManager.addVisibility(fileId, rgId, recordNum,
 *                                           snapshotTimestamp, bitmap, true)
 * </pre>
 *
 * <p>Manifest row groups not represented by the checkpoint are initialized
 * through the real clean-state API using the footer's {@code recordNum}.  If
 * the manifest declares no checkpoint, every row group uses that clean path.
 * The row-group topology mirrors {@code RetinaServerImpl} startup: for every
 * readable layout it loads files only from {@code orderedPaths.get(0)} and
 * {@code compactPaths.get(0)}.  The snapshot manifest records precisely those
 * paths with {@code productionSelectedByRetina=true}; secondary and projection
 * paths are excluded.
 * Restore, request construction, verification and native-object cleanup all
 * run outside the measured interval.  Warmup and measurement use disjoint
 * physical rows and each phase starts from a fresh restore.</p>
 */
public final class SnapshotVisibilityBenchmarkScenario implements BenchmarkScenario
{
    private static final String VISIBILITY_CHECKPOINT = "state/visibility/gc-checkpoint.bin";
    /** TileVisibility stores transaction timestamps in 48 bits. */
    private static final long MAX_NATIVE_TIMESTAMP = (1L << 48) - 1L;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicReference<String> firstDeleteError = new AtomicReference<>();

    private BenchmarkConfig config;
    private SnapshotRuntime snapshotRuntime;
    private SnapshotManifest manifest;
    private SnapshotManifest.TableState table;
    private RetinaResourceManager resourceManager;
    private ExecutorService checkpointReaderExecutor;
    private Path checkpointPath;
    private String checkpointUri;
    private boolean checkpointIncluded;
    private int checkpointEntryCount;
    private int checkpointTableEntryCount;
    private int tileCapacity;
    private int nativeTileCapacity;
    private long deleteTimestamp;
    private long warmupOperationCount;
    private long manifestRowCount;
    private int productionSelectedPathCount;
    private long invalidTimestampSamples;
    private long duplicateLocationSamples;
    private int measurementIndexSamples;
    private int warmupIndexSamples;
    private long lastValidatedDeletes;
    private int lastCheckpointRestores;
    private int lastCleanRestores;

    private List<RowGroupSpec> rowGroups;
    private Map<RowGroupKey, RowGroupSpec> rowGroupsByKey;
    private Set<RowGroupKey> checkpointRowGroups = Collections.emptySet();
    private PreparedDelete[] warmupDeletes;
    private PreparedDelete[] measurementDeletes;
    private PhaseExecution activePhase;

    @Override
    public String name()
    {
        return "visibility";
    }

    @Override
    public void setup(BenchmarkConfig config) throws Exception
    {
        if (this.config != null)
        {
            throw new IllegalStateException("snapshot visibility scenario is already set up");
        }
        this.config = config;
        this.snapshotRuntime = SnapshotRuntime.open(config);
        this.manifest = snapshotRuntime.manifest();
        this.table = snapshotRuntime.requireTable(config.require("snapshot-table"));

        validatePlatformAndTimestamp();
        configureRuntime();

        /* The checkpoint's tile layout is a native compile-time contract. */
        NativeRuntime.prepareRetinaLibrary();
        try
        {
            this.nativeTileCapacity = RGVisibility.getTileCapacity();
        }
        catch (UnsatisfiedLinkError e)
        {
            throw new IllegalStateException("loaded libpixels-retina.so does not expose its "
                    + "compiled RETINA_CAPACITY; rebuild pixels-retina", e);
        }
        if (nativeTileCapacity != tileCapacity)
        {
            throw new IllegalStateException("snapshot/runtime retina.tile.visibility.capacity="
                    + tileCapacity + " but loaded libpixels-retina.so RETINA_CAPACITY="
                    + nativeTileCapacity + "; rebuild the fat JAR with the snapshot capacity");
        }

        buildRowGroupTopology();
        configureCheckpoint();

        CheckpointScan checkpoint = checkpointIncluded
                ? scanCheckpoint()
                : new CheckpointScan(0, Collections.<RowGroupKey, CheckpointState>emptyMap());
        this.checkpointEntryCount = checkpoint.totalEntries;
        this.checkpointTableEntryCount = checkpoint.entries.size();
        this.checkpointRowGroups = Collections.unmodifiableSet(
                new LinkedHashSet<>(checkpoint.entries.keySet()));

        prepareOperationData(checkpoint.entries);

        this.resourceManager = RetinaResourceManager.Instance();
    }

    @Override
    public void preparePhase(BenchmarkPhase phase, long totalOperations) throws Exception
    {
        ensureSetUp();
        if (activePhase != null)
        {
            throw new IllegalStateException("previous visibility phase was not completed");
        }
        PreparedDelete[] operations = operations(phase);
        if (totalOperations != operations.length)
        {
            throw new IllegalArgumentException("unexpected " + phase + " operation count: "
                    + totalOperations + ", expected " + operations.length);
        }

        PhaseExecution execution = new PhaseExecution(phase, operations);
        Exception failure = null;
        try
        {
            /* A benchmark JVM owns these restored objects; remove any stale phase first. */
            cleanupAllRowGroups();
            restoreVisibility(execution);
            this.activePhase = execution;
            this.lastCheckpointRestores = execution.restoredFromCheckpoint.size();
            this.lastCleanRestores = execution.cleanRestores;
        }
        catch (Exception e)
        {
            failure = e;
        }

        if (failure != null)
        {
            try
            {
                cleanupAllRowGroups();
            }
            catch (Exception cleanup)
            {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    @Override
    public BenchmarkWorker createWorker(BenchmarkPhase phase, int workerId, int clientId,
                                        OperationRange range)
    {
        ensureSetUp();
        PhaseExecution execution = requireActivePhase(phase);
        if (clientId < 0 || clientId >= config.clients())
        {
            throw new IllegalArgumentException("invalid local visibility client shard: " + clientId);
        }
        if (range.endExclusive() > execution.operations.length)
        {
            throw new IllegalArgumentException("worker range exceeds prepared snapshot rows");
        }
        SnapshotVisibilityWorker worker = new SnapshotVisibilityWorker(
                execution, workerId, clientId, range);
        execution.workers.add(worker);
        return worker;
    }

    @Override
    public void completePhase(BenchmarkPhase phase, BenchmarkResult result) throws Exception
    {
        PhaseExecution execution = requireActivePhase(phase);
        Exception failure = null;
        try
        {
            verifyCompletedPhase(execution, result);
        }
        catch (Exception e)
        {
            failure = e;
        }
        finally
        {
            this.activePhase = null;
            try
            {
                /* reclaimVisibility closes every native RGVisibility object. */
                cleanupAllRowGroups();
            }
            catch (Exception cleanup)
            {
                if (failure == null)
                {
                    failure = cleanup;
                }
                else
                {
                    failure.addSuppressed(cleanup);
                }
            }
        }
        if (failure != null)
        {
            throw failure;
        }
    }

    @Override
    public Map<String, String> details()
    {
        Map<String, String> details = new LinkedHashMap<>();
        details.put("call", "RetinaResourceManager.deleteRecord(RowLocation,timestamp) (local JNI)");
        details.put("callChain", "RetinaResourceManager.deleteRecord -> RGVisibility.deleteRecord -> JNI");
        details.put("snapshotTable", table == null ? "not-initialized"
                : table.schemaName + "." + table.tableName);
        details.put("snapshotSourceHost", manifest == null ? "not-initialized" : manifest.sourceHost);
        details.put("snapshotSourceQuiesced", manifest == null ? "false"
                : Boolean.toString(manifest.sourceQuiesced));
        details.put("snapshotConsistency", manifest == null ? "not-initialized"
                : String.valueOf(manifest.consistencyNote));
        details.put("snapshotTimestamp", manifest == null ? "0"
                : Long.toString(manifest.snapshotTimestamp));
        details.put("deleteTimestamp", Long.toString(deleteTimestamp));
        details.put("configuredTileCapacity", Integer.toString(tileCapacity));
        details.put("nativeCompiledTileCapacity", Integer.toString(nativeTileCapacity));
        details.put("manifestVisibilityRGs", rowGroups == null ? "0"
                : Integer.toString(rowGroups.size()));
        details.put("manifestVisibilityRows", Long.toString(manifestRowCount));
        details.put("productionSelectedVisibilityPaths",
                Integer.toString(productionSelectedPathCount));
        details.put("visibilityRGScope", "readable layouts; only orderedPaths[0]/compactPaths[0] "
                + "marked productionSelectedByRetina; projections and secondary paths excluded");
        details.put("checkpointDeclared", Boolean.toString(checkpointIncluded));
        details.put("checkpointPath", checkpointPath == null ? "none" : checkpointPath.toString());
        details.put("checkpointEntries", Integer.toString(checkpointEntryCount));
        details.put("checkpointEntriesForTable", Integer.toString(checkpointTableEntryCount));
        details.put("checkpointType", manifest == null ? "none"
                : String.valueOf(manifest.visibilityCheckpointType));
        details.put("checkpointCoverage", manifest == null ? "0/0"
                : manifest.visibilityCheckpointMatchedEntryCount + "/"
                + manifest.visibilityCheckpointExpectedEntryCount);
        details.put("restore", checkpointIncluded
                ? "CheckpointFileIO.readCheckpointParallel -> addVisibility(snapshotTimestamp,bitmap,true); missing RGs use footer clean baseline"
                : "footer recordNum -> addVisibility(fileId,rgId,recordNum,0,null,false)");
        details.put("lastCheckpointRGRestores", Integer.toString(lastCheckpointRestores));
        details.put("lastCleanRGRestores", Integer.toString(lastCleanRestores));
        details.put("measurementIndexSampleLocations", Integer.toString(measurementIndexSamples));
        details.put("warmupIndexSampleLocations", Integer.toString(warmupIndexSamples));
        details.put("indexSamplesSkippedNewerThanSnapshot", Long.toString(invalidTimestampSamples));
        details.put("indexSampleDuplicateLocations", Long.toString(duplicateLocationSamples));
        details.put("warmupMeasurementRows", "disjoint; each phase restores the same base state");
        details.put("logicalOperation", "one unique physical-row deletion; one local API/JNI call");
        details.put("transactionTimestamp", "one constant real timestamp per restored phase");
        details.put("gc", "retina.gc.interval=0, retina.storage.gc.enabled=false");
        details.put("postRunValidation", "queryVisibility checks every successful delete outside timing");
        details.put("lastValidatedDeletes", Long.toString(lastValidatedDeletes));
        details.put("nativeCleanup", "all restored RGs reclaimed after every phase");
        details.put("excludedModules", "Transaction, Index mutation, WriteBuffer and Visibility RPC");
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
        this.activePhase = null;
        if (resourceManager != null && rowGroups != null)
        {
            try
            {
                cleanupAllRowGroups();
            }
            catch (Exception e)
            {
                first = e;
            }
        }
        if (checkpointReaderExecutor != null)
        {
            checkpointReaderExecutor.shutdownNow();
            try
            {
                if (!checkpointReaderExecutor.awaitTermination(5, TimeUnit.SECONDS))
                {
                    IOException timeout = new IOException("checkpoint reader executor did not terminate");
                    if (first == null) first = timeout;
                    else first.addSuppressed(timeout);
                }
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                if (first == null) first = e;
                else first.addSuppressed(e);
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
        warmupDeletes = null;
        measurementDeletes = null;
        if (first != null)
        {
            throw first;
        }
    }

    private void validatePlatformAndTimestamp()
    {
        String osName = System.getProperty("os.name", "").toLowerCase();
        if (!osName.contains("linux"))
        {
            throw new IllegalStateException("the real Retina visibility JNI library is Linux-only; found "
                    + System.getProperty("os.name"));
        }
        if (manifest.snapshotTimestamp < 0 || manifest.snapshotTimestamp > MAX_NATIVE_TIMESTAMP)
        {
            throw new IllegalArgumentException("snapshotTimestamp is outside Retina's 48-bit range: "
                    + manifest.snapshotTimestamp);
        }
        if (table.maxObservedCreateTimestamp < 0
                || table.maxObservedCreateTimestamp > MAX_NATIVE_TIMESTAMP)
        {
            throw new IllegalArgumentException("sampled create timestamp is outside Retina's 48-bit range: "
                    + table.maxObservedCreateTimestamp);
        }
        long base = Math.max(manifest.snapshotTimestamp, table.maxObservedCreateTimestamp);
        if (base >= MAX_NATIVE_TIMESTAMP)
        {
            throw new IllegalArgumentException("snapshot leaves no 48-bit transaction timestamp for deletes");
        }
        this.deleteTimestamp = base + 1L;

        this.warmupOperationCount = config.warmupSeconds() > 0 && config.warmupOperations() > 0
                ? config.warmupOperations() : 0L;
        long combined = Math.addExact(warmupOperationCount, config.dataSize());
        if (combined > Integer.MAX_VALUE)
        {
            throw new IllegalArgumentException("snapshot visibility benchmark supports at most "
                    + Integer.MAX_VALUE + " warmup + measurement physical rows");
        }
    }

    private void configureRuntime() throws IOException
    {
        snapshotRuntime.applyAllConfig();
        ConfigFactory pixels = ConfigFactory.Instance();

        /* The copied checkpoint is always a local file, independent of source storage. */
        pixels.addProperty("enabled.storage.schemes",
                includeScheme(pixels.getProperty("enabled.storage.schemes"), "file"));
        pixels.addProperty("localfs.enable.direct.io", "false");
        pixels.addProperty("localfs.enable.mmap", "false");
        pixels.addProperty("localfs.enable.async.io", "false");
        if (pixels.getProperty("localfs.block.size") == null)
        {
            pixels.addProperty("localfs.block.size", "4096");
        }
        if (pixels.getProperty("localfs.reader.threads") == null)
        {
            pixels.addProperty("localfs.reader.threads", Integer.toString(Math.max(1, config.threads())));
        }

        /* Background mutation would change the target while it is measured. */
        pixels.addProperty("retina.gc.interval", "0");
        pixels.addProperty("retina.storage.gc.enabled", "false");
        int checkpointThreads = positiveOrDefault(
                pixels.getProperty("retina.checkpoint.threads"), Math.max(1, config.clients()));
        pixels.addProperty("retina.checkpoint.threads", Integer.toString(checkpointThreads));
        if (pixels.getProperty("node.virtual.num") == null)
        {
            pixels.addProperty("node.virtual.num", "1");
        }
        Path scratchCheckpoint = snapshotRuntime.workDirectory().resolve("visibility-checkpoints");
        Files.createDirectories(scratchCheckpoint);
        pixels.addProperty("retina.checkpoint.dir", scratchCheckpoint.toUri().toString());

        String manifestCapacity = manifest.effectiveConfig.get("retina.tile.visibility.capacity");
        if (manifestCapacity == null || manifestCapacity.trim().isEmpty())
        {
            throw new IllegalArgumentException("snapshot effectiveConfig lacks "
                    + "retina.tile.visibility.capacity");
        }
        this.tileCapacity = Integer.parseInt(manifestCapacity);
        if (tileCapacity <= 0 || (tileCapacity & 63) != 0 || tileCapacity > 65_536)
        {
            throw new IllegalArgumentException("retina.tile.visibility.capacity must be in [64,65536] "
                    + "and divisible by 64, found " + tileCapacity);
        }
        if (!"0".equals(pixels.getProperty("retina.gc.interval"))
                || Boolean.parseBoolean(pixels.getProperty("retina.storage.gc.enabled")))
        {
            throw new IllegalStateException("snapshot visibility benchmark requires both GC mechanisms disabled");
        }

        AtomicInteger threadIds = new AtomicInteger();
        this.checkpointReaderExecutor = Executors.newFixedThreadPool(checkpointThreads, runnable ->
        {
            Thread thread = new Thread(runnable,
                    "snapshot-visibility-checkpoint-reader-" + threadIds.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        });
    }

    private void buildRowGroupTopology()
    {
        Set<Long> readableLayouts = new HashSet<>();
        Map<LayoutPathKey, String> productionPaths = new HashMap<>();
        for (SnapshotManifest.LayoutState layout : table.layouts)
        {
            if (layout.readable)
            {
                readableLayouts.add(layout.layoutId);
                for (SnapshotManifest.PathState path : layout.paths)
                {
                    if (!path.productionSelectedByRetina)
                    {
                        continue;
                    }
                    if (path.pathId <= 0 || !("ordered".equalsIgnoreCase(path.role)
                            || "compact".equalsIgnoreCase(path.role)))
                    {
                        throw new IllegalArgumentException("invalid production-selected Retina path "
                                + "for layoutId=" + layout.layoutId + ", pathId=" + path.pathId
                                + ", role=" + path.role);
                    }
                    LayoutPathKey key = new LayoutPathKey(layout.layoutId, path.pathId);
                    if (productionPaths.put(key, path.role) != null)
                    {
                        throw new IllegalArgumentException("duplicate production-selected Retina path: "
                                + key);
                    }
                }
            }
        }
        if (readableLayouts.isEmpty())
        {
            throw new IllegalArgumentException("snapshot table has no readable layout: " + table.tableName);
        }
        if (productionPaths.isEmpty())
        {
            throw new IllegalArgumentException("snapshot table has no production-selected "
                    + "ordered/compact path in a readable layout: " + table.tableName);
        }
        this.productionSelectedPathCount = productionPaths.size();

        List<RowGroupSpec> selected = new ArrayList<>();
        Map<RowGroupKey, RowGroupSpec> byKey = new HashMap<>();
        long rows = 0L;
        for (SnapshotManifest.FileState file : table.files)
        {
            if (!readableLayouts.contains(file.layoutId))
            {
                continue;
            }
            String selectedRole = productionPaths.get(new LayoutPathKey(file.layoutId, file.pathId));
            if (selectedRole == null)
            {
                continue;
            }
            if (!selectedRole.equalsIgnoreCase(file.layoutRole))
            {
                throw new IllegalArgumentException("manifest file role does not match its "
                        + "production-selected Retina path for fileId=" + file.fileId
                        + ": " + file.layoutRole + " != " + selectedRole);
            }
            if (file.fileId <= 0 || file.rowGroups == null
                    || file.footerRowGroupCount != file.rowGroups.size()
                    || file.metadataNumRowGroups != file.footerRowGroupCount)
            {
                throw new IllegalArgumentException("invalid manifest file/RG topology for fileId="
                        + file.fileId);
            }
            long fileRows = 0L;
            for (int rowGroupOrdinal = 0; rowGroupOrdinal < file.rowGroups.size(); rowGroupOrdinal++)
            {
                SnapshotManifest.RowGroupState rowGroup = file.rowGroups.get(rowGroupOrdinal);
                if (rowGroup.rgId != rowGroupOrdinal || rowGroup.recordNum < 0)
                {
                    throw new IllegalArgumentException("invalid manifest row group for fileId="
                            + file.fileId + ", rgId=" + rowGroup.rgId);
                }
                RowGroupKey key = new RowGroupKey(file.fileId, rowGroup.rgId);
                RowGroupSpec spec = new RowGroupSpec(key, rowGroup.recordNum);
                if (byKey.put(key, spec) != null)
                {
                    throw new IllegalArgumentException("duplicate manifest row group: " + key);
                }
                selected.add(spec);
                fileRows = Math.addExact(fileRows, rowGroup.recordNum);
            }
            if (fileRows != file.footerRowCount)
            {
                throw new IllegalArgumentException("manifest footer row count mismatch for fileId="
                        + file.fileId + ": " + fileRows + " != " + file.footerRowCount);
            }
            rows = Math.addExact(rows, fileRows);
        }
        if (selected.isEmpty() || rows <= 0)
        {
            throw new IllegalArgumentException("snapshot table has no rows on the "
                    + "production-selected ordered/compact paths of readable layouts");
        }
        selected.sort(Comparator.comparingLong((RowGroupSpec value) -> value.key.fileId)
                .thenComparingInt(value -> value.key.rgId));
        this.rowGroups = Collections.unmodifiableList(selected);
        this.rowGroupsByKey = Collections.unmodifiableMap(byKey);
        this.manifestRowCount = rows;
    }

    private void configureCheckpoint() throws IOException
    {
        this.checkpointIncluded = manifest.visibilityCheckpointIncluded;
        if (checkpointIncluded)
        {
            /* resolveArtifact also requires the exporter checksum declaration. */
            this.checkpointPath = snapshotRuntime.resolveArtifact(VISIBILITY_CHECKPOINT);
        }
        else
        {
            Path undeclared = snapshotRuntime.snapshotDirectory()
                    .resolve(VISIBILITY_CHECKPOINT).normalize();
            if (Files.exists(undeclared, LinkOption.NOFOLLOW_LINKS))
            {
                throw new IOException("snapshot contains an undeclared Visibility checkpoint: "
                        + undeclared + "; regenerate snapshot.json");
            }
        }
        if (checkpointIncluded)
        {
            if (!manifest.sourceQuiesced)
            {
                throw new IllegalArgumentException("Visibility checkpoint restore requires sourceQuiesced=true");
            }
            if (manifest.snapshotTimestamp <= 0)
            {
                throw new IllegalArgumentException("Visibility checkpoint restore requires a positive snapshotTimestamp");
            }
            this.checkpointUri = checkpointPath.toUri().toString();
        }
        else
        {
            this.checkpointPath = null;
            this.checkpointUri = null;
        }
    }

    private CheckpointScan scanCheckpoint() throws IOException
    {
        ConcurrentHashMap<RowGroupKey, CheckpointState> entries = new ConcurrentHashMap<>();
        AtomicReference<RuntimeException> failure = new AtomicReference<>();
        int count = CheckpointFileIO.readCheckpointParallel(checkpointUri, entry ->
        {
            RowGroupKey key = new RowGroupKey(entry.fileId, entry.rgId);
            RowGroupSpec spec = rowGroupsByKey.get(key);
            if (spec == null || failure.get() != null)
            {
                return;
            }
            try
            {
                validateCheckpointEntry(spec, entry);
                CheckpointState previous = entries.putIfAbsent(key,
                        new CheckpointState(entry.recordNum, entry.bitmap));
                if (previous != null)
                {
                    throw new IllegalArgumentException("duplicate checkpoint row group: " + key);
                }
            }
            catch (RuntimeException e)
            {
                failure.compareAndSet(null, e);
            }
        }, checkpointReaderExecutor);
        if (count < 0)
        {
            throw new IOException("invalid negative checkpoint entry count: " + count);
        }
        if (failure.get() != null)
        {
            throw new IOException("invalid Visibility checkpoint", failure.get());
        }
        return new CheckpointScan(count, entries);
    }

    private void validateCheckpointEntry(RowGroupSpec spec, CheckpointFileIO.CheckpointEntry entry)
    {
        if (entry.fileId != spec.key.fileId || entry.rgId != spec.key.rgId)
        {
            throw new IllegalArgumentException("checkpoint callback location mismatch");
        }
        if (entry.recordNum < spec.recordNum)
        {
            throw new IllegalArgumentException("checkpoint recordNum is smaller than footer for "
                    + spec.key + ": " + entry.recordNum + " < " + spec.recordNum);
        }
        long tileCount = (entry.recordNum + (long) tileCapacity - 1L) / tileCapacity;
        long requiredWords = tileCount * (tileCapacity / 64L);
        if (entry.bitmap.length < requiredWords)
        {
            throw new IllegalArgumentException("checkpoint bitmap is too short for " + spec.key
                    + ": " + entry.bitmap.length + " < " + requiredWords);
        }
    }

    private void prepareOperationData(Map<RowGroupKey, CheckpointState> checkpoint) throws Exception
    {
        int measurementRequired = Math.toIntExact(config.dataSize());
        int warmupRequired = Math.toIntExact(warmupOperationCount);
        List<PreparedDelete> measurement = new ArrayList<>(measurementRequired);
        List<PreparedDelete> warmup = new ArrayList<>(warmupRequired);
        Set<LocationKey> reserved = new HashSet<>(initialHashCapacity(
                Math.addExact(measurementRequired, warmupRequired)));

        if (table.indexSamplesFile != null && table.indexSampleCount > 0)
        {
            Path samplesPath = snapshotRuntime.resolveArtifact(table.indexSamplesFile);
            long sampleCount = SnapshotIO.validateIndexSamples(samplesPath);
            if (sampleCount != table.indexSampleCount)
            {
                throw new IOException("index sample count does not match snapshot manifest: "
                        + sampleCount + " != " + table.indexSampleCount);
            }
            SnapshotIO.scanIndexSamples(samplesPath, sample ->
            {
                RowGroupKey rgKey = new RowGroupKey(sample.fileId, sample.rgId);
                RowGroupSpec spec = rowGroupsByKey.get(rgKey);
                if (spec == null)
                {
                    throw new IllegalArgumentException("index sample points outside the Visibility topology: "
                            + rgKey);
                }
                if (sample.key == null || sample.key.length == 0
                        || sample.rgRowOffset < 0 || sample.rgRowOffset >= spec.recordNum)
                {
                    throw new IllegalArgumentException("index sample has an invalid real RowLocation: "
                            + rgKey + ", offset=" + sample.rgRowOffset);
                }
                if (sample.createTimestamp < 0)
                {
                    throw new IllegalArgumentException("index sample has a negative create timestamp at "
                            + rgKey + ", offset=" + sample.rgRowOffset);
                }
                LocationKey location = new LocationKey(sample.fileId, sample.rgId,
                        sample.rgRowOffset);
                if (sample.createTimestamp > manifest.snapshotTimestamp)
                {
                    /* Do not let deterministic fallback reuse this known future row. */
                    reserved.add(location);
                    invalidTimestampSamples++;
                    return true;
                }
                if (isInitiallyDeleted(checkpoint.get(rgKey), sample.rgRowOffset))
                {
                    return true;
                }
                if (!reserved.add(location))
                {
                    duplicateLocationSamples++;
                    return true;
                }
                PreparedDelete delete = preparedDelete(spec, sample.rgRowOffset);
                if (measurement.size() < measurementRequired)
                {
                    measurement.add(delete);
                    measurementIndexSamples++;
                }
                else if (warmup.size() < warmupRequired)
                {
                    warmup.add(delete);
                    warmupIndexSamples++;
                }
                else
                {
                    return false;
                }
                return measurement.size() < measurementRequired
                        || warmup.size() < warmupRequired;
            });
        }

        fillFromManifest(measurement, measurementRequired, reserved, checkpoint);
        fillFromManifest(warmup, warmupRequired, reserved, checkpoint);
        if (measurement.size() != measurementRequired || warmup.size() != warmupRequired)
        {
            throw new IllegalArgumentException("snapshot has only "
                    + (measurement.size() + warmup.size())
                    + " distinct rows visible at snapshotTimestamp, but benchmark requires "
                    + (measurementRequired + warmupRequired));
        }

        this.measurementDeletes = measurement.toArray(new PreparedDelete[measurement.size()]);
        this.warmupDeletes = warmup.toArray(new PreparedDelete[warmup.size()]);
    }

    /** Round-robin RG traversal avoids creating an artificial single-RG hot spot. */
    private void fillFromManifest(List<PreparedDelete> target, int required,
                                  Set<LocationKey> reserved,
                                  Map<RowGroupKey, CheckpointState> checkpoint)
    {
        if (target.size() >= required)
        {
            return;
        }
        int maximumRecords = 0;
        for (RowGroupSpec spec : rowGroups)
        {
            maximumRecords = Math.max(maximumRecords, spec.recordNum);
        }
        for (int offset = 0; offset < maximumRecords; offset++)
        {
            for (RowGroupSpec spec : rowGroups)
            {
                if (offset >= spec.recordNum
                        || isInitiallyDeleted(checkpoint.get(spec.key), offset))
                {
                    continue;
                }
                LocationKey location = new LocationKey(spec.key.fileId, spec.key.rgId, offset);
                if (!reserved.add(location))
                {
                    continue;
                }
                target.add(preparedDelete(spec, offset));
                if (target.size() == required)
                {
                    return;
                }
            }
        }
    }

    private void restoreVisibility(PhaseExecution execution) throws Exception
    {
        if (checkpointIncluded)
        {
            AtomicReference<RuntimeException> failure = new AtomicReference<>();
            int count = CheckpointFileIO.readCheckpointParallel(checkpointUri, entry ->
            {
                RowGroupKey key = new RowGroupKey(entry.fileId, entry.rgId);
                RowGroupSpec spec = rowGroupsByKey.get(key);
                if (spec == null || failure.get() != null)
                {
                    return;
                }
                try
                {
                    validateCheckpointEntry(spec, entry);
                    if (!execution.restoredFromCheckpoint.add(key))
                    {
                        throw new IllegalArgumentException("duplicate checkpoint row group: " + key);
                    }
                    /* Exact production recovery shape from recoverCheckpoints(). */
                    resourceManager.addVisibility(entry.fileId, entry.rgId, entry.recordNum,
                            manifest.snapshotTimestamp, entry.bitmap, true);
                }
                catch (RuntimeException e)
                {
                    failure.compareAndSet(null, e);
                }
            }, checkpointReaderExecutor);
            if (failure.get() != null)
            {
                throw failure.get();
            }
            if (count < 0)
            {
                throw new IOException("invalid negative checkpoint entry count: " + count);
            }
            if (count != checkpointEntryCount)
            {
                throw new IOException("Visibility checkpoint changed after setup: entries="
                        + count + ", expected=" + checkpointEntryCount);
            }
            if (!execution.restoredFromCheckpoint.equals(checkpointRowGroups))
            {
                throw new IOException("Visibility checkpoint table RG set changed after setup");
            }
        }

        for (RowGroupSpec spec : rowGroups)
        {
            if (!execution.restoredFromCheckpoint.contains(spec.key))
            {
                /* Exact clean initialization shape used by addVisibility(filePath). */
                resourceManager.addVisibility(spec.key.fileId, spec.key.rgId,
                        spec.recordNum, 0L, null, false);
                execution.cleanRestores++;
            }
        }
    }

    private void verifyCompletedPhase(PhaseExecution execution, BenchmarkResult result)
            throws RetinaException
    {
        long trackedAttempted = 0L;
        long trackedErrors = 0L;
        Map<RowGroupKey, BitSet> expectedDeleted = new LinkedHashMap<>();
        for (SnapshotVisibilityWorker worker : execution.workers)
        {
            trackedAttempted += worker.attempted;
            trackedErrors += worker.failures.cardinality();
            for (int relative = 0; relative < worker.attempted; relative++)
            {
                if (worker.failures.get(relative))
                {
                    continue;
                }
                int operation = Math.toIntExact(worker.range.startInclusive() + relative);
                PreparedDelete delete = execution.operations[operation];
                expectedDeleted.computeIfAbsent(delete.rowGroup,
                        ignored -> new BitSet())
                        .set(delete.offset);
            }
        }
        long trackedSuccessful = trackedAttempted - trackedErrors;
        if (trackedAttempted != result.totalOperations()
                || trackedErrors != result.errors()
                || trackedSuccessful != result.successfulOperations())
        {
            throw new IllegalStateException("Visibility worker/result accounting mismatch: tracked="
                    + trackedAttempted + "/" + trackedSuccessful + "/" + trackedErrors
                    + ", result=" + result.totalOperations() + "/"
                    + result.successfulOperations() + "/" + result.errors());
        }

        long validated = 0L;
        for (Map.Entry<RowGroupKey, BitSet> entry : expectedDeleted.entrySet())
        {
            RowGroupKey key = entry.getKey();
            long[] bitmap = resourceManager.queryVisibility(
                    key.fileId, key.rgId, deleteTimestamp);
            for (int offset = entry.getValue().nextSetBit(0);
                 offset >= 0; offset = entry.getValue().nextSetBit(offset + 1))
            {
                int word = offset >>> 6;
                long bit = 1L << (offset & 63);
                if (word >= bitmap.length || (bitmap[word] & bit) == 0L)
                {
                    throw new IllegalStateException("Visibility validation failed for " + key
                            + ", offset=" + offset);
                }
                validated++;
                if (offset == Integer.MAX_VALUE)
                {
                    break;
                }
            }
        }
        if (validated != trackedSuccessful)
        {
            throw new IllegalStateException("validated " + validated + " deletes, expected "
                    + trackedSuccessful);
        }
        this.lastValidatedDeletes = validated;
    }

    private void cleanupAllRowGroups() throws Exception
    {
        if (resourceManager == null || rowGroups == null)
        {
            return;
        }
        Exception first = null;
        for (RowGroupSpec spec : rowGroups)
        {
            try
            {
                resourceManager.reclaimVisibility(spec.key.fileId, spec.key.rgId, 0L);
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

    private PreparedDelete[] operations(BenchmarkPhase phase)
    {
        return phase == BenchmarkPhase.WARMUP ? warmupDeletes : measurementDeletes;
    }

    private PhaseExecution requireActivePhase(BenchmarkPhase phase)
    {
        PhaseExecution execution = activePhase;
        if (execution == null || execution.phase != phase)
        {
            throw new IllegalStateException("no active " + phase + " Visibility phase");
        }
        return execution;
    }

    private void ensureSetUp()
    {
        if (config == null || resourceManager == null)
        {
            throw new IllegalStateException("snapshot visibility scenario has not been set up");
        }
        if (closed.get())
        {
            throw new IllegalStateException("snapshot visibility scenario is closed");
        }
    }

    private static PreparedDelete preparedDelete(RowGroupSpec spec, int offset)
    {
        IndexProto.RowLocation location = IndexProto.RowLocation.newBuilder()
                .setFileId(spec.key.fileId)
                .setRgId(spec.key.rgId)
                .setRgRowOffset(offset)
                .build();
        return new PreparedDelete(spec.key, offset, location);
    }

    private static boolean isInitiallyDeleted(CheckpointState checkpoint, int offset)
    {
        if (checkpoint == null)
        {
            return false;
        }
        int word = offset >>> 6;
        return word < checkpoint.bitmap.length
                && (checkpoint.bitmap[word] & (1L << (offset & 63))) != 0L;
    }

    private static String includeScheme(String configured, String required)
    {
        if (configured == null || configured.trim().isEmpty())
        {
            return required;
        }
        for (String scheme : configured.split(","))
        {
            if (required.equalsIgnoreCase(scheme.trim()))
            {
                return configured;
            }
        }
        return configured + "," + required;
    }

    private static int positiveOrDefault(String configured, int defaultValue)
    {
        if (configured == null || configured.trim().isEmpty())
        {
            return defaultValue;
        }
        int parsed = Integer.parseInt(configured);
        return parsed > 0 ? parsed : defaultValue;
    }

    private static int initialHashCapacity(int elements)
    {
        if (elements < 3)
        {
            return 4;
        }
        return elements >= (1 << 29) ? Integer.MAX_VALUE : (int) (elements / 0.75F) + 1;
    }

    private final class SnapshotVisibilityWorker implements BenchmarkWorker
    {
        private final PhaseExecution execution;
        private final int workerId;
        private final int clientId;
        private final OperationRange range;
        private final AtomicBoolean workerClosed = new AtomicBoolean(false);
        private final BitSet failures;
        private boolean prepared;
        private long nextOperation;
        private int attempted;

        private SnapshotVisibilityWorker(PhaseExecution execution, int workerId, int clientId,
                                         OperationRange range)
        {
            this.execution = execution;
            this.workerId = workerId;
            this.clientId = clientId;
            this.range = range;
            this.failures = new BitSet(Math.toIntExact(range.size()));
        }

        @Override
        public void prepare(long firstOperation, long operationCount, int batchSize)
        {
            if (prepared)
            {
                throw new IllegalStateException("snapshot visibility worker was prepared twice");
            }
            if (firstOperation != range.startInclusive() || operationCount != range.size()
                    || batchSize <= 0)
            {
                throw new IllegalArgumentException("unexpected snapshot visibility worker range");
            }
            this.nextOperation = firstOperation;
            this.prepared = true;
        }

        @Override
        public OperationResult execute(long firstOperation, int logicalOperationCount)
        {
            if (!prepared || workerClosed.get())
            {
                throw new IllegalStateException("snapshot visibility worker is not runnable");
            }
            if (firstOperation != nextOperation || logicalOperationCount < 0
                    || firstOperation + logicalOperationCount > range.endExclusive())
            {
                throw new IllegalArgumentException("snapshot visibility operations must execute once in order");
            }

            long successful = 0L;
            long errors = 0L;
            int batchStart = Math.toIntExact(firstOperation);
            int relativeStart = Math.toIntExact(firstOperation - range.startInclusive());
            for (int i = 0; i < logicalOperationCount; i++)
            {
                PreparedDelete delete = execution.operations[batchStart + i];
                try
                {
                    /* The entire measured target: real local update/JNI call. */
                    resourceManager.deleteRecord(delete.location, deleteTimestamp);
                    successful++;
                }
                catch (Exception e)
                {
                    failures.set(relativeStart + i);
                    firstDeleteError.compareAndSet(null, e.getClass().getName() + ": "
                            + String.valueOf(e.getMessage()));
                    errors++;
                }
            }
            attempted += logicalOperationCount;
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
        public void close()
        {
            workerClosed.set(true);
        }
    }

    private static final class PhaseExecution
    {
        private final BenchmarkPhase phase;
        private final PreparedDelete[] operations;
        private final List<SnapshotVisibilityWorker> workers = new ArrayList<>();
        private final Set<RowGroupKey> restoredFromCheckpoint = ConcurrentHashMap.newKeySet();
        private int cleanRestores;

        private PhaseExecution(BenchmarkPhase phase, PreparedDelete[] operations)
        {
            this.phase = phase;
            this.operations = operations;
        }
    }

    private static final class RowGroupSpec
    {
        private final RowGroupKey key;
        private final int recordNum;

        private RowGroupSpec(RowGroupKey key, int recordNum)
        {
            this.key = key;
            this.recordNum = recordNum;
        }
    }

    private static final class LayoutPathKey
    {
        private final long layoutId;
        private final long pathId;

        private LayoutPathKey(long layoutId, long pathId)
        {
            this.layoutId = layoutId;
            this.pathId = pathId;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other) return true;
            if (!(other instanceof LayoutPathKey)) return false;
            LayoutPathKey that = (LayoutPathKey) other;
            return layoutId == that.layoutId && pathId == that.pathId;
        }

        @Override
        public int hashCode()
        {
            return 31 * Long.hashCode(layoutId) + Long.hashCode(pathId);
        }

        @Override
        public String toString()
        {
            return "layoutId=" + layoutId + ",pathId=" + pathId;
        }
    }

    private static final class RowGroupKey
    {
        private final long fileId;
        private final int rgId;

        private RowGroupKey(long fileId, int rgId)
        {
            this.fileId = fileId;
            this.rgId = rgId;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other) return true;
            if (!(other instanceof RowGroupKey)) return false;
            RowGroupKey that = (RowGroupKey) other;
            return fileId == that.fileId && rgId == that.rgId;
        }

        @Override
        public int hashCode()
        {
            return 31 * Long.hashCode(fileId) + rgId;
        }

        @Override
        public String toString()
        {
            return "fileId=" + fileId + ",rgId=" + rgId;
        }
    }

    private static final class LocationKey
    {
        private final long fileId;
        private final int rgId;
        private final int offset;

        private LocationKey(long fileId, int rgId, int offset)
        {
            this.fileId = fileId;
            this.rgId = rgId;
            this.offset = offset;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other) return true;
            if (!(other instanceof LocationKey)) return false;
            LocationKey that = (LocationKey) other;
            return fileId == that.fileId && rgId == that.rgId && offset == that.offset;
        }

        @Override
        public int hashCode()
        {
            int result = Long.hashCode(fileId);
            result = 31 * result + rgId;
            return 31 * result + offset;
        }
    }

    private static final class PreparedDelete
    {
        private final RowGroupKey rowGroup;
        private final int offset;
        private final IndexProto.RowLocation location;

        private PreparedDelete(RowGroupKey rowGroup, int offset,
                               IndexProto.RowLocation location)
        {
            this.rowGroup = rowGroup;
            this.offset = offset;
            this.location = location;
        }
    }

    private static final class CheckpointState
    {
        private final int recordNum;
        private final long[] bitmap;

        private CheckpointState(int recordNum, long[] bitmap)
        {
            this.recordNum = recordNum;
            this.bitmap = bitmap;
        }
    }

    private static final class CheckpointScan
    {
        private final int totalEntries;
        private final Map<RowGroupKey, CheckpointState> entries;

        private CheckpointScan(int totalEntries, Map<RowGroupKey, CheckpointState> entries)
        {
            this.totalEntries = totalEntries;
            this.entries = entries;
        }
    }
}
