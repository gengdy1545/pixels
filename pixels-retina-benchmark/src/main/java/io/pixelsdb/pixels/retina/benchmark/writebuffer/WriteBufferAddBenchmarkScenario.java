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
package io.pixelsdb.pixels.retina.benchmark.writebuffer;

import io.pixelsdb.pixels.common.index.MainIndexFactory;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.retina.PixelsWriteBuffer;
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
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotManifest;
import io.pixelsdb.pixels.retina.benchmark.snapshot.SnapshotRuntime;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Isolated throughput benchmark for the local WriteBuffer Add operation.
 *
 * <p>The measured call is the exact local boundary used by a Retina update:</p>
 * <pre>
 * RetinaServerImpl.processUpdateRequest
 *   -&gt; RetinaResourceManager.insertRecord
 *   -&gt; PixelsWriteBuffer.addRow                         [measured]
 *      -&gt; MemTable.add
 *      -&gt; RowIdAllocator.getRowId
 *      -&gt; switchMemTable / asynchronous flush as needed
 *      -&gt; populate RowLocation
 * </pre>
 *
 * <p>Calling {@code MemTable.add} directly would omit row-id allocation,
 * {@code RowLocation}, the per-buffer {@code rowLock}, memtable switching and
 * real flush behavior.  This scenario therefore constructs genuine
 * {@link PixelsWriteBuffer} instances.  Their required file-registration
 * calls go through the production MetadataService RPC and their row-id ranges
 * go through LocalIndexService/SQLite/PersistentAutoIncrement/etcd.  Setup and
 * final drain are outside the timed interval; periodic work caused by a full
 * buffer remains inside it.</p>
 */
public final class WriteBufferAddBenchmarkScenario implements BenchmarkScenario
{
    /** TileVisibility stores transaction timestamps in 48 bits. */
    private static final long MAX_NATIVE_TIMESTAMP = (1L << 48) - 1L;
    private static final int DEFAULT_ROW_POOL_SIZE = 100_000;

    private final Map<BenchmarkPhase, PhaseState> states = new EnumMap<>(BenchmarkPhase.class);
    private final Map<BenchmarkPhase, Long> closeMillis = new EnumMap<>(BenchmarkPhase.class);
    private final AtomicReference<String> firstWorkloadError = new AtomicReference<>();
    private final AtomicLong checksum = new AtomicLong();
    private final AtomicLong seededRows = new AtomicLong();

    private BenchmarkConfig config;
    private MetadataService metadataService;
    private byte[][][] rowPool;
    private String schemaName;
    private String baseUri;
    private String retinaHostName;
    private boolean dropSchema;
    private List<String> columnNames;
    private List<String> columnTypes;
    private Storage.Scheme storageScheme;
    private SnapshotRuntime snapshotRuntime;
    private SnapshotManifest.TableState snapshotTable;
    private boolean seedBuffers;
    private boolean skipMeasurementClose;
    private boolean measurementCloseSkipped;
    private long snapshotTimestamp;
    private int nativeTileCapacity;

    @Override
    public String name()
    {
        return "write-buffer-add";
    }

    @Override
    public void setup(BenchmarkConfig benchmarkConfig) throws Exception
    {
        this.config = benchmarkConfig;
        if (benchmarkConfig.threads() < benchmarkConfig.clients())
        {
            throw new IllegalArgumentException("write-buffer benchmark requires threads >= clients; "
                    + "each client is one real PixelsWriteBuffer/vnode");
        }

        ConfigFactory pixelsConfig = ConfigFactory.Instance();
        this.snapshotRuntime = SnapshotRuntime.open(benchmarkConfig);
        this.snapshotTable = selectSnapshotTable(snapshotRuntime, benchmarkConfig);
        this.snapshotRuntime.prepareFreshWriteBufferState();
        this.snapshotTimestamp = snapshotRuntime.manifest().snapshotTimestamp;
        if (snapshotTimestamp < 0 || snapshotTimestamp >= MAX_NATIVE_TIMESTAMP)
        {
            throw new IllegalArgumentException("snapshotTimestamp must be in [0,"
                    + (MAX_NATIVE_TIMESTAMP - 1L) + "] for WriteBuffer benchmarking: "
                    + snapshotTimestamp);
        }
        List<String> names = new ArrayList<>(snapshotTable.columns.size());
        List<String> types = new ArrayList<>(snapshotTable.columns.size());
        for (SnapshotManifest.ColumnState column : snapshotTable.columns)
        {
            names.add(column.name);
            types.add(column.type);
        }
        this.columnNames = Collections.unmodifiableList(names);
        this.columnTypes = Collections.unmodifiableList(types);
        this.schemaName = option(benchmarkConfig, "writebuffer-schema",
                "retina_bench_" + UUID.randomUUID().toString().replace("-", ""));
        this.storageScheme = Storage.Scheme.from(option(benchmarkConfig,
                "writebuffer-storage-scheme", snapshotTable.storageScheme));

        applyOptionalPositiveInt(pixelsConfig, benchmarkConfig,
                "writebuffer-memtable-size", "retina.buffer.memTable.size", true);
        applyOptionalPositiveInt(pixelsConfig, benchmarkConfig,
                "writebuffer-flush-count", "retina.buffer.flush.count", false);
        applyOptionalPositiveInt(pixelsConfig, benchmarkConfig,
                "writebuffer-flush-threads", "retina.buffer.object.flush.threads", false);
        applyOptionalPositiveInt(pixelsConfig, benchmarkConfig,
                "writebuffer-flush-interval", "retina.buffer.flush.interval", false);
        applyOptionalNonNegativeInt(pixelsConfig, benchmarkConfig,
                "writebuffer-encoding-level", "retina.buffer.flush.encodingLevel");
        applyOptionalBoolean(pixelsConfig, benchmarkConfig,
                "writebuffer-nulls-padding", "retina.buffer.flush.nullsPadding");
        applyOptionalPositiveLong(pixelsConfig, benchmarkConfig,
                "writebuffer-block-size", "block.size");
        applyOptionalPositiveShort(pixelsConfig, benchmarkConfig,
                "writebuffer-replication", "block.replication");
        applyOptionalPositiveInt(pixelsConfig, benchmarkConfig,
                "writebuffer-vnodes", "node.virtual.num", false);
        String enabledStorage = option(benchmarkConfig, "writebuffer-enabled-storage-schemes", null);
        if (enabledStorage != null)
        {
            pixelsConfig.addProperty("enabled.storage.schemes", enabledStorage);
        }

        String objectFolder = option(benchmarkConfig, "writebuffer-object-folder", null);
        if (objectFolder == null)
        {
            if (storageScheme != Storage.Scheme.file)
            {
                throw new IllegalArgumentException("snapshot WriteBuffer with non-file storage requires a "
                        + "dedicated --writebuffer-object-folder; the source production folder is never reused");
            }
            objectFolder = snapshotRuntime.workDirectory().resolve("write-buffer/objects").toString();
        }
        if (objectFolder == null && storageScheme != Storage.Scheme.file)
        {
            throw new IllegalArgumentException("non-file WriteBuffer storage requires an explicit "
                    + "--writebuffer-object-folder dedicated to the benchmark");
        }
        if (objectFolder != null)
        {
            pixelsConfig.addProperty("retina.buffer.object.storage.folder", objectFolder);
        }
        String objectScheme = option(benchmarkConfig, "writebuffer-object-storage-scheme", null);
        if (objectScheme != null)
        {
            pixelsConfig.addProperty("retina.buffer.object.storage.scheme", objectScheme);
        }
        else if (storageScheme == Storage.Scheme.file)
        {
            pixelsConfig.addProperty("retina.buffer.object.storage.scheme", storageScheme.name());
        }
        String configuredObjectScheme = pixelsConfig.getProperty(
                "retina.buffer.object.storage.scheme");
        if (configuredObjectScheme == null || configuredObjectScheme.trim().isEmpty())
        {
            throw new IllegalArgumentException("retina.buffer.object.storage.scheme is not configured");
        }
        Storage.Scheme objectStorageScheme = Storage.Scheme.from(configuredObjectScheme);
        ensureStorageSchemeEnabled(pixelsConfig, objectStorageScheme);
        pixelsConfig.addProperty("retina.gc.interval", "-1");
        pixelsConfig.addProperty("retina.storage.gc.enabled", "false");

        int configuredVnodes = Integer.parseInt(pixelsConfig.getProperty("node.virtual.num"));
        if (benchmarkConfig.clients() > configuredVnodes)
        {
            throw new IllegalArgumentException("clients=" + benchmarkConfig.clients()
                    + " exceeds node.virtual.num=" + configuredVnodes);
        }
        long memTableSize = Long.parseLong(pixelsConfig.getProperty("retina.buffer.memTable.size"));
        long memTablesPerFile = Long.parseLong(pixelsConfig.getProperty("retina.buffer.flush.count"));
        if (memTableSize * memTablesPerFile > Integer.MAX_VALUE)
        {
            throw new IllegalArgumentException("retina.buffer.memTable.size * flush.count exceeds "
                    + "the int recordNum used by FileWriterManager/RGVisibility");
        }

        String defaultBaseUri = null;
        if (storageScheme == Storage.Scheme.file)
        {
            defaultBaseUri = snapshotRuntime.workDirectory()
                    .resolve("write-buffer/data").toUri().toString();
        }
        this.baseUri = option(benchmarkConfig, "writebuffer-base-uri", defaultBaseUri);
        if (baseUri == null)
        {
            throw new IllegalArgumentException("--writebuffer-base-uri is required for non-file storage; "
                    + "use a dedicated writable benchmark prefix, never a source snapshot path");
        }
        rejectProductionDestination("writebuffer-base-uri", baseUri,
                snapshotTable.productionOrderedPathUri,
                snapshotTable.productionCompactPathUri);
        this.retinaHostName = option(benchmarkConfig, "writebuffer-host-name", "benchmark");
        this.dropSchema = Boolean.parseBoolean(option(benchmarkConfig,
                "writebuffer-drop-schema", "false"));
        Storage.Scheme uriScheme = Storage.Scheme.fromPath(baseUri);
        if (uriScheme == null || uriScheme != storageScheme)
        {
            throw new IllegalArgumentException("writebuffer-base-uri scheme must match --writebuffer-storage-scheme="
                    + storageScheme);
        }
        ensureStorageSchemeEnabled(pixelsConfig, storageScheme);
        this.seedBuffers = benchmarkConfig.getBoolean("writebuffer-seed-buffers", true);
        this.skipMeasurementClose = benchmarkConfig.getBoolean(
                "writebuffer-skip-measurement-close", false);

        int requestedPoolSize = positiveIntOption(benchmarkConfig,
                "writebuffer-row-pool-size", DEFAULT_ROW_POOL_SIZE);
        long boundedPoolSize = Math.min(benchmarkConfig.dataSize(), requestedPoolSize);
        if (boundedPoolSize <= 0 || boundedPoolSize > Integer.MAX_VALUE)
        {
            throw new IllegalArgumentException("invalid write-buffer row pool size: " + boundedPoolSize);
        }
        this.rowPool = buildSnapshotRows((int) boundedPoolSize, snapshotTable);

        /* Must happen before RetinaResourceManager/RGVisibility initialization. */
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
        int configuredTileCapacity = Integer.parseInt(
                pixelsConfig.getProperty("retina.tile.visibility.capacity"));
        if (configuredTileCapacity != nativeTileCapacity)
        {
            throw new IllegalStateException("retina.tile.visibility.capacity="
                    + configuredTileCapacity + " but loaded libpixels-retina.so RETINA_CAPACITY="
                    + nativeTileCapacity + "; rebuild the fat JAR with the snapshot capacity");
        }
        this.metadataService = MetadataService.Instance();
        if (!metadataService.existSchema(schemaName))
        {
            metadataService.createSchema(schemaName);
        }
    }

    @Override
    public synchronized void preparePhase(BenchmarkPhase phase, long totalOperations) throws Exception
    {
        if (states.containsKey(phase))
        {
            throw new IllegalStateException("write-buffer phase already exists: " + phase);
        }

        String tableName = phase == BenchmarkPhase.WARMUP ? "write_buffer_warmup" : "write_buffer_measurement";
        tableName += "_" + Long.toUnsignedString(System.nanoTime());
        String tableBaseUri = stripTrailingSlash(baseUri) + "/" + tableName;
        metadataService.createTable(schemaName, tableName, storageScheme,
                Collections.singletonList(tableBaseUri), buildColumns());

        Layout layout = metadataService.getLatestLayout(schemaName, tableName);
        io.pixelsdb.pixels.common.metadata.domain.Path ordered = layout.getOrderedPaths().get(0);
        io.pixelsdb.pixels.common.metadata.domain.Path compact = layout.getCompactPaths().get(0);
        if (storageScheme == Storage.Scheme.file)
        {
            createFileDirectory(ordered.getUri());
            createFileDirectory(compact.getUri());
        }

        TypeDescription schema = TypeDescription.createSchemaFromStrings(columnNames, columnTypes);
        PixelsWriteBuffer[] buffers = new PixelsWriteBuffer[config.clients()];
        int created = 0;
        try
        {
            for (; created < buffers.length; created++)
            {
                buffers[created] = new PixelsWriteBuffer(layout.getTableId(), schema,
                        ordered, compact, retinaHostName, created);
            }
        }
        catch (Exception e)
        {
            closePrefix(buffers, created);
            throw e;
        }
        long timestampFloor = Math.addExact(snapshotTimestamp,
                Math.addExact((long) config.clients(), 1L));
        long phaseSpan = Math.max(1L,
                Math.addExact(config.dataSize(), (long) config.clients()));
        long timestampBase = Math.addExact(timestampFloor,
                Math.multiplyExact((long) phase.ordinal(), phaseSpan));
        long finalTimestamp = Math.addExact(timestampBase,
                Math.addExact(totalOperations, 1L));
        if (finalTimestamp > MAX_NATIVE_TIMESTAMP)
        {
            throw new IllegalArgumentException("WriteBuffer timestamps exceed Retina's native "
                    + "48-bit representation: " + finalTimestamp);
        }
        PhaseState state = new PhaseState(layout.getTableId(), tableName, buffers,
                ordered.getId(), timestampBase);
        if (seedBuffers)
        {
            for (int client = 0; client < buffers.length; client++)
            {
                IndexProto.RowLocation.Builder seedLocation = IndexProto.RowLocation.newBuilder();
                buffers[client].addRow(rowPool[client % rowPool.length],
                        timestampBase - buffers.length + client, seedLocation);
                state.fileIds.add(seedLocation.getFileId());
                seededRows.incrementAndGet();
            }
        }
        /* FileWriterManager creates a file and native visibility in each
           PixelsWriteBuffer constructor. Record those even if a client never
           reaches a successful timed addRow. */
        recordPathFileIds(state);
        states.put(phase, state);
    }

    @Override
    public BenchmarkWorker createWorker(BenchmarkPhase phase, int workerId, int clientId,
                                        OperationRange range)
    {
        PhaseState state = states.get(phase);
        if (state == null)
        {
            throw new IllegalStateException("write-buffer phase is not prepared: " + phase);
        }
        return new AddWorker(state, state.buffers[clientId]);
    }

    @Override
    public synchronized void completePhase(BenchmarkPhase phase, BenchmarkResult result) throws Exception
    {
        PhaseState state = states.remove(phase);
        if (state == null)
        {
            return;
        }
        if (phase == BenchmarkPhase.MEASUREMENT && skipMeasurementClose)
        {
            measurementCloseSkipped = true;
            closeMillis.put(phase, 0L);
            return;
        }
        long start = System.nanoTime();
        Exception first = null;
        for (PixelsWriteBuffer buffer : state.buffers)
        {
            try
            {
                buffer.close();
            }
            catch (Exception e)
            {
                if (first == null)
                {
                    first = e;
                }
                else
                {
                    first.addSuppressed(e);
                }
            }
        }
        closeMillis.put(phase, (System.nanoTime() - start) / 1_000_000L);

        try
        {
            /* Also capture files created by a memtable switch whose following
               addRow did not complete successfully. */
            recordPathFileIds(state);
        }
        catch (Exception e)
        {
            if (first == null)
            {
                first = e;
            }
            else
            {
                first.addSuppressed(e);
            }
        }

        /* Phase-local JNI state and SQLite row-id state are no longer used.
           Keep the shared SQLite directory for the following phase; the
           snapshot work directory is removed when the scenario closes. */
        for (Long fileId : state.fileIds)
        {
            try
            {
                RetinaResourceManager.Instance().reclaimVisibility(fileId, 0, 0L);
            }
            catch (Exception e)
            {
                if (first == null)
                {
                    first = e;
                }
                else
                {
                    first.addSuppressed(e);
                }
            }
        }
        try
        {
            MainIndexFactory.Instance().closeIndex(state.tableId, false);
        }
        catch (Exception e)
        {
            if (first == null)
            {
                first = e;
            }
            else
            {
                first.addSuppressed(e);
            }
        }
        if (first != null)
        {
            throw first;
        }
    }

    @Override
    public Map<String, String> details()
    {
        Map<String, String> details = new LinkedHashMap<>();
        details.put("call_type", "LOCAL");
        details.put("measured_call", "PixelsWriteBuffer.addRow");
        details.put("nested_dependencies", "MetadataService RPC + local SQLite MainIndex + etcd + "
                + (storageScheme == null ? "configured storage" : storageScheme.name()) + " + JNI visibility");
        details.put("schema", schemaDescription());
        details.put("row_pool_size", rowPool == null ? "0" : Integer.toString(rowPool.length));
        details.put("row_pool_reuse", "values may repeat; each row gets a fresh rowId/location and each driver group gets a fresh timestamp");
        details.put("clients", "one real PixelsWriteBuffer/vnode per client");
        details.put("batch_semantics", "driver timestamp group; addRow remains one scalar local API call per row");
        details.put("memtable_size", ConfigFactory.Instance().getProperty("retina.buffer.memTable.size"));
        details.put("memtables_per_file", ConfigFactory.Instance().getProperty("retina.buffer.flush.count"));
        details.put("object_flush_threads", ConfigFactory.Instance().getProperty("retina.buffer.object.flush.threads"));
        details.put("flush_interval_seconds", ConfigFactory.Instance().getProperty("retina.buffer.flush.interval"));
        details.put("encoding_level", ConfigFactory.Instance().getProperty("retina.buffer.flush.encodingLevel"));
        details.put("nulls_padding", ConfigFactory.Instance().getProperty("retina.buffer.flush.nullsPadding"));
        details.put("block_size", ConfigFactory.Instance().getProperty("block.size"));
        details.put("block_replication", ConfigFactory.Instance().getProperty("block.replication"));
        details.put("object_storage_scheme", ConfigFactory.Instance().getProperty("retina.buffer.object.storage.scheme"));
        details.put("object_storage_folder", ConfigFactory.Instance().getProperty("retina.buffer.object.storage.folder"));
        details.put("node_virtual_num", ConfigFactory.Instance().getProperty("node.virtual.num"));
        details.put("native_compiled_tile_capacity", Integer.toString(nativeTileCapacity));
        details.put("storage", storageScheme == null ? "unknown" : storageScheme.name());
        details.put("snapshot_mode", "true");
        details.put("snapshot_table", snapshotTable == null ? "none"
                : snapshotTable.schemaName + "." + snapshotTable.tableName);
        details.put("snapshot_initialization",
                "source schema + deterministic type-valid rows + semantic WriteBuffer/Pixels config; fresh buffer state");
        details.put("seed_buffers", Boolean.toString(seedBuffers));
        details.put("seed_timing", "one real addRow per buffer outside timed phase when enabled");
        details.put("seed_rows", Long.toString(seededRows.get()));
        details.put("warmup_close_ms", Long.toString(value(closeMillis, BenchmarkPhase.WARMUP)));
        details.put("measurement_close_ms", Long.toString(value(closeMillis, BenchmarkPhase.MEASUREMENT)));
        details.put("measurement_close_status", measurementCloseSkipped ? "skipped" : "completed");
        details.put("persistence_validated", Boolean.toString(!measurementCloseSkipped));
        details.put("close_timing", measurementCloseSkipped
                ? "measurement drain/persist skipped; process exits after printing acceptance throughput"
                : "drain/persist is outside measured throughput");
        details.put("checksum", Long.toUnsignedString(checksum.get()));
        details.put("first_workload_error", firstWorkloadError.get() == null ? "none" : firstWorkloadError.get());
        return details;
    }

    @Override
    public synchronized void close() throws Exception
    {
        Exception first = null;
        for (BenchmarkPhase phase : new ArrayList<>(states.keySet()))
        {
            try
            {
                completePhase(phase, null);
            }
            catch (Exception e)
            {
                if (first == null)
                {
                    first = e;
                }
            }
        }
        if (!measurementCloseSkipped && dropSchema && metadataService != null && schemaName != null)
        {
            try
            {
                if (metadataService.existSchema(schemaName))
                {
                    metadataService.dropSchema(schemaName);
                }
            }
            catch (Exception e)
            {
                if (first == null)
                {
                    first = e;
                }
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

    private final class AddWorker implements BenchmarkWorker
    {
        private final PhaseState state;
        private final PixelsWriteBuffer buffer;
        private final IndexProto.RowLocation.Builder location = IndexProto.RowLocation.newBuilder();
        private long localChecksum;
        private long lastFileId = -1;

        private AddWorker(PhaseState state, PixelsWriteBuffer buffer)
        {
            this.state = state;
            this.buffer = buffer;
        }

        @Override
        public void prepare(long firstOperation, long operationCount, int batchSize)
        {
            // Rows and builders are already prepared; the harness supplies one
            // transaction-timestamp group per execute invocation.
        }

        @Override
        public OperationResult execute(long firstOperation, int logicalOperationCount)
        {
            long successful = 0;
            long errors = 0;
            long timestamp = Math.addExact(state.timestampBase,
                    Math.addExact(firstOperation, 1L));
            for (int i = 0; i < logicalOperationCount; i++)
            {
                long operation = firstOperation + i;
                location.clear();
                try
                {
                    long rowId = buffer.addRow(rowPool[(int) (operation % rowPool.length)],
                            timestamp, location);
                    if (rowId < 0 || location.getFileId() <= 0 || location.getRgId() != 0
                            || location.getRgRowOffset() < 0)
                    {
                        errors++;
                        continue;
                    }
                    successful++;
                    localChecksum ^= rowId ^ location.getFileId() ^ location.getRgRowOffset();
                    if (location.getFileId() != lastFileId)
                    {
                        lastFileId = location.getFileId();
                        state.fileIds.add(lastFileId);
                    }
                }
                catch (Exception e)
                {
                    errors++;
                    firstWorkloadError.compareAndSet(null,
                            e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage()));
                }
            }
            return new OperationResult(logicalOperationCount, successful, errors, logicalOperationCount);
        }

        @Override
        public long expectedApiCalls(int logicalOperationCount)
        {
            return logicalOperationCount;
        }

        @Override
        public void close()
        {
            checksum.getAndAccumulate(localChecksum, (left, right) -> left ^ right);
        }
    }

    private static final class PhaseState
    {
        private final long tableId;
        private final String tableName;
        private final PixelsWriteBuffer[] buffers;
        private final long orderedPathId;
        private final long timestampBase;
        private final Set<Long> fileIds = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
        private PhaseState(long tableId, String tableName, PixelsWriteBuffer[] buffers,
                           long orderedPathId, long timestampBase)
        {
            this.tableId = tableId;
            this.tableName = tableName;
            this.buffers = buffers;
            this.orderedPathId = orderedPathId;
            this.timestampBase = timestampBase;
        }
    }

    private void recordPathFileIds(PhaseState state) throws Exception
    {
        for (File file : metadataService.getFiles(state.orderedPathId))
        {
            state.fileIds.add(file.getId());
        }
    }

    private List<Column> buildColumns()
    {
        List<Column> columns = new ArrayList<>(columnNames.size());
        for (int i = 0; i < columnNames.size(); i++)
        {
            Column column = new Column();
            column.setName(columnNames.get(i));
            column.setType(columnTypes.get(i));
            columns.add(column);
        }
        return columns;
    }

    private static byte[][][] buildSnapshotRows(int count,
                                                SnapshotManifest.TableState table)
    {
        List<TypeDescription> types = new ArrayList<>(table.columns.size());
        for (SnapshotManifest.ColumnState column : table.columns)
        {
            TypeDescription type = TypeDescription.fromString(column.type);
            switch (type.getCategory())
            {
                case STRUCT:
                case VECTOR:
                    throw new IllegalArgumentException("snapshot WriteBuffer row generation does not "
                            + "support " + type.getCategory() + " column "
                            + table.schemaName + "." + table.tableName + "." + column.name);
                default:
                    types.add(type);
            }
        }

        byte[][][] rows = new byte[count][table.columns.size()][];
        for (int row = 0; row < count; row++)
        {
            for (int column = 0; column < types.size(); column++)
            {
                rows[row][column] = deterministicValue(types.get(column), row, column);
            }
        }
        return rows;
    }

    private static byte[] deterministicValue(TypeDescription type, int row, int column)
    {
        long seed = (long) row * 131L + column;
        String value;
        switch (type.getCategory())
        {
            case BOOLEAN:
                value = (seed & 1L) == 0L ? "false" : "true";
                break;
            case BYTE:
                return new byte[]{(byte) (seed % Byte.MAX_VALUE)};
            case SHORT:
                value = Short.toString((short) (seed % Short.MAX_VALUE));
                break;
            case INT:
                value = Integer.toString((int) (seed % Integer.MAX_VALUE));
                break;
            case LONG:
                value = Long.toString(seed);
                break;
            case FLOAT:
            case DOUBLE:
                value = Long.toString(seed) + ".25";
                break;
            case DECIMAL:
                value = Long.toString(seed % 9L);
                break;
            case DATE:
                value = String.format("2000-01-%02d", (seed % 28L) + 1L);
                break;
            case TIME:
                value = String.format("00:00:%02d", seed % 60L);
                break;
            case TIMESTAMP:
                value = String.format("2000-01-01 00:00:%02d", seed % 60L);
                break;
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                value = boundedText("r" + Long.toString(seed, 36), type.getMaxLength());
                break;
            case STRING:
                value = "r" + Long.toString(seed, 36);
                break;
            default:
                throw new IllegalArgumentException("unsupported deterministic row type: "
                        + type.getCategory());
        }
        return type.convertSqlStringToByte(value);
    }

    private static String boundedText(String value, int maximumLength)
    {
        if (maximumLength <= 0)
        {
            throw new IllegalArgumentException("snapshot schema contains a non-positive text/binary length");
        }
        return value.length() <= maximumLength ? value : value.substring(0, maximumLength);
    }

    private static void createFileDirectory(String uri) throws Exception
    {
        Files.createDirectories(Paths.get(URI.create(uri)));
    }

    private static String stripTrailingSlash(String value)
    {
        String result = value;
        while (result.endsWith("/"))
        {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    private static String option(BenchmarkConfig config, String key, String defaultValue)
    {
        String value = config.options().get(key);
        return value == null || value.isEmpty() ? defaultValue : value;
    }

    private static int positiveIntOption(BenchmarkConfig config, String key, int defaultValue)
    {
        int value = Integer.parseInt(option(config, key, Integer.toString(defaultValue)));
        if (value <= 0)
        {
            throw new IllegalArgumentException("--" + key + " must be positive");
        }
        return value;
    }

    private static void applyOptionalPositiveInt(ConfigFactory pixelsConfig, BenchmarkConfig benchmarkConfig,
                                                 String option, String property, boolean multipleOf64)
    {
        String raw = benchmarkConfig.options().get(option);
        if (raw == null)
        {
            return;
        }
        int value = Integer.parseInt(raw);
        if (value <= 0 || (multipleOf64 && value % 64 != 0))
        {
            throw new IllegalArgumentException("--" + option + " must be positive"
                    + (multipleOf64 ? " and a multiple of 64" : ""));
        }
        pixelsConfig.addProperty(property, raw);
    }

    private static void ensureStorageSchemeEnabled(ConfigFactory config, Storage.Scheme scheme)
    {
        String enabled = config.getProperty("enabled.storage.schemes");
        if (enabled != null)
        {
            for (String value : enabled.split(","))
            {
                if (scheme.name().equalsIgnoreCase(value.trim()))
                {
                    return;
                }
            }
            config.addProperty("enabled.storage.schemes", scheme.name() + "," + enabled);
        }
        else
        {
            config.addProperty("enabled.storage.schemes", scheme.name());
        }
    }

    private static void rejectProductionDestination(String option, String candidate,
                                                    String... productionLocations)
    {
        if (candidate == null)
        {
            return;
        }
        String normalizedCandidate = normalizedLocation(candidate);
        for (String production : productionLocations)
        {
            if (production == null || production.trim().isEmpty())
            {
                continue;
            }
            String normalizedProduction = normalizedLocation(production);
            if (normalizedCandidate.equals(normalizedProduction)
                    || normalizedCandidate.startsWith(normalizedProduction + "/")
                    || normalizedProduction.startsWith(normalizedCandidate + "/"))
            {
                throw new IllegalArgumentException("--" + option
                        + " overlaps the source production location " + production
                        + "; use a dedicated benchmark prefix");
            }
        }
    }

    private static String normalizedLocation(String location)
    {
        String normalized = location.trim();
        while (normalized.endsWith("/") && normalized.length() > 1)
        {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private static void applyOptionalNonNegativeInt(ConfigFactory pixelsConfig,
                                                    BenchmarkConfig benchmarkConfig,
                                                    String option, String property)
    {
        String raw = benchmarkConfig.options().get(option);
        if (raw == null)
        {
            return;
        }
        int value = Integer.parseInt(raw);
        if (value < 0 || value > 2)
        {
            throw new IllegalArgumentException("--" + option + " must be 0, 1, or 2");
        }
        pixelsConfig.addProperty(property, raw);
    }

    private static void applyOptionalBoolean(ConfigFactory pixelsConfig, BenchmarkConfig benchmarkConfig,
                                             String option, String property)
    {
        if (!benchmarkConfig.options().containsKey(option))
        {
            return;
        }
        pixelsConfig.addProperty(property,
                Boolean.toString(benchmarkConfig.getBoolean(option, false)));
    }

    private static void applyOptionalPositiveLong(ConfigFactory pixelsConfig,
                                                  BenchmarkConfig benchmarkConfig,
                                                  String option, String property)
    {
        String raw = benchmarkConfig.options().get(option);
        if (raw == null)
        {
            return;
        }
        if (Long.parseLong(raw) <= 0)
        {
            throw new IllegalArgumentException("--" + option + " must be positive");
        }
        pixelsConfig.addProperty(property, raw);
    }

    private static void applyOptionalPositiveShort(ConfigFactory pixelsConfig,
                                                   BenchmarkConfig benchmarkConfig,
                                                   String option, String property)
    {
        String raw = benchmarkConfig.options().get(option);
        if (raw == null)
        {
            return;
        }
        int value = Integer.parseInt(raw);
        if (value <= 0 || value > Short.MAX_VALUE)
        {
            throw new IllegalArgumentException("--" + option + " must be in [1, "
                    + Short.MAX_VALUE + "]");
        }
        pixelsConfig.addProperty(property, raw);
    }

    private static void closePrefix(PixelsWriteBuffer[] buffers, int count)
    {
        for (int i = 0; i < count; i++)
        {
            try
            {
                buffers[i].close();
            }
            catch (Exception ignored)
            {
                // The construction exception is the primary failure.
            }
        }
    }

    private static long value(Map<BenchmarkPhase, Long> values, BenchmarkPhase phase)
    {
        Long value = values.get(phase);
        return value == null ? 0L : value;
    }

    private static SnapshotManifest.TableState selectSnapshotTable(SnapshotRuntime runtime,
                                                                    BenchmarkConfig config)
    {
        String requested = config.get("snapshot-table", null);
        if (requested != null)
        {
            return runtime.requireTable(requested);
        }
        if (runtime.manifest().tables.size() != 1)
        {
            throw new IllegalArgumentException("--snapshot-table is required when snapshot contains multiple tables");
        }
        return runtime.manifest().tables.get(0);
    }

    private String schemaDescription()
    {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < columnNames.size(); i++)
        {
            if (i > 0)
            {
                builder.append(", ");
            }
            builder.append(columnNames.get(i)).append(' ').append(columnTypes.get(i));
        }
        return builder.toString();
    }
}
