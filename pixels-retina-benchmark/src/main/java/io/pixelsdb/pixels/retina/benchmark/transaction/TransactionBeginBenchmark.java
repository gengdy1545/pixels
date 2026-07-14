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
package io.pixelsdb.pixels.retina.benchmark.transaction;

import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkConfig;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkPhase;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkResult;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkScenario;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkWorker;
import io.pixelsdb.pixels.retina.benchmark.common.OperationRange;
import io.pixelsdb.pixels.retina.benchmark.common.OperationResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Throughput benchmark for production write-transaction begin RPCs.
 *
 * <p>Actual path under test (no local transaction-service shortcut):</p>
 * <pre>
 * TransService.beginTrans(false) or beginTransBatch(n, false)
 *   -&gt; generated blocking gRPC stub
 *   -&gt; transaction.proto BeginTrans/BeginTransBatch
 *   -&gt; TransServiceImpl
 *   -&gt; PersistentAutoIncrement + TransContextManager
 * </pre>
 * Source: {@code pixels-common/.../transaction/TransService.java},
 * {@code proto/transaction.proto}, and
 * {@code pixels-daemon/.../transaction/TransServiceImpl.java}.
 *
 * <p>The server must keep every successful begin pending, so the worker retains
 * the returned real {@link TransContext}s.  {@link #completePhase} commits them
 * with the production batch RPC after timing has stopped.  Thus cleanup is not
 * counted as begin throughput.  An ambiguous transport failure can allocate a
 * server context without returning its id; such a context cannot be safely
 * discovered by this client and is left for the server lease cleanup.</p>
 */
public final class TransactionBeginBenchmark implements BenchmarkScenario
{
    private final Map<BenchmarkPhase, List<BeginWorker>> phaseWorkers =
            new EnumMap<>(BenchmarkPhase.class);

    private TransactionRpcClients clients;
    private int cleanupBatchSize;
    private String host;
    private int port;
    private int clientCount;
    private long rpcDeadlineMs;
    private volatile long warmupContextsCleaned;
    private volatile long measurementContextsCleaned;

    @Override
    public String name()
    {
        return "transaction-begin";
    }

    @Override
    public void setup(BenchmarkConfig config)
    {
        TransactionEndpoint endpoint = TransactionEndpoint.from(config);
        host = endpoint.host();
        port = endpoint.port();
        clientCount = config.clients();
        rpcDeadlineMs = config.getLong("rpc-deadline-ms", 30_000L);
        if (rpcDeadlineMs <= 0)
        {
            throw new IllegalArgumentException("rpc-deadline-ms must be positive");
        }
        cleanupBatchSize = config.getInt("trans-cleanup-batch-size",
                Math.max(config.batchSize(), 256));
        if (cleanupBatchSize <= 0)
        {
            throw new IllegalArgumentException("trans-cleanup-batch-size must be positive");
        }
        clients = new TransactionRpcClients(host, port, clientCount, rpcDeadlineMs);
        for (BenchmarkPhase phase : BenchmarkPhase.values())
        {
            phaseWorkers.put(phase, Collections.synchronizedList(new ArrayList<BeginWorker>()));
        }
    }

    @Override
    public void preparePhase(BenchmarkPhase phase, long totalOperations)
    {
        phaseWorkers.get(phase).clear();
    }

    @Override
    public BenchmarkWorker createWorker(BenchmarkPhase phase, int workerId, int clientId,
                                        OperationRange range)
    {
        BeginWorker worker = new BeginWorker(clientId);
        phaseWorkers.get(phase).add(worker);
        return worker;
    }

    @Override
    public void completePhase(BenchmarkPhase phase, BenchmarkResult result)
    {
        long cleaned = 0;
        RuntimeException failure = null;
        List<BeginWorker> workers = phaseWorkers.get(phase);
        synchronized (workers)
        {
            for (BeginWorker worker : workers)
            {
                try
                {
                    cleaned += clients.commitContexts(worker.clientId,
                            worker.completedBatches, cleanupBatchSize);
                    worker.completedBatches.clear();
                }
                catch (Exception e)
                {
                    if (failure == null)
                    {
                        failure = new IllegalStateException(
                                "failed to clean up begun transactions after " + phase, e);
                    }
                    else
                    {
                        failure.addSuppressed(e);
                    }
                }
            }
        }
        if (phase == BenchmarkPhase.WARMUP)
        {
            warmupContextsCleaned = cleaned;
        }
        else
        {
            measurementContextsCleaned = cleaned;
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
        details.put("call_type", "RPC");
        details.put("transaction_mode", "write (readOnly=false)");
        details.put("rpc_mode", "batch-size=1: BeginTrans; batch-size>1: BeginTransBatch");
        details.put("trans_endpoint", host + ":" + port);
        details.put("physical_clients", Integer.toString(clientCount));
        details.put("rpc_deadline_ms", Long.toString(rpcDeadlineMs));
        details.put("server_dependency", "TransServer + etcd");
        details.put("state_growth", "successful begins stay pending until out-of-band phase cleanup");
        details.put("warmup_contexts_cleaned", Long.toString(warmupContextsCleaned));
        details.put("measurement_contexts_cleaned", Long.toString(measurementContextsCleaned));
        details.put("ambiguous_rpc_failure", "server context may remain until its lease expires");
        return details;
    }

    @Override
    public void close()
    {
        if (clients != null)
        {
            clients.close();
        }
    }

    private final class BeginWorker implements BenchmarkWorker
    {
        private final int clientId;
        private List<List<TransContext>> completedBatches;

        private BeginWorker(int clientId)
        {
            this.clientId = clientId;
        }

        @Override
        public void prepare(long firstOperation, long operationCount, int batchSize)
        {
            long expectedCalls = operationCount / batchSize
                    + (operationCount % batchSize == 0 ? 0 : 1);
            int capacity = expectedCalls > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) expectedCalls;
            completedBatches = new ArrayList<>(capacity);
        }

        @Override
        public OperationResult execute(long firstOperation, int logicalOperationCount) throws Exception
        {
            List<TransContext> contexts =
                    clients.beginWriteTransactions(clientId, logicalOperationCount);
            /*
             * Keep only the already-created production result list.  This one
             * reference append is the minimum bookkeeping needed to remove real
             * pending server contexts after the measured interval.
             */
            completedBatches.add(contexts);
            return OperationResult.success(logicalOperationCount, 1);
        }
    }
}
