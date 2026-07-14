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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Throughput benchmark for production write-transaction commit RPCs.
 *
 * <p>Actual measured path:</p>
 * <pre>
 * TransService.commitTrans(id, false) or commitTransBatch(ids, false)
 *   -&gt; generated blocking gRPC stub
 *   -&gt; transaction.proto CommitTrans/CommitTransBatch
 *   -&gt; TransServiceImpl
 *   -&gt; TransContextManager.setTransCommit[Batch] + watermark advancement
 * </pre>
 * Source: {@code pixels-common/.../transaction/TransService.java},
 * {@code proto/transaction.proto},
 * {@code pixels-daemon/.../transaction/TransServiceImpl.java}, and
 * {@code pixels-daemon/.../transaction/TransContextManager.java}.
 *
 * <p>A commit of an invented or reused id is not representative: the server
 * returns {@code TRANS_ID_NOT_EXIST}.  Therefore each worker uses the real begin
 * RPC in {@link BenchmarkWorker#prepare} (outside timing) to create unique,
 * pending, non-readonly transactions.  The timed method performs only commit.
 * Each prepared id is consumed at most once.  Prepared ids left unused because
 * the duration expires are committed in {@link #completePhase}, also outside
 * timing.</p>
 */
public final class TransactionCommitBenchmark implements BenchmarkScenario
{
    private final Map<BenchmarkPhase, List<CommitWorker>> phaseWorkers =
            new EnumMap<>(BenchmarkPhase.class);

    private TransactionRpcClients clients;
    private String host;
    private int port;
    private int clientCount;
    private int prefillBatchSize;
    private long rpcDeadlineMs;
    private volatile long warmupTransactionsPrepared;
    private volatile long measurementTransactionsPrepared;
    private volatile long warmupUnusedCleaned;
    private volatile long measurementUnusedCleaned;

    @Override
    public String name()
    {
        return "transaction-commit";
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
        prefillBatchSize = integerOption(config, "trans-prefill-batch-size",
                Math.max(config.batchSize(), 1024));
        if (prefillBatchSize <= 0)
        {
            throw new IllegalArgumentException("trans-prefill-batch-size must be positive");
        }
        clients = new TransactionRpcClients(host, port, clientCount, rpcDeadlineMs);
        for (BenchmarkPhase phase : BenchmarkPhase.values())
        {
            phaseWorkers.put(phase, Collections.synchronizedList(new ArrayList<CommitWorker>()));
        }
    }

    @Override
    public void preparePhase(BenchmarkPhase phase, long totalOperations)
    {
        phaseWorkers.get(phase).clear();
        if (phase == BenchmarkPhase.WARMUP)
        {
            warmupTransactionsPrepared = totalOperations;
        }
        else
        {
            measurementTransactionsPrepared = totalOperations;
        }
    }

    @Override
    public BenchmarkWorker createWorker(BenchmarkPhase phase, int workerId, int clientId,
                                        OperationRange range)
    {
        CommitWorker worker = new CommitWorker(clientId);
        phaseWorkers.get(phase).add(worker);
        return worker;
    }

    @Override
    public void completePhase(BenchmarkPhase phase, BenchmarkResult result)
    {
        long cleaned = 0;
        RuntimeException failure = null;
        List<CommitWorker> workers = phaseWorkers.get(phase);
        synchronized (workers)
        {
            for (CommitWorker worker : workers)
            {
                while (!worker.preparedIds.isEmpty())
                {
                    List<Long> ids = worker.preparedIds.removeFirst();
                    try
                    {
                        cleaned += clients.commitWriteTransactions(worker.clientId, ids);
                    }
                    catch (Exception e)
                    {
                        if (failure == null)
                        {
                            failure = new IllegalStateException(
                                    "failed to clean unused prepared transactions after " + phase, e);
                        }
                        else
                        {
                            failure.addSuppressed(e);
                        }
                    }
                }
            }
        }
        if (phase == BenchmarkPhase.WARMUP)
        {
            warmupUnusedCleaned = cleaned;
        }
        else
        {
            measurementUnusedCleaned = cleaned;
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
        details.put("rpc_mode", "batch-size=1: CommitTrans; batch-size>1: CommitTransBatch");
        details.put("trans_endpoint", host + ":" + port);
        details.put("physical_clients", Integer.toString(clientCount));
        details.put("rpc_deadline_ms", Long.toString(rpcDeadlineMs));
        details.put("server_dependency", "TransServer + etcd");
        details.put("prefill", "real BeginTrans/BeginTransBatch RPC outside measured interval");
        details.put("prefill_batch_size", Integer.toString(prefillBatchSize));
        details.put("id_reuse", "forbidden; every prepared pending transaction is consumed once");
        details.put("lease_constraint", "prefill plus measurement must stay within the write-transaction lease");
        details.put("warmup_transactions_prepared", Long.toString(warmupTransactionsPrepared));
        details.put("measurement_transactions_prepared", Long.toString(measurementTransactionsPrepared));
        details.put("warmup_unused_transactions_cleaned", Long.toString(warmupUnusedCleaned));
        details.put("measurement_unused_transactions_cleaned", Long.toString(measurementUnusedCleaned));
        details.put("ambiguous_rpc_failure", "an attempted id is not retried because commit outcome may be unknown");
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

    private final class CommitWorker implements BenchmarkWorker
    {
        private final int clientId;
        private final Deque<List<Long>> preparedIds = new ArrayDeque<>();
        private long nextOperation;

        private CommitWorker(int clientId)
        {
            this.clientId = clientId;
        }

        @Override
        public void prepare(long firstOperation, long operationCount, int batchSize) throws Exception
        {
            nextOperation = firstOperation;
            long remaining = operationCount;
            ArrayList<Long> measuredBatch = new ArrayList<>(batchSize);
            while (remaining > 0)
            {
                int count = (int) Math.min(remaining, prefillBatchSize);
                List<TransContext> contexts = clients.beginWriteTransactions(clientId, count);
                for (TransContext context : contexts)
                {
                    measuredBatch.add(context.getTransId());
                    if (measuredBatch.size() == batchSize)
                    {
                        preparedIds.addLast(measuredBatch);
                        measuredBatch = new ArrayList<>(batchSize);
                    }
                }
                remaining -= count;
            }
            if (!measuredBatch.isEmpty())
            {
                preparedIds.addLast(measuredBatch);
            }
        }

        @Override
        public OperationResult execute(long firstOperation, int logicalOperationCount) throws Exception
        {
            if (firstOperation != nextOperation)
            {
                throw new IllegalStateException("non-sequential commit operation: expected "
                        + nextOperation + ", got " + firstOperation);
            }
            List<Long> ids = preparedIds.pollFirst();
            if (ids == null)
            {
                throw new IllegalStateException("no prepared transaction ids remain");
            }
            if (ids.size() != logicalOperationCount)
            {
                throw new IllegalStateException("prepared transaction batch has " + ids.size()
                        + " ids, harness requested " + logicalOperationCount);
            }

            /* Consume before the RPC: an exception can mean an unknown remote
             * outcome, so retrying the same id would corrupt the error rate. */
            nextOperation += logicalOperationCount;
            clients.commitWriteTransactions(clientId, ids);
            return OperationResult.success(logicalOperationCount, 1);
        }
    }

    private static int integerOption(BenchmarkConfig config, String key, int defaultValue)
    {
        String value = config.options().get(key);
        return value == null || value.isEmpty() ? defaultValue : Integer.parseInt(value);
    }
}
