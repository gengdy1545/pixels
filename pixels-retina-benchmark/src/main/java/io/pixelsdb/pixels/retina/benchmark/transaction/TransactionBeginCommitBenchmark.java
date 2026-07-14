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
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkScenario;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkWorker;
import io.pixelsdb.pixels.retina.benchmark.common.OperationRange;
import io.pixelsdb.pixels.retina.benchmark.common.OperationResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Throughput benchmark for complete production write-transaction lifecycles.
 *
 * <p>Each timed worker invocation begins transactions and immediately commits
 * exactly those returned transaction ids. A group therefore represents
 * completed transactions and always consists of two API calls. Scalar groups
 * use BeginTrans followed by CommitTrans; larger groups use BeginTransBatch
 * followed by CommitTransBatch. No transaction ids are prefetched, retained in
 * a pending window, or retried after an exception.</p>
 */
public final class TransactionBeginCommitBenchmark implements BenchmarkScenario
{
    private TransactionRpcClients clients;
    private String host;
    private int port;
    private int clientCount;
    private long rpcDeadlineMs;

    @Override
    public String name()
    {
        return "transaction-begin-commit";
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
        clients = new TransactionRpcClients(host, port, clientCount, rpcDeadlineMs);
    }

    @Override
    public BenchmarkWorker createWorker(BenchmarkPhase phase, int workerId, int clientId,
                                        OperationRange range)
    {
        return new BeginCommitWorker(clientId);
    }

    @Override
    public Map<String, String> details()
    {
        Map<String, String> details = new LinkedHashMap<>();
        details.put("call_type", "RPC");
        details.put("transaction_mode", "write (readOnly=false)");
        details.put("logical_operation", "one successfully begun and committed transaction");
        details.put("rpc_mode", "batch-size=1: BeginTrans + CommitTrans; "
                + "batch-size>1: BeginTransBatch + CommitTransBatch");
        details.put("api_calls_per_group", "2");
        details.put("trans_endpoint", host + ":" + port);
        details.put("physical_clients", Integer.toString(clientCount));
        details.put("rpc_deadline_ms", Long.toString(rpcDeadlineMs));
        details.put("server_dependency", "TransServer + etcd");
        details.put("prefill", "none");
        details.put("pending_window", "none");
        details.put("retry", "none; an exception fails the group");
        details.put("ambiguous_rpc_failure", "a begun context may remain until its lease expires");
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

    private final class BeginCommitWorker implements BenchmarkWorker
    {
        private final int clientId;

        private BeginCommitWorker(int clientId)
        {
            this.clientId = clientId;
        }

        @Override
        public void prepare(long firstOperation, long operationCount, int batchSize)
        {
            // A lifecycle operation has no out-of-band transaction preparation.
        }

        @Override
        public OperationResult execute(long firstOperation, int logicalOperationCount) throws Exception
        {
            List<TransContext> contexts =
                    clients.beginWriteTransactions(clientId, logicalOperationCount);
            List<Long> transactionIds;
            if (logicalOperationCount == 1)
            {
                transactionIds = Collections.singletonList(contexts.get(0).getTransId());
            }
            else
            {
                transactionIds = new ArrayList<>(logicalOperationCount);
                for (TransContext context : contexts)
                {
                    transactionIds.add(context.getTransId());
                }
            }
            clients.commitWriteTransactions(clientId, transactionIds);
            return OperationResult.success(logicalOperationCount, 2);
        }

        @Override
        public long expectedApiCalls(int logicalOperationCount)
        {
            return 2;
        }
    }
}
