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
import io.pixelsdb.pixels.common.transaction.TransService;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Fixed pool of production transaction-service clients.
 *
 * <p>The Retina source tree does not contain the external CDC sink's transaction
 * caller, so the benchmark deliberately does not invent a local shortcut.  It
 * uses the repository's production client call path:</p>
 *
 * <pre>
 * TransService.beginTrans(false) / beginTransBatch(n, false)
 *   -&gt; TransServiceGrpc.TransServiceBlockingStub
 *   -&gt; transaction.proto unary gRPC
 *   -&gt; TransServiceImpl.beginTrans / beginTransBatch
 *
 * TransService.commitTrans(id, false) / commitTransBatch(ids, false)
 *   -&gt; the same blocking stub and production server implementation
 * </pre>
 *
 * <p>Source anchors are {@code pixels-common/.../transaction/TransService.java},
 * {@code proto/transaction.proto}, and
 * {@code pixels-daemon/.../transaction/TransServiceImpl.java}.  Channels are
 * created once with {@code CreateDedicatedInstance} and reused for the complete
 * run; no timed operation creates a channel or thread.</p>
 */
public final class TransactionRpcClients implements AutoCloseable
{
    private final TransService[] clients;

    public TransactionRpcClients(String host, int port, int clientCount, long rpcDeadlineMs)
    {
        if (host == null || host.isEmpty())
        {
            throw new IllegalArgumentException("transaction host is empty");
        }
        if (port <= 0 || port > 65535)
        {
            throw new IllegalArgumentException("invalid transaction port: " + port);
        }
        if (clientCount <= 0)
        {
            throw new IllegalArgumentException("client count must be positive");
        }
        if (rpcDeadlineMs <= 0)
        {
            throw new IllegalArgumentException("RPC deadline must be positive");
        }

        /*
         * TransService constructs its static default client on first active use,
         * even when CreateDedicatedInstance is the requested factory.  Populate
         * these production configuration keys first so that class initialization
         * has a valid address.  The active benchmark channels below are still the
         * requested number of dedicated, owned clients.
         */
        ConfigFactory.Instance().addProperty("trans.server.host", host);
        ConfigFactory.Instance().addProperty("trans.server.port", Integer.toString(port));

        this.clients = new TransService[clientCount];
        int created = 0;
        try
        {
            for (; created < clientCount; created++)
            {
                clients[created] = TransService.CreateDedicatedInstance(host, port, rpcDeadlineMs);
            }
        }
        catch (RuntimeException e)
        {
            closePrefix(created);
            throw e;
        }
    }

    public int size()
    {
        return clients.length;
    }

    /**
     * Begin one or a real RPC batch of non-readonly transactions.
     */
    public List<TransContext> beginWriteTransactions(int clientId, int count) throws Exception
    {
        if (count <= 0)
        {
            throw new IllegalArgumentException("begin count must be positive");
        }
        TransService client = client(clientId);
        List<TransContext> contexts;
        if (count == 1)
        {
            contexts = Collections.singletonList(client.beginTrans(false));
        }
        else
        {
            contexts = client.beginTransBatch(count, false);
        }
        validateBeginResponse(contexts, count);
        return contexts;
    }

    /**
     * Commit one or a real RPC batch of non-readonly transactions.  A partial
     * batch response is an operation error, not a successful RPC.
     */
    public int commitWriteTransactions(int clientId, List<Long> transIds) throws Exception
    {
        if (transIds == null || transIds.isEmpty())
        {
            throw new IllegalArgumentException("transaction id list is empty");
        }
        TransService client = client(clientId);
        if (transIds.size() == 1)
        {
            if (!client.commitTrans(transIds.get(0), false))
            {
                throw new IllegalStateException("commitTrans returned false");
            }
            return 1;
        }

        List<Boolean> results = client.commitTransBatch(transIds, false);
        if (results.size() != transIds.size())
        {
            throw new IllegalStateException("commitTransBatch returned " + results.size()
                    + " results for " + transIds.size() + " transaction ids");
        }
        int success = 0;
        for (Boolean result : results)
        {
            if (Boolean.TRUE.equals(result))
            {
                success++;
            }
        }
        if (success != transIds.size())
        {
            throw new IllegalStateException("commitTransBatch partially failed: " + success
                    + "/" + transIds.size());
        }
        return success;
    }

    /**
     * Convenience used outside timed regions to terminate contexts returned by
     * begin.  Chunking keeps cleanup RPC sizes bounded.
     */
    public long commitContexts(int clientId, List<List<TransContext>> batches,
                               int cleanupBatchSize) throws Exception
    {
        if (cleanupBatchSize <= 0)
        {
            throw new IllegalArgumentException("cleanup batch size must be positive");
        }
        ArrayList<Long> ids = new ArrayList<>(cleanupBatchSize);
        long committed = 0;
        for (List<TransContext> batch : batches)
        {
            for (TransContext context : batch)
            {
                ids.add(context.getTransId());
                if (ids.size() == cleanupBatchSize)
                {
                    committed += commitWriteTransactions(clientId, ids);
                    ids.clear();
                }
            }
        }
        if (!ids.isEmpty())
        {
            committed += commitWriteTransactions(clientId, ids);
        }
        return committed;
    }

    private TransService client(int clientId)
    {
        if (clientId < 0)
        {
            throw new IllegalArgumentException("client id must not be negative");
        }
        return clients[clientId % clients.length];
    }

    private static void validateBeginResponse(List<TransContext> contexts, int expected)
    {
        if (contexts == null || contexts.size() != expected)
        {
            throw new IllegalStateException("begin returned "
                    + (contexts == null ? "null" : contexts.size())
                    + " contexts, expected " + expected);
        }
        long previousId = Long.MIN_VALUE;
        for (TransContext context : contexts)
        {
            if (context == null)
            {
                throw new IllegalStateException("begin returned a null transaction context");
            }
            if (context.isReadOnly())
            {
                throw new IllegalStateException("write transaction was returned as readonly");
            }
            /* Current TransServiceImpl uses transId as the write timestamp. */
            if (context.getTimestamp() != context.getTransId())
            {
                throw new IllegalStateException("write transaction timestamp "
                        + context.getTimestamp() + " does not equal transaction id "
                        + context.getTransId());
            }
            if (previousId != Long.MIN_VALUE && context.getTransId() <= previousId)
            {
                throw new IllegalStateException("transaction ids in a batch are not increasing");
            }
            previousId = context.getTransId();
        }
    }

    @Override
    public void close()
    {
        closePrefix(clients.length);
    }

    private void closePrefix(int count)
    {
        boolean interrupted = false;
        for (int i = 0; i < count; i++)
        {
            if (clients[i] == null)
            {
                continue;
            }
            try
            {
                clients[i].shutdown();
            }
            catch (InterruptedException e)
            {
                interrupted = true;
            }
        }
        if (interrupted)
        {
            Thread.currentThread().interrupt();
        }
    }
}
