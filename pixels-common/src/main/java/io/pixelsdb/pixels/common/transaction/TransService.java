/*
 * Copyright 2022 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.common.transaction;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.common.error.ErrorCode;
import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.lease.Lease;
import io.pixelsdb.pixels.common.metadata.MetadataCache;
import io.pixelsdb.pixels.common.server.HostAddress;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.ShutdownHookManager;
import io.pixelsdb.pixels.daemon.TransProto;
import io.pixelsdb.pixels.daemon.TransServiceGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author hank
 * @create 2022-02-20
 * @update 2023-05-02 merge transaction context management into trans service.
 * @update 2025-10-05 eliminate transaction context cache and metadata cache for non-readonly transactions.
 */
public class TransService
{
    private static final Logger logger = LogManager.getLogger(TransService.class);
    private static final TransService defaultInstance;
    private static final Map<HostAddress, TransService> otherInstances = new ConcurrentHashMap<>();

    static
    {
        String transHost = ConfigFactory.Instance().getProperty("trans.server.host");
        int transPort = Integer.parseInt(ConfigFactory.Instance().getProperty("trans.server.port"));
        defaultInstance = new TransService(transHost, transPort);
        ShutdownHookManager.Instance().registerShutdownHook(TransService.class, false, () -> {
            try
            {
                defaultInstance.shutdown();
                for (TransService otherTransService : otherInstances.values())
                {
                    otherTransService.shutdown();
                }
                otherInstances.clear();
            } catch (InterruptedException e)
            {
                logger.error("failed to shut down trans service", e);
            }
        });
    }

    /**
     * Get the default trans service instance connecting to the trans host:port configured in
     * PIXELS_HOME/etc/pixels.properties. This default instance will be automatically shut down when the process
     * is terminating, no need to call {@link #shutdown()} (although it is idempotent) manually.
     * @return the default trans service instance
     */
    public static TransService Instance()
    {
        return defaultInstance;
    }

    /**
     * This method should only be used to connect to a trans server that is not configured through
     * PIXELS_HOME/etc/pixels.properties. <b>No need</b> to manually shut down the returned trans service.
     * @param host the host name of the trans server
     * @param port the port of the trans server
     * @return the created trans service instance
     */
    public static synchronized TransService CreateInstance(String host, int port)
    {
        HostAddress address = HostAddress.fromParts(host, port);
        TransService transService = otherInstances.get(address);
        if (transService != null)
        {
            return transService;
        }
        transService = new TransService(host, port);
        otherInstances.put(address, transService);
        return transService;
    }

    /**
     * Create a transaction client with a dedicated channel.
     * <p>
     * Normal Pixels callers should use {@link #Instance()} or {@link #CreateInstance(String, int)},
     * both of which intentionally reuse a channel.  A load generator may need more than one
     * physical client connection to the same server, so it cannot use the address-keyed cache in
     * {@code CreateInstance}.  This factory keeps the exact same request construction, blocking
     * stub, and response validation used by production while only changing channel ownership.
     * The caller must invoke {@link #shutdown()} when the dedicated client is no longer needed.
     */
    public static TransService CreateDedicatedInstance(String host, int port)
    {
        return new TransService(host, port);
    }

    /**
     * Create a dedicated transaction client whose every RPC gets a fresh
     * relative deadline. This retains the production request/response path and
     * is intended for bounded load generators; cached production clients keep
     * their historical no-deadline behavior.
     *
     * @param host transaction server host
     * @param port transaction server port
     * @param rpcDeadlineMs positive per-RPC deadline in milliseconds
     */
    public static TransService CreateDedicatedInstance(String host, int port, long rpcDeadlineMs)
    {
        if (rpcDeadlineMs <= 0)
        {
            throw new IllegalArgumentException("rpcDeadlineMs must be positive");
        }
        return new TransService(host, port, rpcDeadlineMs);
    }

    private final ManagedChannel channel;
    private final TransServiceGrpc.TransServiceBlockingStub stub;
    private final long rpcDeadlineMs;
    private boolean isShutDown;

    private TransService(String host, int port)
    {
        this(host, port, 0L);
    }

    private TransService(String host, int port, long rpcDeadlineMs)
    {
        assert (host != null);
        assert (port > 0 && port <= 65535);
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext().build();
        this.stub = TransServiceGrpc.newBlockingStub(channel);
        this.rpcDeadlineMs = rpcDeadlineMs;
        this.isShutDown = false;
    }

    private TransServiceGrpc.TransServiceBlockingStub rpcStub()
    {
        /* withDeadlineAfter must be called for every invocation: a stub stores
           an absolute Deadline, so reusing a decorated stub would expire it. */
        return this.rpcDeadlineMs > 0
                ? this.stub.withDeadlineAfter(this.rpcDeadlineMs, TimeUnit.MILLISECONDS)
                : this.stub;
    }

    /**
     * Close this client's owned channel. Cached production instances are
     * normally closed by {@link ShutdownHookManager}; callers of
     * {@link #CreateDedicatedInstance(String, int)} close their instances
     * explicitly through this method.
     */
    public synchronized void shutdown() throws InterruptedException
    {
        if (!this.isShutDown)
        {
            this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            this.isShutDown = true;
        }
    }

    /**
     * Begin a transaction.
     * @param readOnly true if the transaction is determined to be read only, false otherwise
     * @return the initialized context of the transaction, containing the allocated trans id and timestamp
     * @throws TransException
     */
    public TransContext beginTrans(boolean readOnly) throws TransException
    {
        TransProto.BeginTransRequest request = TransProto.BeginTransRequest.newBuilder()
                .setReadOnly(readOnly).build();
        TransProto.BeginTransResponse response = this.rpcStub().beginTrans(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to begin transaction, error code=" + response.getErrorCode());
        }
        TransContext context = new TransContext(response.getTransId(), response.getTimestamp(),
                response.getLeaseStartMs(), response.getLeasePeriodMs(), readOnly);
        if (readOnly)
        {
            // Issue #1099: only use trans context cache and metadata cache for read only queries.
            TransContextCache.Instance().addTransContext(context);
            MetadataCache.Instance().initCache(context.getTransId());
        }
        return context;
    }

    /**
     * Begin a batch of transactions.
     * @param numTrans the number of transaction to begin as a batch
     * @param readOnly true if the transaction is determined to be read only, false otherwise
     * @return the initialized contexts of the transactions in the batch
     * @throws TransException
     */
    public List<TransContext> beginTransBatch(int numTrans, boolean readOnly) throws TransException
    {
        TransProto.BeginTransBatchRequest request = TransProto.BeginTransBatchRequest.newBuilder()
                .setReadOnly(readOnly).setExpectNumTrans(numTrans).build();
        TransProto.BeginTransBatchResponse response = this.rpcStub().beginTransBatch(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to begin the batch of transactions, error code=" + response.getErrorCode());
        }
        ImmutableList.Builder<TransContext> contexts = ImmutableList.builder();
        for (int i = 0; i < response.getExactNumTrans(); i++)
        {
            long transId = response.getTransIds(i);
            long timestamp = response.getTimestamps(i);
            long leaseStartMs = response.getLeaseStartMses(i);
            long leasePeriodMs = response.getLeasePeriodMses(i);
            TransContext context = new TransContext(transId, timestamp, leaseStartMs, leasePeriodMs, readOnly);
            if (readOnly)
            {
                // Issue #1099: only use trans context cache and metadata cache for read only queries.
                TransContextCache.Instance().addTransContext(context);
                MetadataCache.Instance().initCache(context.getTransId());
            }
            contexts.add(context);
        }
        return contexts.build();
    }

    /**
     * Commit a transaction.
     * @param transId the transaction id
     * @param readOnly true if the transaction is readonly
     * @return true on success
     * @throws TransException
     */
    public boolean commitTrans(long transId , boolean readOnly) throws TransException
    {
        TransProto.CommitTransRequest request = TransProto.CommitTransRequest.newBuilder()
                .setTransId(transId).build();
        TransProto.CommitTransResponse response = this.rpcStub().commitTrans(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to commit transaction, error code=" + response.getErrorCode());
        }
        if (readOnly)
        {
            // Issue #1099: only use trans context cache and metadata cache for read only queries.
            TransContextCache.Instance().setTransCommit(transId);
            MetadataCache.Instance().dropCache(transId);
        }
        return true;
    }

    /**
     * Commit a batch of transactions and return whether the execution succeeded.
     * If execution fails, specific error logs can be obtained from the transService logs,
     * such as the transaction does not exist or the commit fails.
     * @param transIds transaction ids of the transactions to commit
     * @param readOnly true if the transactions are readonly
     * @return whether each transaction was successfully committed
     * @throws TransException
     */
    public List<Boolean> commitTransBatch(List<Long> transIds, boolean readOnly) throws TransException
    {
        if (transIds == null || transIds.isEmpty())
        {
            throw new IllegalArgumentException("transIds is null or empty");
        }
        TransProto.CommitTransBatchRequest request = TransProto.CommitTransBatchRequest.newBuilder()
                .setReadOnly(readOnly).addAllTransIds(transIds).build();
        TransProto.CommitTransBatchResponse response = this.rpcStub().commitTransBatch(request);
        if (response.getErrorCode() == ErrorCode.TRANS_INVALID_ARGUMENT) // other error codes are not thrown as exceptions
        {
            throw new TransException("transaction ids and timestamps size mismatch");
        }
        if (readOnly)
        {
            // Issue #1099: only use trans context cache and metadata cache for read only queries.
            for (long transId : transIds)
            {
                TransContextCache.Instance().setTransCommit(transId);
                MetadataCache.Instance().dropCache(transId);
            }
        }
        return response.getResultsList();
    }

    /**
     * Rollback a transaction.
     * @param transId the transaction id
     * @param readOnly true if the transaction is readonly
     * @return true on success
     * @throws TransException
     */
    public boolean rollbackTrans(long transId, boolean readOnly) throws TransException
    {
        TransProto.RollbackTransRequest request = TransProto.RollbackTransRequest.newBuilder()
                .setTransId(transId).build();
        TransProto.RollbackTransResponse response = this.rpcStub().rollbackTrans(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to rollback transaction, error code=" + response.getErrorCode());
        }
        if (readOnly)
        {
            // Issue #1099: only use trans context cache and metadata cache for read only queries.
            TransContextCache.Instance().setTransRollback(transId);
            MetadataCache.Instance().dropCache(transId);
        }
        return true;
    }

    /**
     * Check and extend the lease of the transaction if it is expiring.
     * <br/>
     * <b>Note: this method is not thread-safe</b>, do not try to extend the lease of the same transaction
     * from concurrent threads.
     * @param transContext the transaction context with a lease
     * @return true if the lease is not expiring or has been successfully extended, false if the transaction lease
     * has already expired
     * @throws TransException if failed to extend the lease on the assigner (transaction server) side
     */
    public boolean extendTransLease(TransContext transContext) throws TransException
    {
        Lease lease = transContext.getLease();
        long currentTimeMs = System.currentTimeMillis();
        if (lease.hasExpired(currentTimeMs, Lease.Role.Holder))
        {
            return false;
        }
        if (!lease.expiring(currentTimeMs, Lease.Role.Holder))
        {
            return true;
        }
        TransProto.ExtendTransLeaseRequest request = TransProto.ExtendTransLeaseRequest.newBuilder()
                .setTransId(transContext.getTransId()).build();
        TransProto.ExtendTransLeaseResponse response = this.rpcStub().extendTransLease(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("transaction " + transContext.getTransId() +
                    " not exist or its lease has expired, error code=" + response.getErrorCode());
        }
        lease.updateStartMs(response.getNewLeaseStartMs());
        return true;
    }

    /**
     * Check and extend the lease of the transactions if they are expiring.
     * <br/>
     * <b>Note: this method is not thread-safe</b>, do not try to extend the lease of the same transaction
     * from concurrent threads.
     * @param transContexts the transaction contexts with leases
     * @return for each transaction, true if the lease is successfully extended,
     * false if the transaction lease has already expired
     * @throws TransException if failed to extend the leases on the assigner (transaction server) side
     */
    public List<Boolean> extendTransLeaseBatch(List<TransContext> transContexts) throws TransException
    {
        TransProto.ExtendTransLeaseBatchRequest.Builder requestBuilder = TransProto.ExtendTransLeaseBatchRequest.newBuilder();
        for (TransContext transContext : transContexts)
        {
            requestBuilder.addTransIds(transContext.getTransId());
        }
        TransProto.ExtendTransLeaseBatchResponse response = this.rpcStub().extendTransLeaseBatch(requestBuilder.build());
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to extend lease of transactions, error code=" + response.getErrorCode());
        }
        long newLeaseStartMs = response.getNewLeaseStartMs();
        List<Boolean> success = response.getSuccessList();
        if (success.size() != transContexts.size())
        {
            throw new TransException("invalid response returned by transaction server");
        }
        for (int i = 0; i < transContexts.size(); i++)
        {
            if (success.get(i))
            {
                TransContext transContext = transContexts.get(i);
                transContext.getLease().updateStartMs(newLeaseStartMs);
            }
        }
        return success;
    }

    public TransContext getTransContext(long transId) throws TransException
    {
        TransProto.GetTransContextRequest request = TransProto.GetTransContextRequest.newBuilder()
                .setTransId(transId).build();
        TransProto.GetTransContextResponse response = this.rpcStub().getTransContext(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to get transaction context, error code=" + response.getErrorCode());
        }
        return new TransContext(response.getTransContext());
    }

    public TransContext getTransContext(String externalTraceId) throws TransException
    {
        TransProto.GetTransContextRequest request = TransProto.GetTransContextRequest.newBuilder()
                .setExternalTraceId(externalTraceId).build();
        TransProto.GetTransContextResponse response = this.rpcStub().getTransContext(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to get transaction context, error code=" + response.getErrorCode());
        }
        return new TransContext(response.getTransContext());
    }

    /**
     * Set the string property of a transaction.
     * @param transId the id of the transaction
     * @param key the property key
     * @param value the property value
     * @return the previous value of the property key, or null if not present
     * @throws TransException if the transaction does not exist
     */
    public String setTransProperty(long transId, String key, String value) throws TransException
    {
        TransProto.SetTransPropertyRequest request = TransProto.SetTransPropertyRequest.newBuilder()
                .setTransId(transId).setKey(key).setValue(value).build();
        return setTransProperty(request);
    }

    /**
     * Set the string property of a transaction.
     * @param externalTraceId the external trace id (token) of the transaction
     * @param key the property key
     * @param value the property value
     * @return the previous value of the property key, or null if not present
     * @throws TransException if the transaction does not exist
     */
    public String setTransProperty(String externalTraceId, String key, String value) throws TransException
    {
        TransProto.SetTransPropertyRequest request = TransProto.SetTransPropertyRequest.newBuilder()
                .setExternalTraceId(externalTraceId).setKey(key).setValue(value).build();
        return setTransProperty(request);
    }

    private String setTransProperty(TransProto.SetTransPropertyRequest request) throws TransException
    {
        TransProto.SetTransPropertyResponse response = this.rpcStub().setTransProperty(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to set transaction property, error code=" + response.getErrorCode());
        }
        if (response.hasPrevValue())
        {
            return response.getPrevValue();
        }
        return null;
    }

    /**
     * Update the costs of a transaction (query).
     * @param transId the id of the transaction
     * @param scanBytes the scan bytes to set in the transaction context
     * @param costCents the cost in cents to set in the transaction context
     * @return true of the costs are updates successfully, otherwise false
     * @throws TransException if the transaction does not exist
     */
    public boolean updateQueryCosts(long transId, double scanBytes, QueryCost costCents) throws TransException
    {
        TransProto.UpdateQueryCostsRequest request = null;
        if (costCents.getType() == QueryCostType.VMCOST)
        {
            request = TransProto.UpdateQueryCostsRequest.newBuilder()
                .setTransId(transId).setScanBytes(scanBytes).setVmCostCents(costCents.getCostCents()).build();
        }
        else if (costCents.getType() == QueryCostType.CFCOST)
        {
            request = TransProto.UpdateQueryCostsRequest.newBuilder()
                    .setTransId(transId).setScanBytes(scanBytes).setCfCostCents(costCents.getCostCents()).build();
        }
        assert(request != null);
        return updateQueryCosts(request);
    }

    /**
     * Update the costs of a transaction (query).
     * @param externalTraceId the external trace id (token) of the transaction
     * @param scanBytes the scan bytes to set in the transaction context
     * @param costCents the cost in cents to set in the transaction context
     * @return true of the costs are updates successfully, otherwise false
     * @throws TransException if the transaction does not exist
     */
    public boolean updateQueryCosts(String externalTraceId, double scanBytes, QueryCost costCents) throws TransException
    {
        TransProto.UpdateQueryCostsRequest request = null;
        if (costCents.getType() == QueryCostType.VMCOST)
        {
            request = TransProto.UpdateQueryCostsRequest.newBuilder()
                    .setExternalTraceId(externalTraceId).setScanBytes(scanBytes).setVmCostCents(costCents.getCostCents()).build();
        }
        else if (costCents.getType() == QueryCostType.CFCOST)
        {
            request = TransProto.UpdateQueryCostsRequest.newBuilder()
                    .setExternalTraceId(externalTraceId).setScanBytes(scanBytes).setCfCostCents(costCents.getCostCents()).build();
        }
        assert(request != null);
        return updateQueryCosts(request);
    }

    private boolean updateQueryCosts(TransProto.UpdateQueryCostsRequest request) throws TransException
    {
        TransProto.UpdateQueryCostsResponse response = this.rpcStub().updateQueryCosts(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to update query costs, error code=" + response.getErrorCode());
        }
        return true;
    }

    public int getTransConcurrency(boolean readOnly) throws TransException
    {
        TransProto.GetTransConcurrencyRequest request = TransProto.GetTransConcurrencyRequest.newBuilder()
                .setReadOnly(readOnly).build();
        TransProto.GetTransConcurrencyResponse response = this.rpcStub().getTransConcurrency(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to get transaction concurrency, error code=" + response.getErrorCode());
        }
        return response.getConcurrency();
    }

    public boolean bindExternalTraceId(long transId, String externalTraceId) throws TransException
    {
        TransProto.BindExternalTraceIdRequest request = TransProto.BindExternalTraceIdRequest.newBuilder()
                .setTransId(transId).setExternalTraceId(externalTraceId).build();
        TransProto.BindExternalTraceIdResponse response = this.rpcStub().bindExternalTraceId(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to bind transaction id and external trace id, error code="
                    + response.getErrorCode());
        }
        return true;
    }

    public long getSafeGcTimestamp() throws TransException
    {
        TransProto.GetSafeGcTimestampResponse response = this.rpcStub().getSafeGcTimestamp(Empty.getDefaultInstance());
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to get safe garbage collection timestamp"
                    + response.getErrorCode());
        }
        return response.getTimestamp();
    }

    /**
     * Mark a transaction as offloaded. This allows the transaction to be skipped when
     * calculating the minimum running transaction timestamp for garbage collection.
     * 
     * @param transId the id of the transaction to mark as offloaded
     * @return true on success
     * @throws TransException if the operation fails
     */
    public boolean markTransOffloaded(long transId) throws TransException
    {
        TransProto.MarkTransOffloadedRequest request = TransProto.MarkTransOffloadedRequest.newBuilder()
                .setTransId(transId).build();
        TransProto.MarkTransOffloadedResponse response = this.rpcStub().markTransOffloaded(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to mark transaction as offloaded, error code=" + response.getErrorCode());
        }
        return true;
    }
}
