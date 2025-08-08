package io.pixelsdb.pixels.grpc.client;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.grpc.config.GrpcConfig;
import io.pixelsdb.pixels.proto.PixelsRetinaServiceGrpc;
import io.pixelsdb.pixels.proto.RetinaPB;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A gRPC client that implements various performance optimizations:
 * 1. Channel reuse through ChannelPool
 * 2. Keep-alive pings to maintain HTTP/2 connections
 * 3. Support for both unary and streaming RPCs
 * 4. Non-blocking asynchronous stubs for parallelization
 */
public class Client implements Closeable {
    private static final Logger LOGGER = LogManager.getLogger(Client.class);

    private final ChannelPool channelPool;
    private final GrpcConfig config;

    public Client(GrpcConfig config) {
        this.config = config;
        this.channelPool = new ChannelPool(config.getHost(), config.getPort(), config);
        LOGGER.info("Created gRPC client with channel pool size: {}", config.getChannelPoolSize());
    }

    /**
     * Perform a unary RPC call to get table statistics.
     * This demonstrates a simple unary RPC optimized for connection reuse.
     *
     * @param schemaName The schema name
     * @param tableName The table name
     * @return The table statistics
     */
    public RetinaPB.GetTableStatResponse getTableStats(String schemaName, String tableName) throws MetadataException {
        ManagedChannel channel = channelPool.getChannel();
        PixelsRetinaServiceGrpc.PixelsRetinaServiceBlockingStub stub =
                PixelsRetinaServiceGrpc.newBlockingStub(channel);

        RetinaPB.GetTableStatRequest request = RetinaPB.GetTableStatRequest.newBuilder()
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .build();

        try {
            return stub.getTableStat(request);
        } catch (Exception e) {
            LOGGER.error("Error getting table stats", e);
            throw new MetadataException("Failed to get table stats", e);
        }
    }

    /**
     * Perform an asynchronous unary RPC call.
     * This demonstrates the use of non-blocking stubs for parallelization.
     *
     * @param schemaName The schema name
     * @param tableName The table name
     * @return A future that will complete with the response
     */
    public CompletableFuture<RetinaPB.GetTableStatResponse> getTableStatsAsync(String schemaName, String tableName) {
        ManagedChannel channel = channelPool.getChannel();
        PixelsRetinaServiceGrpc.PixelsRetinaServiceStub asyncStub =
                PixelsRetinaServiceGrpc.newStub(channel);

        CompletableFuture<RetinaPB.GetTableStatResponse> future = new CompletableFuture<>();

        RetinaPB.GetTableStatRequest request = RetinaPB.GetTableStatRequest.newBuilder()
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .build();

        asyncStub.getTableStat(request, new StreamObserver<RetinaPB.GetTableStatResponse>() {
            @Override
            public void onNext(RetinaPB.GetTableStatResponse response) {
                future.complete(response);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                // Nothing to do here
            }
        });

        return future;
    }

    /**
     * Demonstrate server streaming RPC to get multiple table layouts.
     * This implements the streaming optimization mentioned in point 3.
     *
     * @param schemaName The schema name
     * @param tableName The table name
     * @param observer The observer to receive streaming results
     */
    public void getTableLayoutsStreaming(String schemaName, String tableName,
                                         StreamObserver<RetinaPB.GetTableLayoutResponse> observer) {
        ManagedChannel channel = channelPool.getChannel();
        PixelsRetinaServiceGrpc.PixelsRetinaServiceStub asyncStub =
                PixelsRetinaServiceGrpc.newStub(channel);

        RetinaPB.GetTableLayoutRequest request = RetinaPB.GetTableLayoutRequest.newBuilder()
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .build();

        asyncStub.getTableLayout(request, observer);
    }

    @Override
    public void close() {
        channelPool.close();
        LOGGER.info("Closed gRPC client and channel pool");
    }
}
