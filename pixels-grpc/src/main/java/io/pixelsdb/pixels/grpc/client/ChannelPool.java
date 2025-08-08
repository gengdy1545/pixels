package io.pixelsdb.pixels.grpc.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.grpc.config.GrpcConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A pool of gRPC channels to distribute RPCs over multiple connections.
 * This helps with the HTTP/2 connection stream limit issue described in optimization point 4.
 */
public class ChannelPool implements AutoCloseable {
    private final List<ManagedChannel> channels;
    private final AtomicInteger nextChannelIndex = new AtomicInteger(0);
    private final GrpcConfig config;

    public ChannelPool(String host, int port, GrpcConfig config) {
        this.config = config;
        int poolSize = config.getChannelPoolSize();
        this.channels = new ArrayList<>(poolSize);

        for (int i = 0; i < poolSize; i++) {
            ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext();

            // Apply keepalive settings to optimize connection reuse
            channelBuilder.keepAliveTime(config.getKeepAliveTime(), TimeUnit.MILLISECONDS)
                    .keepAliveTimeout(config.getKeepAliveTimeout(), TimeUnit.MILLISECONDS)
                    .keepAliveWithoutCalls(config.getKeepAliveWithoutCalls());

            // Max inbound message size for streaming large responses
            channelBuilder.maxInboundMessageSize(config.getMaxInboundMessageSize());

            // Add a channel number arg to prevent channel reuse
            // This is necessary to maintain separate channels in the pool
            channelBuilder.defaultLoadBalancingPolicy("round_robin");
            channelBuilder.disableRetry();

            // Create the channel and add it to the pool
            channels.add(channelBuilder.build());
        }
    }

    /**
     * Get the next channel from the pool using round-robin selection.
     * @return A managed channel
     */
    public ManagedChannel getChannel() {
        int index = nextChannelIndex.getAndIncrement() % channels.size();
        return channels.get(index);
    }

    @Override
    public void close() {
        for (ManagedChannel channel : channels) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                if (!channel.isShutdown()) {
                    channel.shutdownNow();
                }
            }
        }
    }
}
