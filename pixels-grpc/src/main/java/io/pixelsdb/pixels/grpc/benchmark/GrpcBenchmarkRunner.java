package io.pixelsdb.pixels.grpc.benchmark;

import io.pixelsdb.pixels.grpc.config.GrpcConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generic framework for benchmarking gRPC operations.
 * Implements various optimization strategies including parallel execution,
 * connection reuse, and customizable thread pools.
 */
public class GrpcBenchmarkRunner {
    private static final Logger LOGGER = LogManager.getLogger(GrpcBenchmarkRunner.class);

    private final GrpcConfig config;
    private final ExecutorService executor;
    private final int warmupRequests;
    private final int benchmarkRequests;
    private final int concurrency;

    public GrpcBenchmarkRunner(GrpcConfig config) {
        this.config = config;
        this.warmupRequests = config.getWarmup();
        this.benchmarkRequests = config.getRequests();
        this.concurrency = config.getConcurrency();

        // Initialize thread pool based on config
        int threadPoolSize = config.getThreadPoolSize();
        if (threadPoolSize <= 0) {
            // Use a cached thread pool if no specific size is provided
            this.executor = Executors.newCachedThreadPool();
            LOGGER.info("Using cached thread pool for benchmark execution");
        } else {
            // Use a fixed thread pool with the specified size
            this.executor = Executors.newFixedThreadPool(threadPoolSize);
            LOGGER.info("Using fixed thread pool with size: {}", threadPoolSize);
        }
    }

    /**
     * Run a benchmark with the given operation.
     * @param benchmarkOperation The operation to benchmark
     * @return The benchmark results
     */
    public BenchmarkResult runBenchmark(BenchmarkOperation benchmarkOperation) {
        try {
            // Run warmup phase
            if (warmupRequests > 0) {
                LOGGER.info("Starting warmup with {} requests", warmupRequests);
                runRequests(benchmarkOperation, warmupRequests, concurrency, true);
                LOGGER.info("Warmup completed");
            }

            // Run actual benchmark
            LOGGER.info("Starting benchmark with {} requests at concurrency level {}",
                    benchmarkRequests, concurrency);

            long startTime = System.currentTimeMillis();
            List<Long> latencies = runRequests(benchmarkOperation, benchmarkRequests, concurrency, false);
            long endTime = System.currentTimeMillis();

            // Calculate statistics
            double totalTimeSeconds = (endTime - startTime) / 1000.0;
            double requestsPerSecond = benchmarkRequests / totalTimeSeconds;

            // Calculate latency stats
            long totalLatency = 0;
            long minLatency = Long.MAX_VALUE;
            long maxLatency = 0;

            for (Long latency : latencies) {
                totalLatency += latency;
                minLatency = Math.min(minLatency, latency);
                maxLatency = Math.max(maxLatency, latency);
            }

            double avgLatencyMs = (double) totalLatency / latencies.size();

            // Sort latencies for percentile calculations
            latencies.sort(Long::compare);
            long p50 = latencies.get((int)(latencies.size() * 0.5));
            long p90 = latencies.get((int)(latencies.size() * 0.9));
            long p99 = latencies.get((int)(latencies.size() * 0.99));

            BenchmarkResult result = new BenchmarkResult(
                    benchmarkRequests,
                    concurrency,
                    totalTimeSeconds,
                    requestsPerSecond,
                    minLatency,
                    maxLatency,
                    avgLatencyMs,
                    p50,
                    p90,
                    p99
            );

            LOGGER.info(result.toString());
            return result;

        } catch (Exception e) {
            LOGGER.error("Benchmark failed", e);
            throw new RuntimeException("Benchmark failed", e);
        }
    }

    /**
     * Run a specific number of requests with the given concurrency.
     * @param operation The operation to run
     * @param requests The number of requests to run
     * @param concurrency The concurrency level
     * @param isWarmup Whether this is a warmup run
     * @return A list of latencies in milliseconds (empty if warmup)
     */
    private List<Long> runRequests(BenchmarkOperation operation, int requests, int concurrency, boolean isWarmup) throws InterruptedException {
        List<Long> latencies = isWarmup ? new ArrayList<>() : new ArrayList<>(requests);
        CountDownLatch latch = new CountDownLatch(requests);
        AtomicInteger activeRequests = new AtomicInteger(0);

        // Submit all requests to the executor
        for (int i = 0; i < requests; i++) {
            executor.submit(() -> {
                try {
                    activeRequests.incrementAndGet();
                    long startTime = System.nanoTime();

                    operation.run();

                    if (!isWarmup) {
                        long endTime = System.nanoTime();
                        long latencyMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
                        synchronized (latencies) {
                            latencies.add(latencyMs);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("Error executing benchmark operation", e);
                } finally {
                    activeRequests.decrementAndGet();
                    latch.countDown();
                }
            });

            // Limit concurrent requests
            while (activeRequests.get() >= concurrency) {
                Thread.sleep(1);
            }
        }

        // Wait for all requests to complete
        latch.await();
        return latencies;
    }

    /**
     * Close resources used by the benchmark runner.
     */
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
