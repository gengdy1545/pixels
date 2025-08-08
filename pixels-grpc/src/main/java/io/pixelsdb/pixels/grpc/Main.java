package io.pixelsdb.pixels.grpc;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.grpc.benchmark.BenchmarkOperation;
import io.pixelsdb.pixels.grpc.benchmark.BenchmarkResult;
import io.pixelsdb.pixels.grpc.benchmark.GrpcBenchmarkRunner;
import io.pixelsdb.pixels.grpc.client.Client;
import io.pixelsdb.pixels.grpc.config.GrpcConfig;
import io.pixelsdb.pixels.retina.RetinaProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Main entry point for the gRPC performance benchmark.
 * This class allows testing different optimization strategies and comparing their performance.
 */
public class Main
{
    private static final Logger logger = LogManager.getLogger(Main.class);

    private static final String DEFAULT_SCHEMA = "default";
    private static final String DEFAULT_TABLE = "test_table";

    public static void main(String[] args)
    {
        logger.info("Starting gRPC performance benchmark");

        GrpcConfig config = new GrpcConfig();

        // Run different benchmark scenarios
        try {
            // Test 1: Benchmark unary RPC with reused channels
            runUnaryBenchmark(config);

            // Test 2: Benchmark async unary RPC for parallelization
            runAsyncUnaryBenchmark(config);

            // Test 3: Benchmark streaming RPC
            runStreamingBenchmark(config);

            logger.info("All benchmarks completed successfully");
        } catch (Exception e) {
            logger.error("Benchmark failed", e);
        }
    }

    /**
     * Run a benchmark for unary RPCs using channel reuse optimization.
     */
    private static void runUnaryBenchmark(GrpcConfig config) {
        logger.info("Running unary RPC benchmark (channel reuse optimization)");

        try (Client client = new Client(config);
             GrpcBenchmarkRunner benchmarkRunner = new GrpcBenchmarkRunner(config)) {

            BenchmarkOperation operation = () -> {
                client.getTableStats(DEFAULT_SCHEMA, DEFAULT_TABLE);
            };

            BenchmarkResult result = benchmarkRunner.runBenchmark(operation);
            logger.info("Unary RPC benchmark completed: {} requests/sec",
                    String.format("%.2f", result.getRequestsPerSecond()));
        }
    }

    /**
     * Run a benchmark for async unary RPCs using non-blocking stubs for parallelization.
     */
    private static void runAsyncUnaryBenchmark(GrpcConfig config) {
        logger.info("Running async unary RPC benchmark (non-blocking stubs optimization)");

        try (Client client = new Client(config);
             GrpcBenchmarkRunner benchmarkRunner = new GrpcBenchmarkRunner(config)) {

            BenchmarkOperation operation = () -> {
                client.getTableStatsAsync(DEFAULT_SCHEMA, DEFAULT_TABLE).get();
            };

            BenchmarkResult result = benchmarkRunner.runBenchmark(operation);
            logger.info("Async unary RPC benchmark completed: {} requests/sec",
                    String.format("%.2f", result.getRequestsPerSecond()));
        }
    }

    /**
     * Run a benchmark for streaming RPCs.
     */
    private static void runStreamingBenchmark(GrpcConfig config) {
        logger.info("Running streaming RPC benchmark");

        try (Client client = new Client(config);
             GrpcBenchmarkRunner benchmarkRunner = new GrpcBenchmarkRunner(config)) {

            BenchmarkOperation operation = () -> {
                CountDownLatch latch = new CountDownLatch(1);
                AtomicInteger responseCount = new AtomicInteger(0);
                List<RetinaPB.GetTableLayoutResponse> responses = new ArrayList<>();

                StreamObserver<RetinaPB.GetTableLayoutResponse> responseObserver =
                        new StreamObserver<RetinaPB.GetTableLayoutResponse>() {
                            @Override
                            public void onNext(RetinaPB.GetTableLayoutResponse response) {
                                responseCount.incrementAndGet();
                                responses.add(response);
                            }

                            @Override
                            public void onError(Throwable t) {
                                logger.error("Error in streaming RPC", t);
                                latch.countDown();
                            }

                            @Override
                            public void onCompleted() {
                                latch.countDown();
                            }
                        };

                client.getTableLayoutsStreaming(DEFAULT_SCHEMA, DEFAULT_TABLE, responseObserver);
                latch.await();

                if (responseCount.get() == 0) {
                    logger.warn("No streaming responses received");
                }
            };

            BenchmarkResult result = benchmarkRunner.runBenchmark(operation);
            logger.info("Streaming RPC benchmark completed: {} requests/sec",
                    String.format("%.2f", result.getRequestsPerSecond()));
        } catch (Exception e) {
            logger.error("Streaming benchmark failed", e);
        }
    }
}
