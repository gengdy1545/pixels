package io.pixelsdb.pixels.grpc.benchmark;

/**
 * Interface for operations to be benchmarked.
 */
public interface BenchmarkOperation {
    /**
     * Executes a single benchmark operation.
     */
    void run() throws Exception;
}
