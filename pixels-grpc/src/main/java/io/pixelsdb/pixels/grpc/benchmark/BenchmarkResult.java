package io.pixelsdb.pixels.grpc.benchmark;

/**
 * Class to hold benchmark results including throughput and latency statistics.
 */
public class BenchmarkResult {
    private final int totalRequests;
    private final int concurrency;
    private final double totalTimeSeconds;
    private final double requestsPerSecond;
    private final long minLatencyMs;
    private final long maxLatencyMs;
    private final double avgLatencyMs;
    private final long p50LatencyMs;
    private final long p90LatencyMs;
    private final long p99LatencyMs;

    public BenchmarkResult(int totalRequests, int concurrency, double totalTimeSeconds,
                           double requestsPerSecond, long minLatencyMs, long maxLatencyMs,
                           double avgLatencyMs, long p50LatencyMs, long p90LatencyMs, long p99LatencyMs) {
        this.totalRequests = totalRequests;
        this.concurrency = concurrency;
        this.totalTimeSeconds = totalTimeSeconds;
        this.requestsPerSecond = requestsPerSecond;
        this.minLatencyMs = minLatencyMs;
        this.maxLatencyMs = maxLatencyMs;
        this.avgLatencyMs = avgLatencyMs;
        this.p50LatencyMs = p50LatencyMs;
        this.p90LatencyMs = p90LatencyMs;
        this.p99LatencyMs = p99LatencyMs;
    }

    public int getTotalRequests() {
        return totalRequests;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public double getTotalTimeSeconds() {
        return totalTimeSeconds;
    }

    public double getRequestsPerSecond() {
        return requestsPerSecond;
    }

    public long getMinLatencyMs() {
        return minLatencyMs;
    }

    public long getMaxLatencyMs() {
        return maxLatencyMs;
    }

    public double getAvgLatencyMs() {
        return avgLatencyMs;
    }

    public long getP50LatencyMs() {
        return p50LatencyMs;
    }

    public long getP90LatencyMs() {
        return p90LatencyMs;
    }

    public long getP99LatencyMs() {
        return p99LatencyMs;
    }

    @Override
    public String toString() {
        return String.format(
                "Benchmark Results:\n" +
                "  Total Requests: %d\n" +
                "  Concurrency: %d\n" +
                "  Total Time: %.2f seconds\n" +
                "  Throughput: %.2f requests/second\n" +
                "  Latency (ms):\n" +
                "    Min: %d\n" +
                "    Max: %d\n" +
                "    Avg: %.2f\n" +
                "    P50: %d\n" +
                "    P90: %d\n" +
                "    P99: %d",
                totalRequests, concurrency, totalTimeSeconds, requestsPerSecond,
                minLatencyMs, maxLatencyMs, avgLatencyMs, p50LatencyMs, p90LatencyMs, p99LatencyMs);
    }
}
