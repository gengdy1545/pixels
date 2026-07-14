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
package io.pixelsdb.pixels.retina.benchmark.common;

import java.io.PrintStream;
import java.util.Locale;
import java.util.Map;

/** Stable, line-oriented output intended for both humans and log parsers. */
public final class ResultPrinter
{
    private ResultPrinter()
    {
    }

    public static void print(BenchmarkResult result, PrintStream output)
    {
        BenchmarkConfig config = result.config();
        line(output, "benchmark", result.benchmark());
        line(output, "phase", result.phase().name().toLowerCase(Locale.ROOT));
        line(output, "threads", config.threads());
        line(output, "clients", config.clients());
        line(output, "batch_size", config.batchSize());
        line(output, "requested_operations", result.requestedOperations());
        line(output, "total_operations", result.totalOperations());
        line(output, "successful_operations", result.successfulOperations());
        line(output, "errors", result.errors());
        formatted(output, "error_rate", result.errorRate());
        formatted(output, "error_rate_percent", result.errorRate() * 100D);
        formatted(output, "elapsed_seconds", result.elapsedSeconds());
        formatted(output, "attempted_ops_per_second",
                result.totalOperations() / result.elapsedSeconds());
        formatted(output, "throughput_ops_per_second", result.throughputOpsPerSecond());
        formatted(output, "average_latency_us", result.averageLatencyMicros());
        formatted(output, "p50_latency_us", result.p50LatencyMicros());
        formatted(output, "p95_latency_us", result.p95LatencyMicros());
        formatted(output, "p99_latency_us", result.p99LatencyMicros());
        line(output, "api_calls", result.apiCalls());
        formatted(output, "api_calls_per_second", result.apiCallsPerSecond());
        formatted(output, "average_api_latency_us", result.averageApiLatencyMicros());
        formatted(output, "p50_api_latency_us", result.p50ApiLatencyMicros());
        formatted(output, "p95_api_latency_us", result.p95ApiLatencyMicros());
        formatted(output, "p99_api_latency_us", result.p99ApiLatencyMicros());
        line(output, "stop_reason", result.stopReason());
        line(output, "first_error", result.firstError() == null ? "none" : result.firstError());
        line(output, "logical_latency_model",
                "execute-group elapsed / logical ops; exact per-op percentiles when batch_size=1");
        line(output, "api_latency_model",
                "execute-group elapsed / API calls; exact call percentiles when one API call is timed per group");
        line(output, "harness_overhead",
                "two nanoTime reads plus thread-local counters and HdrHistogram records per execute group");
        line(output, "duration_semantics",
                "soft deadline checked between execute groups; an in-flight local call may finish after it");
        for (Map.Entry<String, String> entry : config.options().entrySet())
        {
            line(output, "option." + entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, String> entry : result.details().entrySet())
        {
            line(output, "detail." + entry.getKey(), entry.getValue());
        }
    }

    private static void formatted(PrintStream output, String key, double value)
    {
        output.printf(Locale.ROOT, "%s=%.6f%n", key, value);
    }

    private static void line(PrintStream output, String key, Object value)
    {
        String text = String.valueOf(value).replace('\n', ' ').replace('\r', ' ');
        output.println(key + "=" + text);
    }
}
