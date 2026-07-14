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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Immutable statistics for one completed phase. Latencies are microseconds. */
public final class BenchmarkResult
{
    private final String benchmark;
    private final BenchmarkPhase phase;
    private final BenchmarkConfig config;
    private final long requestedOperations;
    private final long totalOperations;
    private final long successfulOperations;
    private final long errors;
    private final long apiCalls;
    private final double elapsedSeconds;
    private final double throughputOpsPerSecond;
    private final double apiCallsPerSecond;
    private final double averageLatencyMicros;
    private final double p50LatencyMicros;
    private final double p95LatencyMicros;
    private final double p99LatencyMicros;
    private final double averageApiLatencyMicros;
    private final double p50ApiLatencyMicros;
    private final double p95ApiLatencyMicros;
    private final double p99ApiLatencyMicros;
    private final String stopReason;
    private final String firstError;
    private final Map<String, String> details;

    public BenchmarkResult(String benchmark, BenchmarkPhase phase, BenchmarkConfig config,
                           long requestedOperations, long totalOperations,
                           long successfulOperations, long errors, long apiCalls,
                           double elapsedSeconds, double throughputOpsPerSecond,
                           double apiCallsPerSecond, double averageLatencyMicros,
                           double p50LatencyMicros, double p95LatencyMicros,
                           double p99LatencyMicros, double averageApiLatencyMicros,
                           double p50ApiLatencyMicros, double p95ApiLatencyMicros,
                           double p99ApiLatencyMicros, String stopReason,
                           String firstError, Map<String, String> details)
    {
        this.benchmark = benchmark;
        this.phase = phase;
        this.config = config;
        this.requestedOperations = requestedOperations;
        this.totalOperations = totalOperations;
        this.successfulOperations = successfulOperations;
        this.errors = errors;
        this.apiCalls = apiCalls;
        this.elapsedSeconds = elapsedSeconds;
        this.throughputOpsPerSecond = throughputOpsPerSecond;
        this.apiCallsPerSecond = apiCallsPerSecond;
        this.averageLatencyMicros = averageLatencyMicros;
        this.p50LatencyMicros = p50LatencyMicros;
        this.p95LatencyMicros = p95LatencyMicros;
        this.p99LatencyMicros = p99LatencyMicros;
        this.averageApiLatencyMicros = averageApiLatencyMicros;
        this.p50ApiLatencyMicros = p50ApiLatencyMicros;
        this.p95ApiLatencyMicros = p95ApiLatencyMicros;
        this.p99ApiLatencyMicros = p99ApiLatencyMicros;
        this.stopReason = stopReason;
        this.firstError = firstError;
        this.details = Collections.unmodifiableMap(new LinkedHashMap<>(details));
    }

    public BenchmarkResult withDetails(Map<String, String> newDetails)
    {
        return new BenchmarkResult(benchmark, phase, config, requestedOperations, totalOperations,
                successfulOperations, errors, apiCalls, elapsedSeconds, throughputOpsPerSecond,
                apiCallsPerSecond, averageLatencyMicros, p50LatencyMicros, p95LatencyMicros,
                p99LatencyMicros, averageApiLatencyMicros, p50ApiLatencyMicros,
                p95ApiLatencyMicros, p99ApiLatencyMicros, stopReason, firstError, newDetails);
    }

    public String benchmark() { return benchmark; }
    public BenchmarkPhase phase() { return phase; }
    public BenchmarkConfig config() { return config; }
    public long requestedOperations() { return requestedOperations; }
    public long totalOperations() { return totalOperations; }
    public long successfulOperations() { return successfulOperations; }
    public long errors() { return errors; }
    public long apiCalls() { return apiCalls; }
    public double elapsedSeconds() { return elapsedSeconds; }
    public double throughputOpsPerSecond() { return throughputOpsPerSecond; }
    public double apiCallsPerSecond() { return apiCallsPerSecond; }
    public double averageLatencyMicros() { return averageLatencyMicros; }
    public double p50LatencyMicros() { return p50LatencyMicros; }
    public double p95LatencyMicros() { return p95LatencyMicros; }
    public double p99LatencyMicros() { return p99LatencyMicros; }
    public double averageApiLatencyMicros() { return averageApiLatencyMicros; }
    public double p50ApiLatencyMicros() { return p50ApiLatencyMicros; }
    public double p95ApiLatencyMicros() { return p95ApiLatencyMicros; }
    public double p99ApiLatencyMicros() { return p99ApiLatencyMicros; }
    public String stopReason() { return stopReason; }
    public String firstError() { return firstError; }
    public Map<String, String> details() { return details; }

    public double errorRate()
    {
        return totalOperations == 0 ? 0D : (double) errors / totalOperations;
    }
}
