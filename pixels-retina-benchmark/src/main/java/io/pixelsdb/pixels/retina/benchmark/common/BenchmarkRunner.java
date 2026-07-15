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

import org.HdrHistogram.Histogram;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Reusable fixed-thread throughput harness.
 *
 * <p>Workers, connections and test records are prepared before the start
 * latch. The measured hot path adds one {@code nanoTime} pair, thread-local
 * counters and two thread-local HdrHistogram records per execute group. There
 * are no per-operation thread creations, connection creations, console writes
 * or shared success counters. With {@code batch-size > 1}, logical latency is
 * the group's elapsed time divided by logical operations; API latency is the
 * same elapsed time divided by reported API calls. Thus percentiles are exact
 * invocation percentiles only when the corresponding divisor is one.</p>
 */
public final class BenchmarkRunner
{
    private static final long HIGHEST_TRACKABLE_NANOS = TimeUnit.HOURS.toNanos(1);

    private BenchmarkRunner()
    {
    }

    public static BenchmarkResult run(BenchmarkScenario scenario, BenchmarkConfig config) throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool(config.threads(), new BenchmarkThreadFactory());
        Exception failure = null;
        try
        {
            scenario.setup(config);
            if (config.warmupSeconds() > 0 && config.warmupOperations() > 0)
            {
                runPhase(scenario, config, executor, BenchmarkPhase.WARMUP,
                        config.warmupOperations(), config.warmupSeconds());
            }
            BenchmarkResult measured = runPhase(scenario, config, executor,
                    BenchmarkPhase.MEASUREMENT, config.dataSize(), config.durationSeconds());
            Map<String, String> details = scenario.details();
            return measured.withDetails(details);
        }
        catch (Exception e)
        {
            failure = e;
            throw e;
        }
        finally
        {
            executor.shutdownNow();
            try
            {
                executor.awaitTermination(10, TimeUnit.SECONDS);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                if (failure == null)
                {
                    throw e;
                }
                failure.addSuppressed(e);
            }
            try
            {
                scenario.close();
            }
            catch (Exception e)
            {
                if (failure == null)
                {
                    throw e;
                }
                failure.addSuppressed(e);
            }
        }
    }

    private static BenchmarkResult runPhase(BenchmarkScenario scenario, BenchmarkConfig config,
                                            ExecutorService executor, BenchmarkPhase phase,
                                            long requestedOperations, long seconds) throws Exception
    {
        scenario.preparePhase(phase, requestedOperations);

        List<BenchmarkWorker> workers = new ArrayList<>(config.threads());
        List<OperationRange> ranges = new ArrayList<>(config.threads());
        for (int workerId = 0; workerId < config.threads(); workerId++)
        {
            OperationRange range = OperationRange.partition(requestedOperations,
                    config.threads(), workerId);
            ranges.add(range);
            workers.add(scenario.createWorker(phase, workerId,
                    workerId % config.clients(), range));
        }

        boolean workersClosed = false;
        try
        {
            prepareWorkers(executor, workers, ranges, config.batchSize());
            scenario.completePreparation(phase);

            CountDownLatch ready = new CountDownLatch(workers.size());
            CountDownLatch start = new CountDownLatch(1);
            PhaseClock clock = new PhaseClock();
            AtomicBoolean abort = new AtomicBoolean(false);
            AtomicReference<String> firstError = new AtomicReference<>();
            boolean failFast = config.getBoolean("fail-fast", false);
            List<Future<WorkerStatistics>> futures = new ArrayList<>(workers.size());

            for (int i = 0; i < workers.size(); i++)
            {
                final BenchmarkWorker worker = workers.get(i);
                final OperationRange range = ranges.get(i);
                futures.add(executor.submit(() -> runWorker(worker, range, config.batchSize(),
                        ready, start, clock, abort, failFast, firstError)));
            }

            ready.await();
            clock.startNanos = System.nanoTime();
            clock.deadlineNanos = saturatingAdd(clock.startNanos, TimeUnit.SECONDS.toNanos(seconds));
            start.countDown();

            Histogram logicalLatency = histogram();
            Histogram apiLatency = histogram();
            long attempted = 0;
            long successful = 0;
            long errors = 0;
            long apiCalls = 0;
            for (Future<WorkerStatistics> future : futures)
            {
                WorkerStatistics worker = future.get();
                attempted += worker.attempted;
                successful += worker.successful;
                errors += worker.errors;
                apiCalls += worker.apiCalls;
                logicalLatency.add(worker.logicalLatency);
                apiLatency.add(worker.apiLatency);
            }
            long finishNanos = System.nanoTime();

            closeWorkers(workers);
            workersClosed = true;

            double elapsed = Math.max(1L, finishNanos - clock.startNanos) / 1_000_000_000D;
            String stopReason;
            if (abort.get())
            {
                stopReason = "error";
            }
            else if (attempted >= requestedOperations)
            {
                stopReason = "data-size";
            }
            else
            {
                stopReason = "duration";
            }

            BenchmarkResult result = new BenchmarkResult(scenario.name(), phase, config,
                    requestedOperations, attempted, successful, errors, apiCalls, elapsed,
                    successful / elapsed, apiCalls / elapsed,
                    micros(logicalLatency.getMean()), micros(logicalLatency.getValueAtPercentile(50D)),
                    micros(logicalLatency.getValueAtPercentile(95D)),
                    micros(logicalLatency.getValueAtPercentile(99D)),
                    micros(apiLatency.getMean()), micros(apiLatency.getValueAtPercentile(50D)),
                    micros(apiLatency.getValueAtPercentile(95D)),
                    micros(apiLatency.getValueAtPercentile(99D)), stopReason,
                    firstError.get(), java.util.Collections.<String, String>emptyMap());
            scenario.completePhase(phase, result);
            return result;
        }
        finally
        {
            if (!workersClosed)
            {
                closeWorkers(workers);
            }
        }
    }

    private static void prepareWorkers(ExecutorService executor, List<BenchmarkWorker> workers,
                                       List<OperationRange> ranges, int batchSize) throws Exception
    {
        List<Future<?>> preparations = new ArrayList<>(workers.size());
        for (int i = 0; i < workers.size(); i++)
        {
            final BenchmarkWorker worker = workers.get(i);
            final OperationRange range = ranges.get(i);
            preparations.add(executor.submit(() ->
            {
                worker.prepare(range.startInclusive(), range.size(), batchSize);
                return null;
            }));
        }
        for (Future<?> preparation : preparations)
        {
            preparation.get();
        }
    }

    private static WorkerStatistics runWorker(BenchmarkWorker worker, OperationRange range,
                                              int batchSize, CountDownLatch ready,
                                              CountDownLatch start, PhaseClock clock,
                                              AtomicBoolean abort, boolean failFast,
                                              AtomicReference<String> firstError) throws Exception
    {
        WorkerStatistics statistics = new WorkerStatistics();
        ready.countDown();
        start.await();

        long cursor = range.startInclusive();
        while (cursor < range.endExclusive())
        {
            /* Reuse the latency start timestamp for the soft-deadline check so
               the hot path has exactly two nanoTime reads per execute group. */
            long before = System.nanoTime();
            if (before >= clock.deadlineNanos || (failFast && abort.get()))
            {
                break;
            }
            int count = (int) Math.min((long) batchSize, range.endExclusive() - cursor);
            OperationResult result;
            try
            {
                result = worker.execute(cursor, count);
                if (result.attempted() != count)
                {
                    throw new IllegalStateException("worker reported " + result.attempted()
                            + " attempted operations for requested group of " + count);
                }
            }
            catch (Exception e)
            {
                long expectedCalls = Math.max(0L, worker.expectedApiCalls(count));
                result = OperationResult.failure(count, expectedCalls);
                firstError.compareAndSet(null, describe(e));
                if (failFast)
                {
                    abort.set(true);
                }
            }
            long elapsed = Math.max(1L, System.nanoTime() - before);

            statistics.attempted += result.attempted();
            statistics.successful += result.successful();
            statistics.errors += result.errors();
            statistics.apiCalls += result.apiCalls();
            if (result.attempted() > 0)
            {
                statistics.logicalLatency.recordValueWithCount(
                        clamp(elapsed / result.attempted()), result.attempted());
            }
            if (result.apiCalls() > 0)
            {
                statistics.apiLatency.recordValueWithCount(
                        clamp(elapsed / result.apiCalls()), result.apiCalls());
            }
            if (failFast && result.errors() > 0)
            {
                abort.set(true);
            }
            cursor += count;
        }
        return statistics;
    }

    private static void closeWorkers(List<BenchmarkWorker> workers) throws Exception
    {
        Exception first = null;
        for (BenchmarkWorker worker : workers)
        {
            try
            {
                worker.close();
            }
            catch (Exception e)
            {
                if (first == null)
                {
                    first = e;
                }
                else
                {
                    first.addSuppressed(e);
                }
            }
        }
        if (first != null)
        {
            throw first;
        }
    }

    private static Histogram histogram()
    {
        return new Histogram(1L, HIGHEST_TRACKABLE_NANOS, 3);
    }

    private static long clamp(long nanos)
    {
        return Math.max(1L, Math.min(HIGHEST_TRACKABLE_NANOS, nanos));
    }

    private static double micros(double nanos)
    {
        return nanos / 1_000D;
    }

    private static long saturatingAdd(long left, long right)
    {
        long result = left + right;
        return result < left ? Long.MAX_VALUE : result;
    }

    private static String describe(Exception error)
    {
        return error.getClass().getName() + ": " + String.valueOf(error.getMessage());
    }

    private static final class PhaseClock
    {
        private long startNanos;
        private long deadlineNanos;
    }

    private static final class WorkerStatistics
    {
        private long attempted;
        private long successful;
        private long errors;
        private long apiCalls;
        private final Histogram logicalLatency = histogram();
        private final Histogram apiLatency = histogram();
    }

    private static final class BenchmarkThreadFactory implements ThreadFactory
    {
        private final AtomicInteger ids = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable)
        {
            Thread thread = new Thread(runnable, "retina-benchmark-worker-" + ids.getAndIncrement());
            thread.setDaemon(false);
            return thread;
        }
    }
}
