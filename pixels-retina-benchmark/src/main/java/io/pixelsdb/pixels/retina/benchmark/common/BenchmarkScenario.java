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
import java.util.Map;

/** Lifecycle and worker factory implemented by each independent Retina benchmark. */
public interface BenchmarkScenario extends AutoCloseable
{
    String name();

    void setup(BenchmarkConfig config) throws Exception;

    default void preparePhase(BenchmarkPhase phase, long totalOperations) throws Exception
    {
    }

    BenchmarkWorker createWorker(BenchmarkPhase phase, int workerId, int clientId,
                                 OperationRange range) throws Exception;

    default void completePreparation(BenchmarkPhase phase) throws Exception
    {
    }

    default void completePhase(BenchmarkPhase phase, BenchmarkResult result) throws Exception
    {
    }

    default Map<String, String> details()
    {
        return Collections.emptyMap();
    }

    @Override
    default void close() throws Exception
    {
    }
}
