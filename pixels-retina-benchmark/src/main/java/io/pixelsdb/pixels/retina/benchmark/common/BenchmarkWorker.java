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

/**
 * Per-thread benchmark state. {@link #prepare} is called outside the measured
 * interval so requests, payloads, and lookup tables can be pre-generated.
 */
public interface BenchmarkWorker extends AutoCloseable
{
    void prepare(long firstOperation, long operationCount, int batchSize) throws Exception;

    OperationResult execute(long firstOperation, int logicalOperationCount) throws Exception;

    /** Used only if execute throws before it can return precise call counts. */
    default long expectedApiCalls(int logicalOperationCount)
    {
        return 1;
    }

    @Override
    default void close() throws Exception
    {
    }
}
