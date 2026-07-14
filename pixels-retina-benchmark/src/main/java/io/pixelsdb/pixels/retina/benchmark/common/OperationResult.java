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

/** Counts produced by one timed invocation of a prepared benchmark worker. */
public final class OperationResult
{
    private final long attempted;
    private final long successful;
    private final long errors;
    private final long apiCalls;

    public OperationResult(long attempted, long successful, long errors, long apiCalls)
    {
        if (attempted < 0 || successful < 0 || errors < 0 || apiCalls < 0 || successful + errors != attempted)
        {
            throw new IllegalArgumentException("invalid operation result counts");
        }
        this.attempted = attempted;
        this.successful = successful;
        this.errors = errors;
        this.apiCalls = apiCalls;
    }

    public static OperationResult success(long logicalOperations, long apiCalls)
    {
        return new OperationResult(logicalOperations, logicalOperations, 0, apiCalls);
    }

    public static OperationResult failure(long logicalOperations, long apiCalls)
    {
        return new OperationResult(logicalOperations, 0, logicalOperations, apiCalls);
    }

    public long attempted() { return attempted; }
    public long successful() { return successful; }
    public long errors() { return errors; }
    public long apiCalls() { return apiCalls; }
    public long getAttempted() { return attempted; }
    public long getSuccessful() { return successful; }
    public long getErrors() { return errors; }
    public long getApiCalls() { return apiCalls; }
}
