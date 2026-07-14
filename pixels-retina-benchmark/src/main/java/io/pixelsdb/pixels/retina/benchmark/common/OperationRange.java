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

/** A half-open range of absolute logical-operation indexes. */
public final class OperationRange
{
    private final long startInclusive;
    private final long endExclusive;

    public OperationRange(long startInclusive, long endExclusive)
    {
        if (startInclusive < 0 || endExclusive < startInclusive)
        {
            throw new IllegalArgumentException("invalid operation range [" + startInclusive + ", " + endExclusive + ")");
        }
        this.startInclusive = startInclusive;
        this.endExclusive = endExclusive;
    }

    public long startInclusive() { return startInclusive; }
    public long endExclusive() { return endExclusive; }
    public long size() { return endExclusive - startInclusive; }
    public long getStartInclusive() { return startInclusive; }
    public long getEndExclusive() { return endExclusive; }

    public static OperationRange partition(long totalOperations, int partitionCount, int partitionId)
    {
        if (totalOperations < 0 || partitionCount <= 0 || partitionId < 0 || partitionId >= partitionCount)
        {
            throw new IllegalArgumentException("invalid partition arguments");
        }
        long base = totalOperations / partitionCount;
        long remainder = totalOperations % partitionCount;
        long start = partitionId * base + Math.min((long) partitionId, remainder);
        long length = base + (partitionId < remainder ? 1 : 0);
        return new OperationRange(start, start + length);
    }
}
