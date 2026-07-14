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
package io.pixelsdb.pixels.retina.benchmark.snapshot;

/** Binary sample DTOs shared by the exporter and restored benchmarks. */
public final class SnapshotSamples
{
    private SnapshotSamples()
    {
    }

    public static final class IndexSample
    {
        public final byte[] key;
        public final long createTimestamp;
        public final long fileId;
        public final int rgId;
        public final int rgRowOffset;
        public final int bucketId;

        public IndexSample(byte[] key, long createTimestamp, long fileId,
                           int rgId, int rgRowOffset, int bucketId)
        {
            this.key = key;
            this.createTimestamp = createTimestamp;
            this.fileId = fileId;
            this.rgId = rgId;
            this.rgRowOffset = rgRowOffset;
            this.bucketId = bucketId;
        }
    }

    public static final class RowSample
    {
        public final byte[][] columns;

        public RowSample(byte[][] columns)
        {
            this.columns = columns;
        }
    }
}
