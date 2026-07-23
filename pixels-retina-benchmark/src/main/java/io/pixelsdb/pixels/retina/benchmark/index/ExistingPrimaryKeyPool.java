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
package io.pixelsdb.pixels.retina.benchmark.index;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.utils.IndexUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-bucket pool of existing HyBench primary keys (4-byte big-endian ints).
 *
 * <p>HyBench primary keys are consecutive integers in a table-specific closed
 * range {@code [firstKey, lastKey]} (for example company starts after customer).
 * This pool materializes enough keys in each Index bucket for Update (sample
 * with replacement) and Delete (unique claim).</p>
 */
final class ExistingPrimaryKeyPool
{
    private final ByteString[][] keysByBucket;
    private final AtomicInteger[] claimCursors;
    private final long firstKey;
    private final long lastKey;
    private final long totalKeys;

    private ExistingPrimaryKeyPool(ByteString[][] keysByBucket, long firstKey, long lastKey)
    {
        this.keysByBucket = keysByBucket;
        this.claimCursors = new AtomicInteger[keysByBucket.length];
        long total = 0L;
        for (int i = 0; i < keysByBucket.length; i++)
        {
            claimCursors[i] = new AtomicInteger(0);
            total += keysByBucket[i].length;
        }
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        this.totalKeys = total;
    }

    static ExistingPrimaryKeyPool build(long firstKey, long lastKey, int[] usableBuckets,
                                        int bucketCount, long keysNeeded)
    {
        if (firstKey <= 0 || lastKey < firstKey)
        {
            throw new IllegalArgumentException("invalid HyBench key range ["
                    + firstKey + "," + lastKey + "]");
        }
        if (keysNeeded <= 0)
        {
            throw new IllegalArgumentException("keysNeeded must be positive");
        }
        if (usableBuckets == null || usableBuckets.length == 0)
        {
            throw new IllegalArgumentException("usableBuckets must not be empty");
        }
        if (lastKey > Integer.MAX_VALUE)
        {
            throw new IllegalArgumentException("HyBench int key space exceeds Integer.MAX_VALUE");
        }

        long keySpaceSize = lastKey - firstKey + 1L;
        long perBucketTarget = Math.max(1L,
                (keysNeeded + usableBuckets.length - 1L) / usableBuckets.length);
        // Slack for uneven SHA-256 bucket assignment.
        perBucketTarget = Math.min(keySpaceSize,
                Math.addExact(perBucketTarget, perBucketTarget / 8 + 64));
        if (perBucketTarget > Integer.MAX_VALUE)
        {
            throw new IllegalArgumentException("per-bucket key target exceeds Integer.MAX_VALUE");
        }

        List<ByteString>[] builders = newListArray(bucketCount);
        for (int bucket : usableBuckets)
        {
            if (bucket < 0 || bucket >= bucketCount)
            {
                throw new IllegalArgumentException("usable bucket out of range: " + bucket);
            }
            builders[bucket] = new ArrayList<>((int) perBucketTarget);
        }

        for (long value = firstKey; value <= lastKey; value++)
        {
            ByteString key = intKey((int) value);
            int bucket = IndexUtils.getBucketIdFromByteBuffer(key);
            List<ByteString> bucketKeys = builders[bucket];
            if (bucketKeys == null || bucketKeys.size() >= perBucketTarget)
            {
                continue;
            }
            bucketKeys.add(key);
            if (allUsableBucketsFull(builders, usableBuckets, (int) perBucketTarget))
            {
                break;
            }
        }

        ByteString[][] keysByBucket = new ByteString[bucketCount][];
        long total = 0L;
        for (int bucket = 0; bucket < bucketCount; bucket++)
        {
            List<ByteString> bucketKeys = builders[bucket];
            if (bucketKeys == null || bucketKeys.isEmpty())
            {
                keysByBucket[bucket] = new ByteString[0];
                continue;
            }
            keysByBucket[bucket] = bucketKeys.toArray(new ByteString[0]);
            total += keysByBucket[bucket].length;
        }
        if (total <= 0)
        {
            throw new IllegalStateException("failed to materialize any existing primary keys");
        }
        for (int bucket : usableBuckets)
        {
            if (keysByBucket[bucket].length == 0)
            {
                throw new IllegalStateException("no existing primary keys landed in bucket " + bucket
                        + " while scanning key space [" + firstKey + "," + lastKey + "]");
            }
        }
        return new ExistingPrimaryKeyPool(keysByBucket, firstKey, lastKey);
    }

    private static boolean allUsableBucketsFull(List<ByteString>[] builders, int[] usableBuckets,
                                                int perBucketTarget)
    {
        for (int bucket : usableBuckets)
        {
            if (builders[bucket].size() < perBucketTarget)
            {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private static List<ByteString>[] newListArray(int size)
    {
        return (List<ByteString>[]) new List<?>[size];
    }

    static ByteString intKey(int value)
    {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(value);
        return ByteString.copyFrom(buffer.array());
    }

    long firstKey()
    {
        return firstKey;
    }

    long lastKey()
    {
        return lastKey;
    }

    long keySpaceSize()
    {
        return lastKey - firstKey + 1L;
    }

    long totalKeys()
    {
        return totalKeys;
    }

    int availableInBucket(int bucket)
    {
        ByteString[] keys = keysByBucket[bucket];
        return keys == null ? 0 : keys.length;
    }

    ByteString sample(int bucket)
    {
        ByteString[] keys = requireBucket(bucket);
        int index = ThreadLocalRandom.current().nextInt(keys.length);
        return keys[index];
    }

    /**
     * Sample an existing key exclusively owned by {@code workerId}.
     *
     * <p>Update uses this so concurrent workers never race on the same primary
     * key. {@link io.pixelsdb.pixels.common.index.service.LocalIndexService}
     * updates the single-point index before inserting the new MainIndex row id;
     * two threads updating one key can make the second thread observe a row id
     * that is not yet in MainIndex.</p>
     */
    ByteString sampleForWorker(int bucket, int workerId, int workerCount)
    {
        ByteString[] keys = requireBucket(bucket);
        if (workerCount <= 1)
        {
            return sample(bucket);
        }
        if (workerId < 0 || workerId >= workerCount)
        {
            throw new IllegalArgumentException("workerId out of range: " + workerId
                    + " workerCount=" + workerCount);
        }
        int owned = 0;
        for (int i = 0; i < keys.length; i++)
        {
            if (Math.floorMod(i, workerCount) == workerId)
            {
                owned++;
            }
        }
        if (owned <= 0)
        {
            throw new IllegalStateException("worker " + workerId + " owns no existing keys in bucket "
                    + bucket + " (poolSize=" + keys.length + ", workerCount=" + workerCount + ")");
        }
        int pick = ThreadLocalRandom.current().nextInt(owned);
        for (int i = 0; i < keys.length; i++)
        {
            if (Math.floorMod(i, workerCount) != workerId)
            {
                continue;
            }
            if (pick == 0)
            {
                return keys[i];
            }
            pick--;
        }
        throw new IllegalStateException("failed to sample worker-owned key in bucket " + bucket);
    }

    ByteString claim(int bucket)
    {
        ByteString[] keys = requireBucket(bucket);
        int index = claimCursors[bucket].getAndIncrement();
        if (index >= keys.length)
        {
            throw new IllegalStateException("existing primary key pool exhausted in bucket " + bucket
                    + " (size=" + keys.length + ")");
        }
        return keys[index];
    }

    private ByteString[] requireBucket(int bucket)
    {
        if (bucket < 0 || bucket >= keysByBucket.length)
        {
            throw new IllegalArgumentException("invalid bucket: " + bucket);
        }
        ByteString[] keys = keysByBucket[bucket];
        if (keys == null || keys.length == 0)
        {
            throw new IllegalStateException("no existing primary keys materialized for bucket " + bucket);
        }
        return keys;
    }
}
