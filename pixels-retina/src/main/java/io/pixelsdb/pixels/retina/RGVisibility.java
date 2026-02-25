/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.retina;

import com.sun.jna.Platform;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is used to manage the visibility of a row-group.
 * It provides methods to add visibility, delete record, and garbage collect.
 */
public class RGVisibility implements AutoCloseable
{
    private static final Logger logger = LogManager.getLogger(RGVisibility.class);
    static
    {
        String pixelsHome = System.getenv("PIXELS_HOME");
        if (pixelsHome == null || pixelsHome.isEmpty())
        {
            throw new IllegalStateException("Environment variable PIXELS_HOME is not set");
        }

        if (!Platform.isLinux())
        {
            logger.error("Direct io is not supported on OS other than Linux");
        }
        String libPath = Paths.get(pixelsHome, "lib/libpixels-retina.so").toString();
        File libFile = new File(libPath);
        if (!libFile.exists())
        {
            throw new IllegalStateException("libpixels-retina.so not found at " + libPath);
        }
        if (!libFile.canRead())
        {
            throw new IllegalStateException("libpixels-retina.so is not readable at " + libPath);
        }
        System.load(libPath);
    }

    /**
     * Constructor creates C++ object and returns handle.
     */
    private final AtomicLong nativeHandle = new AtomicLong();
    private final long recordNum;

    /**
     * The actual number of rows in this row-group, as provided at construction time.
     * Stored here to avoid a JNI call for getTotalRowCount(), and more accurate than
     * the C++ tileCount * CAPACITY (which rounds up to tile boundaries).
     */
    private final long rgRecordNum;

    public RGVisibility(long rgRecordNum)
    {
        this.rgRecordNum = rgRecordNum;
        this.nativeHandle.set(createNativeObject(rgRecordNum));
    }

    public RGVisibility(long rgRecordNum, long timestamp, long[] initialBitmap)
    {
        this.rgRecordNum = rgRecordNum;
        if (initialBitmap == null)
        {
            this.nativeHandle.set(createNativeObject(rgRecordNum));
        } else
        {
            this.nativeHandle.set(createNativeObjectInitialized(rgRecordNum, timestamp, initialBitmap));
        }
    }

    public long getRecordNum()
    {
        return recordNum;
    }

    @Override
    public void close()
    {
        long handle = nativeHandle.getAndSet(0);
        if (handle != 0)
        {
            destroyNativeObject(handle);
        }
    }

    // native methods
    private native long createNativeObject(long rgRecordNum);
    private native long createNativeObjectInitialized(long rgRecordNum, long timestamp, long[] bitmap);
    private native void destroyNativeObject(long nativeHandle);
    private native void deleteRecord(int rgRowOffset, long timestamp, long nativeHandle);
    private native long[] getVisibilityBitmap(long timestamp, long nativeHandle);
    private native void garbageCollect(long timestamp, long nativeHandle);
    private native double getInvalidRatio(long nativeHandle);
    private native long[] exportDeletionBlocks(long nativeHandle);
    private native void prependDeletionBlocks(long[] items, long nativeHandle);
    private native long[] getBaseBitmap(long nativeHandle);
    private native long getInvalidCount(long nativeHandle);

    public void deleteRecord(int rgRowOffset, long timestamp)
    {
        long handle = nativeHandle.get();
        if (handle == 0) throw new IllegalStateException("RGVisibility is closed");
        deleteRecord(rgRowOffset, timestamp, handle);
    }

    public long[] getVisibilityBitmap(long timestamp)
    {
        long handle = this.nativeHandle.get();
        if (handle == 0)
        {
            throw new IllegalStateException("RGVisibility instance has been closed.");
        }
        long[] bitmap = getVisibilityBitmap(timestamp, handle);
        if (bitmap == null)
        {
            logger.warn("Native layer returned null bitmap for timestamp: {}", timestamp);
            return new long[0];
        }
        return bitmap;
    }

    public void garbageCollect(long timestamp)
    {
        long handle = this.nativeHandle.get();
        if (handle == 0)
        {
            throw new IllegalStateException("RGVisibility instance has been closed.");
        }

        garbageCollect(timestamp, handle);
    }

    public double getInvalidRatio()
    {
        long handle = this.nativeHandle.get();
        if (handle == 0)
        {
            throw new IllegalStateException("RGVisibility instance has been closed.");
        }
        return getInvalidRatio(handle);
    }

    /**
     * Export all deletion blocks from the deletion chain.
     * Used by Storage GC to migrate deletion history to new files.
     *
     * @return array of deletion items (rowId + timestamp packed in uint64)
     */
    public long[] exportDeletionBlocks()
    {
        long handle = this.nativeHandle.get();
        if (handle == 0)
        {
            throw new IllegalStateException("RGVisibility instance has been closed.");
        }
        return exportDeletionBlocks(handle);
    }

    /**
     * Prepend deletion blocks to the head of the deletion chain.
     * Used by Storage GC to restore deletion history in new files.
     *
     * @param items array of deletion items (rowId + timestamp packed in uint64)
     */
    public void prependDeletionBlocks(long[] items)
    {
        long handle = this.nativeHandle.get();
        if (handle == 0)
        {
            throw new IllegalStateException("RGVisibility instance has been closed.");
        }
        prependDeletionBlocks(items, handle);
    }

    /**
     * Get the base bitmap (Memory GC compacted bitmap).
     * Used by Storage GC to determine which rows can be physically deleted.
     *
     * @return base bitmap as long array
     */
    public long[] getBaseBitmap()
    {
        long handle = this.nativeHandle.get();
        if (handle == 0)
        {
            throw new IllegalStateException("RGVisibility instance has been closed.");
        }
        return getBaseBitmap(handle);
    }

    /**
     * Get the total number of invalid rows (baseBitmap 1-bits) across all tiles.
     * Used by Storage GC for accurate file-level invalid ratio calculation:
     * fileInvalidRatio = Σ(RG invalid count) / Σ(RG total row count)
     *
     * @return total invalid row count
     */
    public long getInvalidCount()
    {
        long handle = this.nativeHandle.get();
        if (handle == 0)
        {
            throw new IllegalStateException("RGVisibility instance has been closed.");
        }
        return getInvalidCount(handle);
    }

    /**
     * Get the actual number of rows in this row-group.
     * Returns the {@code rgRecordNum} stored at construction time, which is more accurate
     * than the C++ {@code tileCount * CAPACITY} (the latter rounds up to tile boundaries).
     * Used together with {@link #getInvalidCount()} to compute file-level invalid ratio:
     * {@code fileInvalidRatio = Σ(invalid count) / Σ(total row count)}
     *
     * @return actual row count of this RG
     */
    public long getTotalRowCount()
    {
        return this.rgRecordNum;
    }


}
