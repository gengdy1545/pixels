/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.common.index;

import io.pixelsdb.pixels.common.exception.RowIdException;
import io.pixelsdb.pixels.common.lock.EtcdAutoIncrement;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.common.exception.EtcdException;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author hank, Rolland1944
 * @create 2025-02-19
 */
public class MainIndexImpl implements MainIndex
{
    private static final Logger logger = LogManager.getLogger(MainIndexImpl.class);
    // Cache for storing generated rowIds
    private final Queue<Long> rowIdCache = new ConcurrentLinkedQueue<>();
    // Assumed batch size for generating rowIds
    private static final int BATCH_SIZE = 1;
    // The etcd auto-increment key
    private static final String ROW_ID_KEY = "rowId/auto";
    // Get the singleton instance of EtcdUtil
    EtcdUtil etcdUtil = EtcdUtil.Instance();
    // Read-Write lock
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    // Dirty flag
    private boolean dirty = false;

    public MainIndexImpl()
    {
        try
        {
            EtcdAutoIncrement.InitId(ROW_ID_KEY);  // 初始化 auto-increment ID
        }
        catch (EtcdException e)
        {
            throw new RuntimeException("Failed to initialize auto-increment ID in etcd", e);
        }
    }

    public static class Entry
    {
        private final RowIdRange rowIdRange;
        private final RgLocation rgLocation;

        public Entry(RowIdRange rowIdRange, RgLocation rgLocation)
        {
            this.rowIdRange = rowIdRange;
            this.rgLocation = rgLocation;
        }

        public RowIdRange getRowIdRange()
        {
            return rowIdRange;
        }

        public RgLocation getRgLocation()
        {
            return rgLocation;
        }
    }

    private final List<Entry> entries = new ArrayList<>();

    @Override
    public IndexProto.RowLocation getLocation(long rowId)
    {
        // Use binary search to find the Entry containing the rowId
        int index = binarySearch(rowId);
        if (index >= 0) {
            Entry entry = entries.get(index);
            RgLocation rgLocation = entry.getRgLocation();
            return IndexProto.RowLocation.newBuilder()
                    .setFileId(rgLocation.getFileId())
                    .setRgId(rgLocation.getRowGroupId())
                    .setRgRowId((int) (rowId - entry.getRowIdRange().getStartRowId())) // Calculate the offset within the row group
                    .build();
        }
        return null; // Return null if not found
    }

    @Override
    public boolean putRowId(long rowId, IndexProto.RowLocation rowLocation)
    {
        RowIdRange newRange = new RowIdRange(rowId, rowId);
        RgLocation rgLocation = new RgLocation(rowLocation.getFileId(), rowLocation.getRgId());
        return putRowIdsOfRg(newRange, rgLocation);
    }

    @Override
    public  boolean deleteRowId(long rowId)
    {
        int index = binarySearch(rowId);
        if (index < 0) {
            logger.error("Delete failure: RowId {} not found", rowId);
            return false;
        }

        Entry entry = entries.get(index);
        RowIdRange original = entry.getRowIdRange();

        long start = original.getStartRowId();
        long end = original.getEndRowId();
        // lock
        rwLock.writeLock().lock();
        entries.remove(index);
        // In-place insert the remaining entries
        if (rowId > start)
        {
            entries.add(index, new Entry(new RowIdRange(start, rowId - 1), entry.getRgLocation()));
            index++;
        }
        if (rowId < end)
        {
            entries.add(index, new Entry(new RowIdRange(rowId + 1, end), entry.getRgLocation()));
        }
        rwLock.writeLock().unlock();
        dirty = true;
        return true;
    }

    @Override
    public boolean putRowIdsOfRg(RowIdRange rowIdRangeOfRg, RgLocation rgLocation)
    {
        long start = rowIdRangeOfRg.getStartRowId();
        long end = rowIdRangeOfRg.getEndRowId();
        if (start > end)
        {
            logger.error("Invalid RowIdRange: startRowId {} > endRowId {}", start, end);
            return false;
        }

        // Check whether it conflicts with the last entry.
        if (!entries.isEmpty())
        {
            RowIdRange lastRange = entries.get(entries.size() - 1).getRowIdRange();
            if (start <= lastRange.getEndRowId())
            {
                logger.error("Insert failure: RowIdRange [{}-{}] overlaps with previous [{}-{}]",
                        start, end, lastRange.getStartRowId(), lastRange.getEndRowId());
                return false;
            }
        }
        rwLock.writeLock().lock();
        entries.add(new Entry(rowIdRangeOfRg, rgLocation));
        rwLock.writeLock().unlock();
        dirty = true;
        return true;
    }

    @Override
    public boolean deleteRowIdRange(RowIdRange targetRange)
    {
        int index = binarySearch(targetRange.getStartRowId());
        if (index < 0)
        {
            logger.error("Delete failure: RowIdRange [{}-{}] not found", targetRange.getStartRowId(), targetRange.getEndRowId());
            return false;
        }

        Entry entry = entries.get(index);
        RowIdRange existingRange = entry.getRowIdRange();

        if (existingRange.getStartRowId() == targetRange.getStartRowId() && existingRange.getEndRowId() == targetRange.getEndRowId())
        {
            rwLock.writeLock().lock();
            entries.remove(index);
            rwLock.writeLock().unlock();
            dirty = true;
            return true;
        }
        else
        {
            logger.error("Delete failure: RowIdRange [{}-{}] does not exactly match existing range [{}-{}]",
                    targetRange.getStartRowId(), targetRange.getEndRowId(),
                    existingRange.getStartRowId(), existingRange.getEndRowId());
            return false;
        }

    }

    @Override
    public boolean getRowId(SinglePointIndex.Entry entry) throws RowIdException
    {
        ensureRowIdsAvailable(1);
        Long rowId = rowIdCache.poll();
        if (rowId == null)
        {
            logger.error("Failed to generate rowId");
            throw new RowIdException("Failed to generate single row id");
        }
        entry.setRowId(rowId);
        return true;
    }

    @Override
    public boolean getRgOfRowIds(List<SinglePointIndex.Entry> entries) throws RowIdException
    {
        int needed = entries.size();
        ensureRowIdsAvailable(needed);

        for (SinglePointIndex.Entry entry : entries)
        {
            Long rowId = rowIdCache.poll();
            if (rowId == null)
            {
                logger.error("Insufficient rowIds available in cache");
                throw new RowIdException("Failed to generate single row id");
            }
            entry.setRowId(rowId);
        }
        return true;
    }

    @Override
    public boolean persist()
    {
        try
        {
            // Iterate through entries and persist each to etcd
            for (Entry entry : entries)
            {
                String key = "/mainindex/" + entry.getRowIdRange().getStartRowId();
                String value = serializeEntry(entry); // Serialize Entry to string
                etcdUtil.putKeyValue(key, value);
            }
            logger.info("Persisted {} entries to etcd", entries.size());
            return true;
        }
        catch (Exception e)
        {
            logger.error("Failed to persist entries to etcd", e);
            return false;
        }
    }

    public boolean persistIfDirty()
    {
        if (dirty)
        {
            if (persist())
            {
                dirty = false; // Reset dirty flag
                return true;
            }
            return false;
        }
        return true; // No changes, no need to persist
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            // Check dirty flag and persist to etcd if true
            if (!persistIfDirty())
            {
                logger.error("Failed to persist data to etcd before closing");
                throw new IOException("Failed to persist data to etcd before closing");
            }
            logger.info("Data persisted to etcd successfully before closing");
        }
        catch (Exception e)
        {
            logger.error("Error occurred while closing MainIndexImpl", e);
            throw new IOException("Error occurred while closing MainIndexImpl", e);
        }
    }

    private int binarySearch(long rowId)
    {
        int low = 0;
        int high = entries.size() - 1;

        while (low <= high)
        {
            int mid = (low + high) >>> 1;
            Entry entry = entries.get(mid);
            RowIdRange range = entry.getRowIdRange();

            if (rowId >= range.getStartRowId() && rowId <= range.getEndRowId())
            {
                return mid; // Found the containing Entry
            }
            else if (rowId < range.getStartRowId())
            {
                high = mid - 1;
            }
            else
            {
                low = mid + 1;
            }
        }

        return -1; // Not found
    }

    // Check if two RowIdRanges overlap
    private boolean isOverlapping(RowIdRange range1, RowIdRange range2)
    {
        return range1.getStartRowId() <= range2.getEndRowId() && range1.getEndRowId() >= range2.getStartRowId();
    }

    // Check if RowIdRange overlaps with existing ranges
    private boolean isOverlapping(RowIdRange newRange)
    {
        for (Entry entry : entries)
        {
            if (isOverlapping(entry.getRowIdRange(), newRange))
            {
                return true;
            }
        }
        return false;
    }

    // Ensures the rowId cache contains at least `requiredCount` IDs
    // If not, load BATCH_SIZE number of rowIds from etcd
    private void ensureRowIdsAvailable(int requiredCount) throws RowIdException
    {
        if (rowIdCache.size() >= requiredCount)
        {
            return;
        }
        // Lock
        synchronized (this)
        {
            // Double-check locking
            if (rowIdCache.size() >= requiredCount)
            {
                return;
            }
            try
            {
                long step = Math.max(BATCH_SIZE, requiredCount);
                EtcdAutoIncrement.Segment segment = EtcdAutoIncrement.GenerateId(ROW_ID_KEY, step);
                long start = segment.getStart();
                long end = start + segment.getLength();

                for (long i = start; i < end; i++)
                {
                    rowIdCache.add(i);
                }

                logger.info("Generated {} new rowIds ({} ~ {})", segment.getLength(), start, end - 1);
            }
            catch (EtcdException e)
            {
                logger.error("Failed to generate rowIds from EtcdAutoIncrement", e);
                throw new RowIdException("Failed to generate rowIds from EtcdAutoIncrement", e);
            }
        }
    }


    // Serialize Entry
    private String serializeEntry(Entry entry)
    {
        return String.format("{\"startRowId\": %d, \"endRowId\": %d, \"fieldId\": %d, \"rowGroupId\": %d}",
                entry.getRowIdRange().getStartRowId(),
                entry.getRowIdRange().getEndRowId(),
                entry.getRgLocation().getFileId(),
                entry.getRgLocation().getRowGroupId());
    }
}