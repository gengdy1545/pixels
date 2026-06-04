/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the Affero
 * GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.retina;

import io.pixelsdb.pixels.index.IndexProto;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class StorageGcJournalTask
{
    private static final int SERIALIZATION_VERSION = 2;

    public enum State
    {
        INDEX_SWITCHING,
        SWAPPED_NOT_CHECKPOINTED,
        CHECKPOINTED,
        ABORTED;

        public boolean isPending()
        {
            return this == INDEX_SWITCHING || this == SWAPPED_NOT_CHECKPOINTED;
        }

        public boolean isTerminal()
        {
            return this == CHECKPOINTED || this == ABORTED;
        }

        public boolean canTransitionTo(State target)
        {
            if (target == null)
            {
                return false;
            }
            if (this == target)
            {
                return true;
            }
            switch (this)
            {
                case INDEX_SWITCHING:
                    return target == SWAPPED_NOT_CHECKPOINTED || target == ABORTED;
                case SWAPPED_NOT_CHECKPOINTED:
                    return target == CHECKPOINTED || target == ABORTED;
                case CHECKPOINTED:
                case ABORTED:
                    return false;
                default:
                    return false;
            }
        }
    }

    public static final class RollbackEntry
    {
        private final IndexProto.IndexKey indexKey;
        private final long oldRowId;
        private final long newRowId;
        private final boolean updated;

        public RollbackEntry(IndexProto.IndexKey indexKey, long oldRowId, long newRowId, boolean updated)
        {
            this.indexKey = Objects.requireNonNull(indexKey, "indexKey");
            this.oldRowId = oldRowId;
            this.newRowId = newRowId;
            this.updated = updated;
        }

        public IndexProto.IndexKey getIndexKey()
        {
            return indexKey;
        }

        public long getOldRowId()
        {
            return oldRowId;
        }

        public long getNewRowId()
        {
            return newRowId;
        }

        public boolean isUpdated()
        {
            return updated;
        }

        public RollbackEntry markUpdated()
        {
            return updated ? this : new RollbackEntry(indexKey, oldRowId, newRowId, true);
        }

        public io.pixelsdb.pixels.common.index.RollbackEntry toIndexRollbackEntry()
        {
            return new io.pixelsdb.pixels.common.index.RollbackEntry(indexKey, oldRowId, newRowId);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (!(o instanceof RollbackEntry))
            {
                return false;
            }
            RollbackEntry that = (RollbackEntry) o;
            return oldRowId == that.oldRowId
                    && newRowId == that.newRowId
                    && updated == that.updated
                    && indexKey.equals(that.indexKey);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(indexKey, oldRowId, newRowId, updated);
        }
    }

    private final String taskId;
    private final long tableId;
    private final int virtualNodeId;
    private final List<Long> oldFileIds;
    private final long newFileId;
    private final String newFilePath;
    private final long newRowIdStart;
    private final int newRowCount;
    private final List<RollbackEntry> rollbackEntries;
    private final State state;

    private StorageGcJournalTask(String taskId, long tableId, int virtualNodeId,
                                 List<Long> oldFileIds, long newFileId, String newFilePath,
                                 long newRowIdStart, int newRowCount,
                                 List<RollbackEntry> rollbackEntries,
                                 State state)
    {
        this.taskId = requireTaskId(taskId);
        this.tableId = tableId;
        this.virtualNodeId = virtualNodeId;
        this.oldFileIds = immutableLongList(oldFileIds, "oldFileIds");
        this.newFileId = newFileId;
        this.newFilePath = newFilePath == null ? "" : newFilePath;
        this.newRowIdStart = newRowIdStart;
        this.newRowCount = newRowCount;
        this.rollbackEntries = immutableRollbackEntries(rollbackEntries);
        this.state = Objects.requireNonNull(state, "state");
    }

    public static StorageGcJournalTask create(String taskId, long tableId, int virtualNodeId,
                                              List<Long> oldFileIds, long newFileId,
                                              long newRowIdStart, int newRowCount)
    {
        return create(taskId, tableId, virtualNodeId, oldFileIds, newFileId, "",
                newRowIdStart, newRowCount);
    }

    public static StorageGcJournalTask create(String taskId, long tableId, int virtualNodeId,
                                              List<Long> oldFileIds, long newFileId, String newFilePath,
                                              long newRowIdStart, int newRowCount)
    {
        return new StorageGcJournalTask(taskId, tableId, virtualNodeId, oldFileIds,
                newFileId, newFilePath, newRowIdStart, newRowCount, Collections.emptyList(),
                State.INDEX_SWITCHING);
    }

    public StorageGcJournalTask withRollbackEntry(RollbackEntry entry)
    {
        Objects.requireNonNull(entry, "entry");
        List<RollbackEntry> entries = new ArrayList<>(rollbackEntries);
        entries.add(entry);
        return copyWith(entries, state);
    }

    public StorageGcJournalTask markRollbackEntryUpdated(IndexProto.IndexKey indexKey, long newRowId)
    {
        Objects.requireNonNull(indexKey, "indexKey");
        List<RollbackEntry> entries = new ArrayList<>(rollbackEntries.size());
        boolean found = false;
        for (RollbackEntry entry : rollbackEntries)
        {
            if (entry.getNewRowId() == newRowId && entry.getIndexKey().equals(indexKey))
            {
                entries.add(entry.markUpdated());
                found = true;
            }
            else
            {
                entries.add(entry);
            }
        }
        if (!found)
        {
            throw new IllegalArgumentException("Rollback entry not found for taskId=" + taskId);
        }
        return copyWith(entries, state);
    }

    public StorageGcJournalTask transitionTo(State target)
    {
        Objects.requireNonNull(target, "target");
        if (!state.canTransitionTo(target))
        {
            throw new IllegalStateException("Invalid Storage GC journal transition: "
                    + state + " -> " + target + " for taskId=" + taskId);
        }
        if (state == target)
        {
            return this;
        }
        return copyWith(rollbackEntries, target);
    }

    public byte[] toBytes()
    {
        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeInt(SERIALIZATION_VERSION);
            out.writeUTF(taskId);
            out.writeLong(tableId);
            out.writeInt(virtualNodeId);
            out.writeInt(oldFileIds.size());
            for (Long oldFileId : oldFileIds)
            {
                out.writeLong(oldFileId);
            }
            out.writeLong(newFileId);
            out.writeUTF(newFilePath);
            out.writeLong(newRowIdStart);
            out.writeInt(newRowCount);
            out.writeUTF(state.name());
            out.writeInt(rollbackEntries.size());
            for (RollbackEntry entry : rollbackEntries)
            {
                byte[] keyBytes = entry.getIndexKey().toByteArray();
                out.writeInt(keyBytes.length);
                out.write(keyBytes);
                out.writeLong(entry.getOldRowId());
                out.writeLong(entry.getNewRowId());
                out.writeBoolean(entry.isUpdated());
            }
            out.flush();
            return baos.toByteArray();
        }
        catch (IOException e)
        {
            throw new IllegalStateException("Failed to serialize Storage GC journal task", e);
        }
    }

    public static StorageGcJournalTask fromBytes(byte[] bytes)
    {
        Objects.requireNonNull(bytes, "bytes");
        try
        {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            int version = in.readInt();
            if (version != 1 && version != SERIALIZATION_VERSION)
            {
                throw new IllegalArgumentException("Unsupported Storage GC journal version: " + version);
            }
            String taskId = in.readUTF();
            long tableId = in.readLong();
            int virtualNodeId = in.readInt();
            int oldFileCount = in.readInt();
            List<Long> oldFileIds = new ArrayList<>(oldFileCount);
            for (int i = 0; i < oldFileCount; i++)
            {
                oldFileIds.add(in.readLong());
            }
            long newFileId = in.readLong();
            String newFilePath = version >= 2 ? in.readUTF() : "";
            long newRowIdStart = in.readLong();
            int newRowCount = in.readInt();
            State state = State.valueOf(in.readUTF());
            int entryCount = in.readInt();
            List<RollbackEntry> entries = new ArrayList<>(entryCount);
            for (int i = 0; i < entryCount; i++)
            {
                int keySize = in.readInt();
                byte[] keyBytes = new byte[keySize];
                in.readFully(keyBytes);
                IndexProto.IndexKey indexKey = IndexProto.IndexKey.parseFrom(keyBytes);
                long oldRowId = in.readLong();
                long newRowId = in.readLong();
                boolean updated = in.readBoolean();
                entries.add(new RollbackEntry(indexKey, oldRowId, newRowId, updated));
            }
            return new StorageGcJournalTask(taskId, tableId, virtualNodeId, oldFileIds,
                    newFileId, newFilePath, newRowIdStart, newRowCount, entries, state);
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException("Failed to deserialize Storage GC journal task", e);
        }
    }

    private StorageGcJournalTask copyWith(List<RollbackEntry> entries, State state)
    {
        return new StorageGcJournalTask(taskId, tableId, virtualNodeId, oldFileIds,
                newFileId, newFilePath, newRowIdStart, newRowCount, entries, state);
    }

    private static String requireTaskId(String taskId)
    {
        if (taskId == null || taskId.trim().isEmpty())
        {
            throw new IllegalArgumentException("taskId must not be blank");
        }
        return taskId;
    }

    private static List<Long> immutableLongList(List<Long> values, String name)
    {
        Objects.requireNonNull(values, name);
        for (Long value : values)
        {
            Objects.requireNonNull(value, name + " contains null");
        }
        return Collections.unmodifiableList(new ArrayList<>(values));
    }

    private static List<RollbackEntry> immutableRollbackEntries(List<RollbackEntry> entries)
    {
        Objects.requireNonNull(entries, "rollbackEntries");
        for (RollbackEntry entry : entries)
        {
            Objects.requireNonNull(entry, "rollbackEntries contains null");
        }
        return Collections.unmodifiableList(new ArrayList<>(entries));
    }

    public String getTaskId()
    {
        return taskId;
    }

    public long getTableId()
    {
        return tableId;
    }

    public int getVirtualNodeId()
    {
        return virtualNodeId;
    }

    public List<Long> getOldFileIds()
    {
        return oldFileIds;
    }

    public long getNewFileId()
    {
        return newFileId;
    }

    public String getNewFilePath()
    {
        return newFilePath;
    }

    public long getNewRowIdStart()
    {
        return newRowIdStart;
    }

    public int getNewRowCount()
    {
        return newRowCount;
    }

    public List<RollbackEntry> getRollbackEntries()
    {
        return rollbackEntries;
    }

    public State getState()
    {
        return state;
    }

    public boolean isPendingForFile(long fileId)
    {
        return state.isPending() && (newFileId == fileId || oldFileIds.contains(fileId));
    }
}
