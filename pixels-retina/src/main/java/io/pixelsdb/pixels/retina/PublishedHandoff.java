/*
 * Copyright 2026 PixelsDB.
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

import io.pixelsdb.pixels.common.exception.RetinaException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Authoritative file-to-buffer handoff state for one published ingest file.
 *
 * <p>The retained entries keep the owner references transferred out of the
 * write buffer. They are released only after the placement safe boundary has
 * passed {@link #fence}.</p>
 */
public final class PublishedHandoff
{
    public enum State
    {
        PREPARING,
        FENCED,
        RETIRING,
        CANCELLED
    }

    static final long UNASSIGNED_FENCE = -1L;

    private final long fileId;
    private final long tableId;
    private final int virtualNodeId;
    private final long firstBlockId;
    private final long lastBlockId;

    private State state = State.PREPARING;
    private boolean metadataPublished;
    private long fence = UNASSIGNED_FENCE;
    private boolean entriesAttached;
    private List<ObjectEntry> retainedEntries = Collections.emptyList();
    private int pendingDeletes;

    PublishedHandoff(long fileId, long tableId, int virtualNodeId,
                     long firstBlockId, long lastBlockId)
    {
        this.fileId = fileId;
        this.tableId = tableId;
        this.virtualNodeId = virtualNodeId;
        this.firstBlockId = firstBlockId;
        this.lastBlockId = lastBlockId;
    }

    public long getFileId()
    {
        return fileId;
    }

    public long getTableId()
    {
        return tableId;
    }

    public int getVirtualNodeId()
    {
        return virtualNodeId;
    }

    public long getFirstBlockId()
    {
        return firstBlockId;
    }

    public long getLastBlockId()
    {
        return lastBlockId;
    }

    public synchronized State getState()
    {
        return state;
    }

    synchronized void verifyIdentity(long expectedTableId, int expectedVirtualNodeId,
                                     long expectedFirstBlockId, long expectedLastBlockId)
            throws RetinaException
    {
        if (this.tableId != expectedTableId
                || this.virtualNodeId != expectedVirtualNodeId
                || this.firstBlockId != expectedFirstBlockId
                || this.lastBlockId != expectedLastBlockId)
        {
            throw new RetinaException("Conflicting handoff identity for fileId=" + fileId);
        }
    }

    synchronized void prepareRetry() throws RetinaException
    {
        if (state == State.CANCELLED)
        {
            if (entriesAttached || !retainedEntries.isEmpty())
            {
                throw new RetinaException("Cancelled handoff retained blocks for fileId=" + fileId);
            }
            metadataPublished = false;
            fence = UNASSIGNED_FENCE;
            pendingDeletes = 0;
            state = State.PREPARING;
            notifyAll();
        }
    }

    synchronized boolean isMetadataPublished()
    {
        return metadataPublished;
    }

    synchronized void markMetadataPublished() throws RetinaException
    {
        requireState(State.PREPARING);
        metadataPublished = true;
    }

    synchronized long getFence()
    {
        return fence;
    }

    synchronized void assignFence(long placementFence) throws RetinaException
    {
        requireState(State.PREPARING);
        if (!metadataPublished)
        {
            throw new RetinaException("Cannot fence unpublished metadata for fileId=" + fileId);
        }
        if (placementFence < 0)
        {
            throw new RetinaException("Invalid placement fence " + placementFence + " for fileId=" + fileId);
        }
        if (fence != UNASSIGNED_FENCE && fence != placementFence)
        {
            throw new RetinaException("Placement fence already assigned for fileId=" + fileId);
        }
        fence = placementFence;
    }

    synchronized boolean hasRetainedEntries()
    {
        return entriesAttached;
    }

    synchronized void validateRetainedEntries(List<ObjectEntry> entries) throws RetinaException
    {
        if (entriesAttached)
        {
            throw new RetinaException("Retained entries already attached for fileId=" + fileId);
        }

        long expectedCount = lastBlockId < firstBlockId ? 0L : lastBlockId - firstBlockId + 1L;
        if (entries.size() != expectedCount)
        {
            throw new RetinaException("Incomplete handoff blocks for fileId=" + fileId
                    + ": expected=" + expectedCount + ", actual=" + entries.size());
        }

        Set<Long> ids = new HashSet<>();
        for (ObjectEntry entry : entries)
        {
            if (entry.getFileId() != fileId
                    || entry.getId() < firstBlockId
                    || entry.getId() > lastBlockId
                    || !ids.add(entry.getId()))
            {
                throw new RetinaException("Invalid retained block " + entry.getId()
                        + " for fileId=" + fileId);
            }
        }
    }

    synchronized void attachRetainedEntries(List<ObjectEntry> entries) throws RetinaException
    {
        requireState(State.PREPARING);
        validateRetainedEntries(entries);
        List<ObjectEntry> ordered = new ArrayList<>(entries);
        ordered.sort(Comparator.comparingLong(ObjectEntry::getId));
        retainedEntries = Collections.unmodifiableList(ordered);
        entriesAttached = true;
    }

    synchronized List<ObjectEntry> getRetainedEntries()
    {
        return retainedEntries;
    }

    synchronized void publishFenced() throws RetinaException
    {
        requireState(State.PREPARING);
        if (!metadataPublished || fence == UNASSIGNED_FENCE || !entriesAttached)
        {
            throw new RetinaException("Incomplete handoff publication for fileId=" + fileId);
        }
        state = State.FENCED;
        notifyAll();
    }

    synchronized void cancel() throws RetinaException
    {
        requireState(State.PREPARING);
        if (entriesAttached)
        {
            throw new RetinaException("Cannot cancel handoff after block transfer for fileId=" + fileId);
        }
        state = State.CANCELLED;
        notifyAll();
    }

    synchronized State awaitStableState(long timeoutMillis) throws RetinaException
    {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (state == State.PREPARING)
        {
            long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0)
            {
                throw new RetinaException("Timed out waiting for handoff fileId=" + fileId);
            }
            long waitMillis = Math.max(1L, remainingNanos / 1_000_000L);
            try
            {
                wait(waitMillis);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new RetinaException("Interrupted while waiting for handoff fileId=" + fileId, e);
            }
        }
        return state;
    }

    synchronized RetinaProto.ReadSource resolveSource(long transId) throws RetinaException
    {
        if (state == State.PREPARING)
        {
            throw new RetinaException("Handoff is still preparing for fileId=" + fileId);
        }
        if (state == State.CANCELLED)
        {
            return RetinaProto.ReadSource.BUFFER;
        }
        if (state == State.RETIRING)
        {
            return RetinaProto.ReadSource.FILE;
        }
        if (transId == fence)
        {
            throw new RetinaException("Transaction id equals reserved placement fence " + fence);
        }
        return transId < fence ? RetinaProto.ReadSource.BUFFER : RetinaProto.ReadSource.FILE;
    }

    synchronized List<ObjectEntry> beginRetiring() throws RetinaException
    {
        requireState(State.FENCED);
        state = State.RETIRING;
        pendingDeletes = retainedEntries.size();
        notifyAll();
        return retainedEntries;
    }

    synchronized boolean markObjectDeleted() throws RetinaException
    {
        requireState(State.RETIRING);
        if (pendingDeletes <= 0)
        {
            throw new RetinaException("No pending object deletion for fileId=" + fileId);
        }
        pendingDeletes--;
        return pendingDeletes == 0;
    }

    synchronized boolean hasPendingDeletes()
    {
        return pendingDeletes > 0;
    }

    private void requireState(State expected) throws RetinaException
    {
        if (state != expected)
        {
            throw new RetinaException("Expected handoff state " + expected + " for fileId="
                    + fileId + ", actual=" + state);
        }
    }
}
