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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.pixelsdb.pixels.retina.RecoveryCheckpoint.PendingSegmentEntry;

/**
 * Value objects and pure helpers shared by Retina startup recovery
 * orchestration. The orchestration itself (load → cleanse → compute replay →
 * retire orphans) lives in {@code RetinaServerImpl}; this class holds the
 * recovery-result POJOs and the side-effect-free {@link #computeReplay} /
 * {@link #defaultExpectedVnodes} helpers so they remain unit-testable in isolation.
 */
public final class RecoveryProcedure
{
    private RecoveryProcedure() {}

    /**
     * Computes CDC replay start timestamps from checkpoint segment entries.
     *
     * <p>For every {@link PendingSegmentEntry} the per-scope replay start is
     * {@code min(checkpointAppliedTs, minCommitTs)}; scopes with
     * {@code minCommitTs == Long.MAX_VALUE} fall back to {@code checkpointAppliedTs}
     * (no unsafe insert pending). The result is aggregated per virtual node, and
     * any expected vnode without a {@link PendingSegmentEntry} defaults to
     * {@code checkpointAppliedTs}. The node-level replay start is the minimum
     * across all vnode entries.
     */
    public static ReplayResult computeReplay(long checkpointAppliedTs,
                                             List<PendingSegmentEntry> segmentEntries,
                                             Set<Integer> expectedVnodes)
    {
        Map<Integer, Long> vnodeReplayStarts = new HashMap<>();
        if (segmentEntries != null)
        {
            for (PendingSegmentEntry se : segmentEntries)
            {
                long ts = se.getMinCommitTs() == Long.MAX_VALUE
                        ? checkpointAppliedTs
                        : Math.min(checkpointAppliedTs, se.getMinCommitTs());
                vnodeReplayStarts.merge(se.getVirtualNodeId(), ts, Math::min);
            }
        }
        if (expectedVnodes != null)
        {
            for (Integer vnode : expectedVnodes)
            {
                vnodeReplayStarts.putIfAbsent(vnode, checkpointAppliedTs);
            }
        }

        long nodeReplayFromTs = checkpointAppliedTs;
        if (!vnodeReplayStarts.isEmpty())
        {
            nodeReplayFromTs = Long.MAX_VALUE;
            for (Long v : vnodeReplayStarts.values())
            {
                nodeReplayFromTs = Math.min(nodeReplayFromTs, v);
            }
        }
        return new ReplayResult(vnodeReplayStarts, nodeReplayFromTs);
    }

    public static Set<Integer> defaultExpectedVnodes(int nodeVirtualNum)
    {
        Set<Integer> result = new HashSet<>(nodeVirtualNum);
        for (int i = 0; i < nodeVirtualNum; i++)
        {
            result.add(i);
        }
        return result;
    }

    public static final class Outcome
    {
        public enum Kind { FRESH_DEPLOYMENT, CHECKPOINT_APPLIED }

        private final Kind kind;
        private final String checkpointId;
        private final ReplayResult replay;
        private final long checkpointAppliedTs;

        public Outcome(Kind kind, String checkpointId, ReplayResult replay, long checkpointAppliedTs)
        {
            this.kind = kind;
            this.checkpointId = checkpointId;
            this.replay = replay;
            this.checkpointAppliedTs = checkpointAppliedTs;
        }

        public Kind getKind() { return kind; }
        public String getCheckpointId() { return checkpointId; }
        public ReplayResult getReplay() { return replay; }
        public long getCheckpointAppliedTs() { return checkpointAppliedTs; }
    }

    public static final class CleanseResult
    {
        public final Set<Long> baselineVisibleFileIds;
        public final List<PendingSegmentEntry> cleansedSegmentEntries;
        public final int warnCount;

        public CleanseResult(Set<Long> baselineVisibleFileIds,
                             List<PendingSegmentEntry> cleansedSegmentEntries,
                             int warnCount)
        {
            this.baselineVisibleFileIds = Collections.unmodifiableSet(new HashSet<>(baselineVisibleFileIds));
            this.cleansedSegmentEntries = Collections.unmodifiableList(new ArrayList<>(cleansedSegmentEntries));
            this.warnCount = warnCount;
        }
    }

    public static final class ReplayResult
    {
        public final Map<Integer, Long> vnodeReplayStarts;
        public final long nodeReplayFromTs;

        public ReplayResult(Map<Integer, Long> vnodeReplayStarts, long nodeReplayFromTs)
        {
            this.vnodeReplayStarts = Collections.unmodifiableMap(new HashMap<>(vnodeReplayStarts));
            this.nodeReplayFromTs = nodeReplayFromTs;
        }

        public Map<Integer, Long> getVnodeReplayStarts() { return vnodeReplayStarts; }
        public long getNodeReplayFromTs() { return nodeReplayFromTs; }
    }

    public static final class RetireOutcome
    {
        public final List<Long> retiredFileIds;
        public final List<Long> protectedFileIds;

        public RetireOutcome(List<Long> retiredFileIds, List<Long> protectedFileIds)
        {
            this.retiredFileIds = Collections.unmodifiableList(new ArrayList<>(retiredFileIds));
            this.protectedFileIds = Collections.unmodifiableList(new ArrayList<>(protectedFileIds));
        }
    }
}
