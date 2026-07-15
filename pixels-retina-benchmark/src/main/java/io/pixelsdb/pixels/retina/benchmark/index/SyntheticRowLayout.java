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

import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkPhase;

/** Production-like contiguous row-id and row-location domains for Index fixtures. */
final class SyntheticRowLayout
{
    enum Role
    {
        OLD,
        NEW
    }

    private final long rowIdBase;
    private final long fileIdBase;
    private final long warmupOperations;
    private final long measurementOperations;
    private final long operationsPerRole;
    private final int rowsPerRowGroup;

    SyntheticRowLayout(long rowIdBase, long fileIdBase, long warmupOperations,
                       long measurementOperations, int rowsPerRowGroup)
    {
        if (rowIdBase < 0 || fileIdBase <= 0 || warmupOperations < 0
                || measurementOperations <= 0 || rowsPerRowGroup <= 0)
        {
            throw new IllegalArgumentException("invalid synthetic row layout");
        }
        this.rowIdBase = rowIdBase;
        this.fileIdBase = fileIdBase;
        this.warmupOperations = warmupOperations;
        this.measurementOperations = measurementOperations;
        this.operationsPerRole = Math.addExact(warmupOperations, measurementOperations);
        this.rowsPerRowGroup = rowsPerRowGroup;
        validateDomains();
    }

    long rowId(BenchmarkPhase phase, long operationId, Role role)
    {
        long global = globalOperation(phase, operationId);
        long roleOffset = role == Role.OLD ? 0L : operationsPerRole;
        return Math.addExact(rowIdBase, Math.addExact(roleOffset, global));
    }

    IndexProto.RowLocation location(BenchmarkPhase phase, long operationId, Role role)
    {
        validateOperation(phase, operationId);
        return IndexProto.RowLocation.newBuilder()
                .setFileId(fileId(phase, role))
                .setRgId((int) (operationId / rowsPerRowGroup))
                .setRgRowOffset((int) (operationId % rowsPerRowGroup))
                .build();
    }

    IndexProto.RowLocation locationForRowId(long rowId)
    {
        long relative = rowId - rowIdBase;
        if (relative < 0 || relative >= Math.multiplyExact(operationsPerRole, 2L))
        {
            throw new IllegalArgumentException("row id is outside synthetic domains: " + rowId);
        }
        Role role = relative < operationsPerRole ? Role.OLD : Role.NEW;
        long global = role == Role.OLD ? relative : relative - operationsPerRole;
        BenchmarkPhase phase = global < warmupOperations
                ? BenchmarkPhase.WARMUP : BenchmarkPhase.MEASUREMENT;
        long operationId = phase == BenchmarkPhase.WARMUP
                ? global : global - warmupOperations;
        return location(phase, operationId, role);
    }

    long fileId(BenchmarkPhase phase, Role role)
    {
        long phaseOffset = phase == BenchmarkPhase.WARMUP ? 0L : 2L;
        long roleOffset = role == Role.OLD ? 0L : 1L;
        return Math.addExact(fileIdBase, phaseOffset + roleOffset);
    }

    long globalOperation(BenchmarkPhase phase, long operationId)
    {
        validateOperation(phase, operationId);
        return phase == BenchmarkPhase.WARMUP
                ? operationId : Math.addExact(warmupOperations, operationId);
    }

    long phaseOperations(BenchmarkPhase phase)
    {
        return phase == BenchmarkPhase.WARMUP ? warmupOperations : measurementOperations;
    }

    long rangeCount(BenchmarkPhase phase)
    {
        long operations = phaseOperations(phase);
        return operations == 0 ? 0 : (operations + rowsPerRowGroup - 1L) / rowsPerRowGroup;
    }

    int rowsPerRowGroup()
    {
        return rowsPerRowGroup;
    }

    private void validateOperation(BenchmarkPhase phase, long operationId)
    {
        if (phase == null || operationId < 0 || operationId >= phaseOperations(phase))
        {
            throw new IllegalArgumentException("operation is outside " + phase + " domain: " + operationId);
        }
    }

    private void validateDomains()
    {
        long rowIds = Math.multiplyExact(operationsPerRole, 2L);
        Math.addExact(rowIdBase, rowIds - 1L);
        Math.addExact(fileIdBase, 3L);
        long largestPhase = Math.max(warmupOperations, measurementOperations);
        long maximumRowGroup = (largestPhase - 1L) / rowsPerRowGroup;
        if (maximumRowGroup > Integer.MAX_VALUE)
        {
            throw new IllegalArgumentException("synthetic RowLocation exceeds Java row-group range");
        }
    }
}
