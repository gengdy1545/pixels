/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.retina.benchmark.index;

import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkPhase;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SyntheticRowLayoutTest
{
    @Test
    void allocatesContiguousDisjointRoleAndPhaseDomains()
    {
        SyntheticRowLayout layout = new SyntheticRowLayout(100L, 10L, 2L, 3L, 2);

        assertEquals(100L, layout.rowId(BenchmarkPhase.WARMUP, 0L, SyntheticRowLayout.Role.OLD));
        assertEquals(101L, layout.rowId(BenchmarkPhase.WARMUP, 1L, SyntheticRowLayout.Role.OLD));
        assertEquals(102L, layout.rowId(BenchmarkPhase.MEASUREMENT, 0L, SyntheticRowLayout.Role.OLD));
        assertEquals(104L, layout.rowId(BenchmarkPhase.MEASUREMENT, 2L, SyntheticRowLayout.Role.OLD));
        assertEquals(105L, layout.rowId(BenchmarkPhase.WARMUP, 0L, SyntheticRowLayout.Role.NEW));
        assertEquals(107L, layout.rowId(BenchmarkPhase.MEASUREMENT, 0L, SyntheticRowLayout.Role.NEW));
        assertEquals(109L, layout.rowId(BenchmarkPhase.MEASUREMENT, 2L, SyntheticRowLayout.Role.NEW));
    }

    @Test
    void usesDistinctFilesAndReconstructsLocations()
    {
        SyntheticRowLayout layout = new SyntheticRowLayout(100L, 10L, 2L, 3L, 2);

        assertEquals(10L, layout.fileId(BenchmarkPhase.WARMUP, SyntheticRowLayout.Role.OLD));
        assertEquals(11L, layout.fileId(BenchmarkPhase.WARMUP, SyntheticRowLayout.Role.NEW));
        assertEquals(12L, layout.fileId(BenchmarkPhase.MEASUREMENT, SyntheticRowLayout.Role.OLD));
        assertEquals(13L, layout.fileId(BenchmarkPhase.MEASUREMENT, SyntheticRowLayout.Role.NEW));

        IndexProto.RowLocation expected = IndexProto.RowLocation.newBuilder()
                .setFileId(13L).setRgId(1).setRgRowOffset(0).build();
        assertEquals(expected, layout.locationForRowId(109L));
        assertEquals(1L, layout.rangeCount(BenchmarkPhase.WARMUP));
        assertEquals(2L, layout.rangeCount(BenchmarkPhase.MEASUREMENT));
    }

    @Test
    void rejectsRowsAndOperationsOutsideGeneratedDomains()
    {
        SyntheticRowLayout layout = new SyntheticRowLayout(100L, 10L, 0L, 3L, 2);

        assertThrows(IllegalArgumentException.class,
                () -> layout.rowId(BenchmarkPhase.WARMUP, 0L, SyntheticRowLayout.Role.OLD));
        assertThrows(IllegalArgumentException.class, () -> layout.locationForRowId(99L));
        assertThrows(IllegalArgumentException.class, () -> layout.locationForRowId(106L));
    }
}
