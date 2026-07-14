/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.retina.benchmark.index;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexOperationTest
{
    @Test
    void exposesSixIndependentCommands()
    {
        assertEquals(6, IndexOperation.values().length);
        assertEquals("index-put-primary", IndexOperation.PUT_PRIMARY.commandName());
        assertEquals("index-update-secondary", IndexOperation.UPDATE_SECONDARY.commandName());
    }

    @Test
    void marksFixtureRequirements()
    {
        assertFalse(IndexOperation.PUT_PRIMARY.requiresExistingKey());
        assertTrue(IndexOperation.UPDATE_PRIMARY.requiresExistingKey());
        assertTrue(IndexOperation.DELETE_SECONDARY.requiresExistingKey());
    }
}
