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

/** The six physical LocalIndexService batch operations benchmarked independently. */
public enum IndexOperation
{
    PUT_PRIMARY("index-put-primary", true, false),
    UPDATE_PRIMARY("index-update-primary", true, true),
    DELETE_PRIMARY("index-delete-primary", true, true),
    PUT_SECONDARY("index-put-secondary", false, false),
    UPDATE_SECONDARY("index-update-secondary", false, true),
    DELETE_SECONDARY("index-delete-secondary", false, true);

    private final String commandName;
    private final boolean primary;
    private final boolean existingKeyRequired;

    IndexOperation(String commandName, boolean primary, boolean existingKeyRequired)
    {
        this.commandName = commandName;
        this.primary = primary;
        this.existingKeyRequired = existingKeyRequired;
    }

    public String commandName()
    {
        return commandName;
    }

    public boolean isPrimary()
    {
        return primary;
    }

    public boolean requiresExistingKey()
    {
        return existingKeyRequired;
    }
}
