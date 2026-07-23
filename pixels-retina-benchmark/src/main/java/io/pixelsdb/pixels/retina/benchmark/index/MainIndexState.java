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

import java.util.Locale;

/**
 * MainIndex state immediately before a timed primary update or delete.
 *
 * <p>{@link #NATURAL} uses restored snapshot keys as-is with no fixture
 * prefill and no artificial cache warm-up. Cache hits/misses are part of the
 * measured path.</p>
 */
enum MainIndexState
{
    NATURAL("natural"),
    HOT_BUFFER("hot-buffer"),
    WARM_CACHE("warm-cache"),
    COLD_START("cold-start");

    private final String optionValue;

    MainIndexState(String optionValue)
    {
        this.optionValue = optionValue;
    }

    String optionValue()
    {
        return optionValue;
    }

    static MainIndexState parse(String value)
    {
        String normalized = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
        for (MainIndexState state : values())
        {
            if (state.optionValue.equals(normalized))
            {
                return state;
            }
        }
        throw new IllegalArgumentException("--main-index-state must be natural, hot-buffer, "
                + "warm-cache, or cold-start: " + value);
    }
}
