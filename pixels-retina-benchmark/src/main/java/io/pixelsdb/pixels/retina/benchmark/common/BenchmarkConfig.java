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
package io.pixelsdb.pixels.retina.benchmark.common;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Immutable command-line configuration supporting both --key=value and --key value. */
public final class BenchmarkConfig
{
    private static final long DEFAULT_DATA_SIZE = 1_000_000L;
    private static final long DEFAULT_WARMUP_OPERATIONS = 100_000L;
    private final Map<String, String> options;

    private BenchmarkConfig(Map<String, String> options)
    {
        this.options = Collections.unmodifiableMap(new LinkedHashMap<>(options));
        validate();
    }

    public static BenchmarkConfig parse(String... args)
    {
        Map<String, String> values = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++)
        {
            String arg = args[i];
            if (!arg.startsWith("--") || arg.length() == 2)
            {
                throw new IllegalArgumentException("expected --key=value or --key value, got: " + arg);
            }
            String body = arg.substring(2);
            int equals = body.indexOf('=');
            String key;
            String value;
            if (equals >= 0)
            {
                key = body.substring(0, equals);
                value = body.substring(equals + 1);
            }
            else
            {
                key = body;
                if (i + 1 < args.length && !args[i + 1].startsWith("--"))
                {
                    value = args[++i];
                }
                else
                {
                    value = "true";
                }
            }
            if (key.isEmpty() || value.isEmpty())
            {
                throw new IllegalArgumentException("empty option key or value in: " + arg);
            }
            values.put(key, value);
        }
        return new BenchmarkConfig(values);
    }

    public int threads() { return getInt("threads", Math.max(1, Runtime.getRuntime().availableProcessors())); }
    public int clients() { return getInt("clients", 1); }
    public long warmupSeconds() { return getLong("warmup-seconds", 10); }
    public long durationSeconds() { return getLong("duration-seconds", 30); }
    public long dataSize() { return getLong("data-size", DEFAULT_DATA_SIZE); }
    public int batchSize() { return getInt("batch-size", 1); }
    public long warmupOperations() { return getLong("warmup-operations", Math.min(dataSize(), DEFAULT_WARMUP_OPERATIONS)); }

    public String get(String key, String defaultValue) { return options.containsKey(key) ? options.get(key) : defaultValue; }
    public String require(String key)
    {
        String value = options.get(key);
        if (value == null) throw new IllegalArgumentException("missing required option --" + key);
        return value;
    }
    public int getInt(String key, int defaultValue) { return Integer.parseInt(get(key, Integer.toString(defaultValue))); }
    public long getLong(String key, long defaultValue) { return Long.parseLong(get(key, Long.toString(defaultValue))); }
    public double getDouble(String key, double defaultValue) { return Double.parseDouble(get(key, Double.toString(defaultValue))); }
    public boolean getBoolean(String key, boolean defaultValue)
    {
        String value = get(key, Boolean.toString(defaultValue));
        if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value))
        {
            throw new IllegalArgumentException("--" + key + " must be true or false");
        }
        return Boolean.parseBoolean(value);
    }
    public Map<String, String> options() { return options; }

    private void validate()
    {
        if (threads() <= 0 || clients() <= 0 || warmupSeconds() < 0 || durationSeconds() <= 0 ||
                dataSize() <= 0 || batchSize() <= 0 || warmupOperations() < 0)
        {
            throw new IllegalArgumentException("threads, clients, duration, data-size and batch-size must be positive; warmup values must be non-negative");
        }
        if (clients() > threads())
        {
            throw new IllegalArgumentException("clients must not exceed threads; otherwise some clients are idle");
        }
    }
}
