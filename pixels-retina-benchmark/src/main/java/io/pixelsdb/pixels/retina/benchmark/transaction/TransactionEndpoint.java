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
package io.pixelsdb.pixels.retina.benchmark.transaction;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkConfig;

/**
 * Transaction service endpoint resolved consistently by benchmarks and the
 * minimal production server launcher.
 */
public final class TransactionEndpoint
{
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 18889;

    private final String host;
    private final int port;

    private TransactionEndpoint(String host, int port)
    {
        this.host = host;
        this.port = port;
    }

    public static TransactionEndpoint from(BenchmarkConfig config)
    {
        String host = valueOrNull(config.options().get("trans-host"));
        String port = valueOrNull(config.options().get("trans-port"));
        return resolve(host, port);
    }

    /**
     * Resolve launcher arguments. Both {@code --port} and {@code --trans-port}
     * override the configured transaction port.
     */
    public static TransactionEndpoint fromServerArgs(String[] args)
    {
        String host = null;
        String port = null;
        for (int i = 0; i < args.length; i++)
        {
            String arg = args[i];
            if (arg.startsWith("--trans-host="))
            {
                host = requireValue("--trans-host", arg.substring("--trans-host=".length()));
            }
            else if ("--trans-host".equals(arg))
            {
                host = nextValue(args, ++i, "--trans-host");
            }
            else if (arg.startsWith("--trans-port="))
            {
                port = requireValue("--trans-port", arg.substring("--trans-port=".length()));
            }
            else if ("--trans-port".equals(arg))
            {
                port = nextValue(args, ++i, "--trans-port");
            }
            else if (arg.startsWith("--port="))
            {
                port = requireValue("--port", arg.substring("--port=".length()));
            }
            else if ("--port".equals(arg))
            {
                port = nextValue(args, ++i, "--port");
            }
        }
        return resolve(host, port);
    }

    public String host()
    {
        return host;
    }

    public int port()
    {
        return port;
    }

    private static TransactionEndpoint resolve(String cliHost, String cliPort)
    {
        ConfigFactory config = ConfigFactory.Instance();
        String host = firstNonEmpty(cliHost, config.getProperty("trans.server.host"), DEFAULT_HOST);
        String portText = firstNonEmpty(cliPort, config.getProperty("trans.server.port"),
                Integer.toString(DEFAULT_PORT));
        return new TransactionEndpoint(host, parsePort(portText));
    }

    private static int parsePort(String value)
    {
        final int port;
        try
        {
            port = Integer.parseInt(value);
        }
        catch (NumberFormatException e)
        {
            throw new IllegalArgumentException("invalid transaction server port: " + value, e);
        }
        if (port <= 0 || port > 65535)
        {
            throw new IllegalArgumentException("invalid transaction server port: " + port);
        }
        return port;
    }

    private static String firstNonEmpty(String first, String second, String fallback)
    {
        String value = valueOrNull(first);
        if (value != null)
        {
            return value;
        }
        value = valueOrNull(second);
        return value == null ? fallback : value;
    }

    private static String valueOrNull(String value)
    {
        return value == null || value.isEmpty() ? null : value;
    }

    private static String nextValue(String[] args, int index, String option)
    {
        if (index >= args.length || args[index].startsWith("--"))
        {
            throw new IllegalArgumentException(option + " requires a value");
        }
        return requireValue(option, args[index]);
    }

    private static String requireValue(String option, String value)
    {
        if (value == null || value.isEmpty())
        {
            throw new IllegalArgumentException(option + " requires a value");
        }
        return value;
    }
}
