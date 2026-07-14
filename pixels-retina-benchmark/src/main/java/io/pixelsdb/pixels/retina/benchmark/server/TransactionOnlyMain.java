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
package io.pixelsdb.pixels.retina.benchmark.server;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.daemon.transaction.TransServer;

/**
 * Starts only the production transaction gRPC server used by the transaction
 * benchmarks.
 *
 * <p>This is intentionally a very small launcher, not an in-process mock.  Its
 * call path is exactly {@code TransServer -> ServerBuilder -> TransServiceImpl}
 * (see {@code pixels-daemon/.../transaction/TransServer.java}).  Therefore the
 * server still uses {@code PersistentAutoIncrement} and {@code EtcdUtil} from
 * {@code TransServiceImpl}; an etcd endpoint configured by {@code etcd.hosts}
 * and {@code etcd.port} must be reachable before this process starts.</p>
 *
 * <p>The port is selected, in order, from {@code --port}, the system property
 * {@code retina.benchmark.trans.port}, and {@code trans.server.port} in the
 * Pixels properties file.  This launcher does not require the rest of
 * {@code DaemonMain}'s coordinator services.</p>
 */
public final class TransactionOnlyMain
{
    private TransactionOnlyMain()
    {
    }

    public static void main(String[] args)
    {
        int port = resolvePort(args);
        ConfigFactory.Instance().addProperty("trans.server.port", Integer.toString(port));
        final TransServer server = new TransServer(port);
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown,
                "retina-benchmark-trans-shutdown"));
        server.run();
    }

    private static int resolvePort(String[] args)
    {
        for (int i = 0; i < args.length; i++)
        {
            String arg = args[i];
            if (arg.startsWith("--port="))
            {
                return validatePort(Integer.parseInt(arg.substring("--port=".length())));
            }
            if ("--port".equals(arg))
            {
                if (i + 1 >= args.length)
                {
                    throw new IllegalArgumentException("--port requires a value");
                }
                return validatePort(Integer.parseInt(args[i + 1]));
            }
        }

        String value = System.getProperty("retina.benchmark.trans.port");
        if (value == null || value.isEmpty())
        {
            value = ConfigFactory.Instance().getProperty("trans.server.port");
        }
        if (value == null || value.isEmpty())
        {
            value = "18889";
        }
        return validatePort(Integer.parseInt(value));
    }

    private static int validatePort(int port)
    {
        if (port <= 0 || port > 65535)
        {
            throw new IllegalArgumentException("invalid transaction server port: " + port);
        }
        return port;
    }
}
