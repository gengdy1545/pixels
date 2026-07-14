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
import io.pixelsdb.pixels.daemon.metadata.MetadataServer;
import io.pixelsdb.pixels.daemon.node.NodeServer;

/**
 * Starts the smallest real service set needed to construct a
 * {@code PixelsWriteBuffer}: the production metadata server and its production
 * node service dependency.
 *
 * <p>No RPC is mocked.  {@code MetadataServer} registers the repository's
 * {@code MetadataServiceImpl}; {@code NodeServer} registers
 * {@code NodeServiceImpl}.  The latter is needed because table creation asks
 * the node service for Retina nodes before returning.  Consequently MySQL and
 * etcd, as configured in {@code pixels.properties}, must already be healthy.</p>
 *
 * <p>The measured {@code PixelsWriteBuffer.addRow} path does not issue a
 * metadata RPC for every row, but {@code FileWriterManager} uses the real
 * metadata RPC whenever it creates a new output file.  Those periodic calls
 * are deliberately retained in the benchmark.</p>
 */
public final class MetadataOnlyMain
{
    private MetadataOnlyMain()
    {
    }

    public static void main(String[] args) throws InterruptedException
    {
        int metadataPort = resolvePort(args, "--metadata-port", "metadata.server.port", 18888);
        int nodePort = resolvePort(args, "--node-port", "node.server.port", 18891);

        /* MetadataServiceImpl uses the NodeService client singleton, so a CLI
         * port override must be visible before that singleton initializes. */
        ConfigFactory.Instance().addProperty("metadata.server.port", Integer.toString(metadataPort));
        ConfigFactory.Instance().addProperty("node.server.port", Integer.toString(nodePort));

        final NodeServer nodeServer = new NodeServer(nodePort);
        final MetadataServer metadataServer = new MetadataServer(metadataPort);

        Thread nodeThread = new Thread(nodeServer::run, "retina-benchmark-node-server");
        Thread metadataThread = new Thread(metadataServer::run, "retina-benchmark-metadata-server");

        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            metadataServer.shutdown();
            nodeServer.shutdown();
        }, "retina-benchmark-metadata-shutdown"));

        nodeThread.start();
        metadataThread.start();
        try
        {
            metadataThread.join();
            nodeThread.join();
        }
        finally
        {
            metadataServer.shutdown();
            nodeServer.shutdown();
        }
    }

    private static int resolvePort(String[] args, String option, String property, int defaultPort)
    {
        for (int i = 0; i < args.length; i++)
        {
            String arg = args[i];
            if (arg.startsWith(option + "="))
            {
                return validatePort(Integer.parseInt(arg.substring(option.length() + 1)));
            }
            if (option.equals(arg))
            {
                if (i + 1 >= args.length)
                {
                    throw new IllegalArgumentException(option + " requires a value");
                }
                return validatePort(Integer.parseInt(args[i + 1]));
            }
        }

        String value = ConfigFactory.Instance().getProperty(property);
        return validatePort(value == null || value.isEmpty() ? defaultPort : Integer.parseInt(value));
    }

    private static int validatePort(int port)
    {
        if (port <= 0 || port > 65535)
        {
            throw new IllegalArgumentException("invalid server port: " + port);
        }
        return port;
    }
}
