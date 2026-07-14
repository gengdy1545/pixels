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
import io.pixelsdb.pixels.retina.benchmark.transaction.TransactionEndpoint;

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
 * <p>The endpoint is selected from {@code --trans-host} and
 * {@code --port}/{@code --trans-port}, then {@code trans.server.host/port} in
 * the Pixels properties file, then {@code localhost:18889}. The host is
 * recorded in configuration for clients; {@link TransServer} itself binds by
 * port through {@code ServerBuilder.forPort}. This launcher does not require
 * the rest of {@code DaemonMain}'s coordinator services.</p>
 */
public final class TransactionOnlyMain
{
    private TransactionOnlyMain()
    {
    }

    public static void main(String[] args)
    {
        TransactionEndpoint endpoint = TransactionEndpoint.fromServerArgs(args);
        ConfigFactory.Instance().addProperty("trans.server.host", endpoint.host());
        ConfigFactory.Instance().addProperty("trans.server.port", Integer.toString(endpoint.port()));
        final TransServer server = new TransServer(endpoint.port());
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown,
                "retina-benchmark-trans-shutdown"));
        server.run();
    }
}
