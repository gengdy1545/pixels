/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 */
package io.pixelsdb.pixels.retina.benchmark.transaction;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.retina.benchmark.common.BenchmarkConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TransactionEndpointTest
{
    @Test
    void usesPixelsPropertiesWhenCliIsAbsent()
    {
        ConfigFactory.Instance().addProperty("trans.server.host", "transaction.example");
        ConfigFactory.Instance().addProperty("trans.server.port", "19999");

        TransactionEndpoint endpoint = TransactionEndpoint.from(BenchmarkConfig.parse(new String[0]));

        assertEquals("transaction.example", endpoint.host());
        assertEquals(19999, endpoint.port());
    }

    @Test
    void cliOverridesPixelsProperties()
    {
        ConfigFactory.Instance().addProperty("trans.server.host", "properties.example");
        ConfigFactory.Instance().addProperty("trans.server.port", "18889");
        BenchmarkConfig config = BenchmarkConfig.parse(new String[]{
                "--trans-host", "cli.example", "--trans-port", "20001"
        });

        TransactionEndpoint endpoint = TransactionEndpoint.from(config);

        assertEquals("cli.example", endpoint.host());
        assertEquals(20001, endpoint.port());
    }

    @Test
    void rejectsInvalidPort()
    {
        BenchmarkConfig config = BenchmarkConfig.parse(new String[]{"--trans-port", "70000"});
        assertThrows(IllegalArgumentException.class, () -> TransactionEndpoint.from(config));
    }
}
