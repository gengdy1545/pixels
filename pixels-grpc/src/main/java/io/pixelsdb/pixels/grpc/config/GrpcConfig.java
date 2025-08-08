
package io.pixelsdb.pixels.grpc.config;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class GrpcConfig
{
    private static GrpcConfig instance = null;

    private final Properties prop = new Properties();

    private String host;
    private int port;

    public GrpcConfig()
    {
        try(InputStream in = this.getClass().getResourceAsStream("/grpc.properties"))
        {
            if (in != null)
            {
                prop.load(in);
                String grpcTestTarget = prop.getProperty("grpc.test.target");

                ConfigFactory configFactory = ConfigFactory.Instance();
                host = configFactory.getProperty(grpcTestTarget + ".server.host");
                port = Integer.parseInt(configFactory.getProperty(grpcTestTarget + ".server.port"));
            }
        } catch (IOException e)
        {
            throw new RuntimeException("Failed to load grpc.properties", e);
        }
    }

    public String getHost()
    {
        return host;
    }

    public int getPort()
    {
        return port;
    }

    public int getConcurrency()
    {
        return Integer.parseInt(prop.getProperty("grpc.concurrency"));
    }

    public int getRequests()
    {
        return Integer.parseInt(prop.getProperty("grpc.requests"));
    }

    public int getWarmup()
    {
        return Integer.parseInt(prop.getProperty("grpc.warmup"));
    }

    public int getKeepAliveTime()
    {
        return Integer.parseInt(prop.getProperty("grpc.keepalive.time"));
    }

    public int getKeepAliveTimeout()
    {
        return Integer.parseInt(prop.getProperty("grpc.keepalive.timeout"));
    }

    public boolean getKeepAliveWithoutCalls()
    {
        return Boolean.parseBoolean(prop.getProperty("grpc.keepalive.without.calls"));
    }

    public int getMaxInboundMessageSize()
    {
        return Integer.parseInt(prop.getProperty("grpc.max.inbound.message.size"));
    }

    public int getThreadPoolSize()
    {
        return Integer.parseInt(prop.getProperty("grpc.thread.pool.size"));
    }

    public int getChannelPoolSize()
    {
        return Integer.parseInt(prop.getProperty("grpc.channel.pool.size"));
    }
}
