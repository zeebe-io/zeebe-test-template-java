package io.zeebe.fixtures;

import java.util.Properties;
import java.util.function.Supplier;

import io.zeebe.client.ZeebeClient;
import org.junit.rules.ExternalResource;

public class ClientRule extends ExternalResource
{
    private ZeebeClient client;
    private final Properties properties;

    public ClientRule()
    {
        this(() -> new Properties());
    }

    public ClientRule(Supplier<Properties> propertiesProvider)
    {
        this.properties = propertiesProvider.get();
    }

    public ZeebeClient getClient()
    {
        return client;
    }

    @Override
    protected void before() throws Throwable
    {
        client = ZeebeClient.create(properties);
        client.connect();
    }

    @Override
    protected void after()
    {
        client.close();
        client = null;
    }

}
