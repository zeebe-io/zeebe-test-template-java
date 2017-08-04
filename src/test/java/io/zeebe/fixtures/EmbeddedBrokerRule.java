package io.zeebe.fixtures;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

import io.zeebe.broker.Broker;
import org.junit.rules.ExternalResource;

public class EmbeddedBrokerRule extends ExternalResource
{
    private Broker broker;
    private Supplier<InputStream> configSupplier;

    public EmbeddedBrokerRule()
    {
        this(() -> null);
    }

    public EmbeddedBrokerRule(Supplier<InputStream> configSupplier)
    {
        this.configSupplier = configSupplier;
    }


    @Override
    protected void before() throws Throwable
    {
        startBroker();
    }

    @Override
    protected void after()
    {
        broker.close();

        broker = null;
        System.gc();
    }

    public void startBroker()
    {
        try (InputStream configStream = configSupplier.get())
        {
            broker = new Broker(configStream);
        }
        catch (final IOException e)
        {
            throw new RuntimeException("Unable to read configuration", e);
        }

        // wait until up and running
        try
        {
            Thread.sleep(1000);
        }
        catch (InterruptedException e)
        {
            // ignore
        }
    }

}
