package com.doinkey.cg;

import com.doinkey.cg.config.Configuration;
import com.doinkey.cg.config.ConfigurationLoader;
import com.doinkey.cg.streams.ChargeStream;
import com.doinkey.cg.streams.StreamPropertiesBuilder;

import java.util.Properties;

public class App
{
    public static void addShutdownHookAndBlock(ChargeStream service) throws InterruptedException {
        Thread.currentThread().setUncaughtExceptionHandler((t, e) -> service.stop());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                service.stop();
            } catch (Exception ignored) {
            }
        }));
        Thread.currentThread().join();
    }

    public static void main( String[] args ) throws InterruptedException {
        String configFilename = args[0];

        ConfigurationLoader configurationLoader = new ConfigurationLoader();
        Configuration config = configurationLoader.load(configFilename);
        Properties chargeStreamProperties = StreamPropertiesBuilder.build(config.getChargeStream());

        ChargeStream chargeStream = new ChargeStream();
        chargeStream.start(chargeStreamProperties, "transaction-topic", "charge-topic", "failed-transactions");
        addShutdownHookAndBlock(chargeStream);
    }
}
