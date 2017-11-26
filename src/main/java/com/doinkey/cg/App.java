package com.doinkey.cg;

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
        ChargeStream chargeStream = new ChargeStream();
        // TODO Create config from file specify in args
        String applicationId = "charge-stream";
        String bootstrapServers = "localhost:9092";
        String schemaRegistryUrl = "http://localhost:8081";
        Properties streamsConfiguration = StreamsConfiguration
                .buildConfiguration(applicationId,
                        bootstrapServers,
                        schemaRegistryUrl);

        chargeStream.start(streamsConfiguration, "transaction-topic", "charge-topic", "failed-transactions");
        addShutdownHookAndBlock(chargeStream);
    }
}
