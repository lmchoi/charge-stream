package com.doinkey.cg;

import com.doinkey.cg.config.Configuration;
import com.doinkey.cg.config.ConfigurationLoader;
import com.doinkey.cg.domain.ChargeCalculator;
import com.doinkey.cg.domain.TransactionValidator;
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

        // read config
        ConfigurationLoader configurationLoader = new ConfigurationLoader();
        Configuration config = configurationLoader.load(configFilename);
        Properties chargeStreamProperties = StreamPropertiesBuilder.build(config.getChargeStream());

        // create the charge stream
        TransactionValidator transactionValidator = new TransactionValidator();
        ChargeCalculator chargeCalculator = new ChargeCalculator();
        ChargeStream chargeStream = new ChargeStream(transactionValidator, chargeCalculator);

        // start processing
        chargeStream.start(chargeStreamProperties, "transaction-topic", "charge-topic", "failed-transactions");
        addShutdownHookAndBlock(chargeStream);
    }
}
