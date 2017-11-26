package com.doinkey.cg.app;

import com.doinkey.cg.app.domain.ChargeCalculator;
import com.doinkey.cg.app.domain.TransactionValidator;

import java.util.Properties;

public class ChargeService {
    private final ChargeStream chargeStream;
    private final Properties chargeStreamProperties;
    private final String outputTopic;
    private final String errorTopic;

    public ChargeService(Properties chargeStreamProperties) {
        this.chargeStreamProperties = chargeStreamProperties;

        TransactionValidator transactionValidator = new TransactionValidator();
        ChargeCalculator chargeCalculator = new ChargeCalculator();
        chargeStream = new ChargeStream(transactionValidator, chargeCalculator);

        // Create topics
//        Serde<String> stringSerde = Serdes.String();
//        SpecificAvroSerde<Charge> chargeSerde = new SpecificAvroSerde<>();
//        SpecificAvroSerde<FailedTransaction> errorSerde = new SpecificAvroSerde<>();
        outputTopic = "charge-topic";
        errorTopic = "failed-transactions";
    }

    public void start() throws InterruptedException {
        // start processing
        chargeStream.start(chargeStreamProperties, "transaction-topic", outputTopic, errorTopic);
        addShutdownHookAndBlock(chargeStream);
    }

    // TODO perhaps this should be in a generic "service" class...
    public static void addShutdownHookAndBlock(ChargeStream stream) throws InterruptedException {
        Thread.currentThread().setUncaughtExceptionHandler((t, e) -> stream.stop());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                stream.stop();
            } catch (Exception ignored) {
            }
        }));
        Thread.currentThread().join();
    }
}
