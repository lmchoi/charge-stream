package com.doinkey.cg.app;

import com.doinkey.cg.app.domain.ChargeCalculator;
import com.doinkey.cg.app.domain.TransactionValidator;

import java.util.Properties;

public class ChargeService {
    private final ChargeStream chargeStream;
    private final Properties chargeStreamProperties;
    private final String outputTopic;
    private final String errorTopic;

    public ChargeService(Properties chargeStreamProperties, String outputTopic, String errorTopic) {
        this.chargeStreamProperties = chargeStreamProperties;
        this.outputTopic = outputTopic;
        this.errorTopic = errorTopic;

        TransactionValidator transactionValidator = new TransactionValidator();
        ChargeCalculator chargeCalculator = new ChargeCalculator();
        chargeStream = new ChargeStream(transactionValidator, chargeCalculator);

        // TODO where does topic creation belong? here? or when the topology is created?
//        Serde<String> stringSerde = Serdes.String();
//        SpecificAvroSerde<Charge> chargeSerde = new SpecificAvroSerde<>();
//        SpecificAvroSerde<FailedTransaction> errorSerde = new SpecificAvroSerde<>();
//        Topic charges = new Topic(outputTopic, stringSerde, chargeSerde);
//        Topic errors = new Topic(errorTopic, stringSerde, errorSerde);
    }

    public void start() throws InterruptedException {
        chargeStream.start(chargeStreamProperties, "transaction-topic", outputTopic, errorTopic);
    }

    public void stop() {
        chargeStream.stop();
    }
}
