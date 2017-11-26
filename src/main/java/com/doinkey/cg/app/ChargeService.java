package com.doinkey.cg.app;

import com.doinkey.cg.app.domain.ChargeCalculator;
import com.doinkey.cg.app.domain.TransactionValidator;
import com.doinkey.cg.streams.Topic;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class ChargeService {
    private final ChargeStream chargeStream;
    private final Properties chargeStreamProperties;
    private final Topic<String, Transaction> transactionTopic;
    private final Topic<String, Charge> chargeTopic;
    private final Topic<String, FailedTransaction> failedTransactionTopic;

    public ChargeService(Properties chargeStreamProperties,
                         String inputTopicName,
                         String outputTopicName,
                         String errorTopicName) {
        this.chargeStreamProperties = chargeStreamProperties;

        TransactionValidator transactionValidator = new TransactionValidator();
        ChargeCalculator chargeCalculator = new ChargeCalculator();
        chargeStream = new ChargeStream(transactionValidator, chargeCalculator);

        // TODO where does topic creation belong? here? or when the topology is created?
        Serde<String> stringSerde = Serdes.String();
        Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                chargeStreamProperties.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));

        // transactionserde is not actually use if StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG is set to SpecificAvroSerde
        SpecificAvroSerde<Transaction> transactionSerde = new SpecificAvroSerde<>();
        transactionSerde.configure(serdeConfig, false);
        transactionTopic = new Topic<>(inputTopicName, stringSerde, transactionSerde);

        SpecificAvroSerde<Charge> chargeSerde = new SpecificAvroSerde<>();
        chargeSerde.configure(serdeConfig, false);
        chargeTopic = new Topic<>(outputTopicName, stringSerde, chargeSerde);

        SpecificAvroSerde<FailedTransaction> errorSerde = new SpecificAvroSerde<>();
        errorSerde.configure(serdeConfig, false);
        failedTransactionTopic = new Topic<>(errorTopicName, stringSerde, errorSerde);
    }

    public void start() throws InterruptedException {
        chargeStream.start(chargeStreamProperties, transactionTopic, chargeTopic, failedTransactionTopic);
    }

    public void stop() {
        chargeStream.stop();
    }
}
