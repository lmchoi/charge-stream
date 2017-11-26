package com.doinkey.cg;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class ChargeStream {

    private KafkaStreams streams;
    private SpecificAvroSerde<Charge> chargeSerde = new SpecificAvroSerde<>();
    private SpecificAvroSerde<FailedTransaction> errorSerde = new SpecificAvroSerde();

    private TransactionValidator transactionValidator;
    private ChargeCalculator chargeCalculator;

    public void start(Properties streamsConfiguration, String inputTopic, String outputTopic, String errorTopic) {
        processStreams(streamsConfiguration, inputTopic, outputTopic, errorTopic);
        Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                streamsConfiguration.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
        chargeSerde.configure(serdeConfig, false);
        errorSerde.configure(serdeConfig, false);

        transactionValidator = new TransactionValidator();
        chargeCalculator = new ChargeCalculator();
        streams.start();
    }

    private void processStreams(Properties streamsConfiguration, String inputTopic, String outputTopic, String errorTopic) {
        KStreamBuilder builder = new KStreamBuilder();

        final Serde<String> stringSerde = Serdes.String();
        KStream<String, Transaction> transactionStream = builder.stream(inputTopic);
        KStream<String, Transaction>[] validatedTransactions = transactionStream.branch(
                (k, v) -> transactionValidator.isKeyBad(k),
                (k, v) -> true);

        validatedTransactions[0].mapValues(t -> new FailedTransaction(t.getTxnId(), TransactionValidator.BAD_KEY_MESSAGE))
                .to(stringSerde, errorSerde, errorTopic);

        validatedTransactions[1].mapValues(t -> chargeCalculator.calculate(t))
                .to(stringSerde, chargeSerde, outputTopic);

        streams = new KafkaStreams(builder, streamsConfiguration);
    }

    public void stop() {
        if (streams != null) {
            streams.close();
        }
    }
}
