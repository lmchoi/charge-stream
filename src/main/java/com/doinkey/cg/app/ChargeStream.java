package com.doinkey.cg.app;

import com.doinkey.cg.app.domain.ChargeCalculator;
import com.doinkey.cg.app.domain.TransactionValidator;
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

// TODO extract charge specific code and make this a generic avro transform stream
public class ChargeStream {

    private KafkaStreams streams;
    private SpecificAvroSerde<Charge> chargeSerde = new SpecificAvroSerde<>();
    private SpecificAvroSerde<FailedTransaction> errorSerde = new SpecificAvroSerde<>();

    private final TransactionValidator transactionValidator;
    private final ChargeCalculator chargeCalculator;

    public ChargeStream(TransactionValidator transactionValidator, ChargeCalculator chargeCalculator) {
        this.transactionValidator = transactionValidator;
        this.chargeCalculator = chargeCalculator;
    }

    public void start(Properties streamsConfiguration, String inputTopic, String outputTopic, String errorTopic) {
        streams = createProcessTopology(streamsConfiguration, inputTopic, outputTopic, errorTopic);

        // TODO consider creating Topic class and move these out
        Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                streamsConfiguration.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
        chargeSerde.configure(serdeConfig, false);
        errorSerde.configure(serdeConfig, false);

        if (streams != null) {
            streams.start();
        }
    }

    private KafkaStreams createProcessTopology(Properties streamsConfiguration, String inputTopic, String outputTopic, String errorTopic) {
        KStreamBuilder builder = new KStreamBuilder();

        final Serde<String> stringSerde = Serdes.String();
//        Topic charges = new Topic(outputTopic, stringSerde, chargeSerde);
//        Topic errors = new Topic(outputTopic, stringSerde, errorSerde);

        KStream<String, Transaction> transactionStream = builder.stream(inputTopic);
        KStream<String, Transaction>[] validatedTransactions = transactionStream.branch(
                (k, v) -> transactionValidator.isKeyBad(k),
                (k, v) -> true);

        validatedTransactions[0].mapValues(t -> new FailedTransaction(t.getTxnId(), TransactionValidator.BAD_KEY_MESSAGE))
                .to(stringSerde, errorSerde, errorTopic);

        validatedTransactions[1].mapValues(t -> chargeCalculator.calculate(t))
                .to(stringSerde, chargeSerde, outputTopic);

        return new KafkaStreams(builder, streamsConfiguration);
    }

    public void stop() {
        if (streams != null) {
            streams.close();
        }
    }
}
