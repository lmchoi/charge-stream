package com.doinkey.cg;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Collections;
import java.util.Properties;

public class ChargeStream {

    private KafkaStreams streams;
    private SpecificAvroSerde<Charge> chargeSerde = new SpecificAvroSerde<>();

    public void start(Properties streamsConfiguration, String inputTopic, String outputTopic) {
        processStreams(streamsConfiguration, inputTopic, outputTopic);
        final boolean isKeySerde = false;
        chargeSerde.configure(
                Collections.singletonMap(
                        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        streamsConfiguration.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG)),
                isKeySerde);
        streams.start();
    }

    private void processStreams(Properties streamsConfiguration, String inputTopic, String outputTopic) {
        KStreamBuilder builder = new KStreamBuilder();

        final Serde<String> stringSerde = Serdes.String();
        KStream<String, Transaction> stream = builder.stream(inputTopic);
        stream.mapValues(t -> new Charge(t.getTxnId()))
            .to(stringSerde, chargeSerde, outputTopic);

        streams = new KafkaStreams(builder, streamsConfiguration);
    }

    public void stop() {
        if (streams != null) {
            streams.close();
        }
    }
}
