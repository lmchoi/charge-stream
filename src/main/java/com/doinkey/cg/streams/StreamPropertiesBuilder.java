package com.doinkey.cg.streams;

import com.doinkey.cg.config.StreamsConfiguration;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class StreamPropertiesBuilder {
    public static Properties build(StreamsConfiguration streamsConfiguration) {
        return build(
                streamsConfiguration.getApplicationId(),
                String.join(",", streamsConfiguration.getBootstrapServers()),
                streamsConfiguration.getSchemaRegistryUrl());
    }

    public static Properties build(String applicationId,
                                   String bootstrapServers,
                                   String schemaRegistryUrl) {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // TODO need to revisit the configs [See https://kafka.apache.org/documentation/#streamsconfigs]
//        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
//        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
//        streamsConfiguration.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        return streamsConfiguration;
    }
}
