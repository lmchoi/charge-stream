package com.doinkey.cg;

import io.confluent.examples.streams.IntegrationTestUtils;
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * End-to-end integration test that demonstrates how to work on Generic Avro data.
 *
 */
public class ChargeStreamIntegrationTest {

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    private static String inputTopic = "transaction-topic";
    private static String outputTopic = "charge-topic";

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(inputTopic);
        CLUSTER.createTopic(outputTopic);
    }

    @Test
    public void shouldRoundTripGenericAvroDataThroughKafka() throws Exception {
        // create test data
        Schema schema = new Schema.Parser().parse(
                getClass().getResourceAsStream("/com/doinkey/cg/transaction.avsc"));
        GenericRecord record = new GenericData.Record(schema);
        record.put("txn_id", "lulz");
        List<KeyValue<String, GenericRecord>> inputValues = Collections.singletonList(new KeyValue<>("much", record));

        //
        // Step 1: Configure and start the processor topology.
        //

        String bootstrapServers = CLUSTER.bootstrapServers();
        String registryUrl = CLUSTER.schemaRegistryUrl();
        Properties properties = StreamsConfiguration
                .buildConfiguration(
                        "generic-avro-integration-test",
                        bootstrapServers,
                        registryUrl);
        ChargeStream streams = new ChargeStream();
        streams.start(properties, inputTopic, outputTopic);

        //
        // Step 2: Produce some input data to the input topic.
        //
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        IntegrationTestUtils.produceKeyValuesSynchronously(inputTopic, inputValues, producerConfig);

        //
        // Step 3: Verify the application's output data.
        //
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "generic-avro-integration-test-standard-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        List<KeyValue<String, GenericRecord>> actualValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig,
                outputTopic, inputValues.size());
        streams.stop();
        assertEquals(inputValues.get(0).value.get("txn_id"), actualValues.get(0).value.get("txn_id"));
    }
}
