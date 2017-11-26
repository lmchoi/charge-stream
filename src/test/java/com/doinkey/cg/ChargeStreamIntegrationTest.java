package com.doinkey.cg;

import com.doinkey.cg.domain.ChargeCalculator;
import com.doinkey.cg.domain.TransactionValidator;
import com.doinkey.cg.streams.ChargeStream;
import com.doinkey.cg.streams.StreamPropertiesBuilder;
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

    private static String INPUT_TOPIC = "transaction-topic";
    private static String OUTPUT_TOPIC = "charge-topic";
    private static String ERROR_TOPIC = "failed-transactions";
    private static Schema TRANSACTION_SCHEMA;

    private static Properties PRODUCER_CONFIG;
    private static Properties CONSUMER_CONFIG;

    private static Properties CHARGE_STREAM_CONFIG;

    private final TransactionValidator transactionValidator = new TransactionValidator();
    private final ChargeCalculator chargeCalculator = new ChargeCalculator();

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(INPUT_TOPIC);
        CLUSTER.createTopic(OUTPUT_TOPIC);

        String bootstrapServers = CLUSTER.bootstrapServers();
        String registryUrl = CLUSTER.schemaRegistryUrl();

        // producer for test input
        PRODUCER_CONFIG = createProducerConfig(bootstrapServers, registryUrl);
        // consumer for test output
        CONSUMER_CONFIG = createConsumerConfig(bootstrapServers, registryUrl);

        // stream being tested
        CHARGE_STREAM_CONFIG = StreamPropertiesBuilder
                .build("generic-avro-integration-test",
                        bootstrapServers,
                        registryUrl);

        TRANSACTION_SCHEMA = new Schema.Parser().parse(
                ChargeStreamIntegrationTest.class.getResourceAsStream("/com/doinkey/cg/transaction.avsc"));
    }

    @Test
    public void transactionShouldPassThroughToCharge() throws Exception {
        String validId = "good";

        // Step 1: Configure and start the processor topology.
        ChargeStream streams = new ChargeStream(transactionValidator, chargeCalculator);
        streams.start(CHARGE_STREAM_CONFIG, INPUT_TOPIC, OUTPUT_TOPIC, ERROR_TOPIC);

        // Step 2: Produce some input data to the input topic.
        GenericRecord record = new GenericData.Record(TRANSACTION_SCHEMA);
        record.put("txn_id", "lulz");
        List<KeyValue<String, GenericRecord>> inputValues = Collections.singletonList(new KeyValue<>(validId, record));
        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, inputValues, PRODUCER_CONFIG);

        // Step 3: Verify the application's output data.
        List<KeyValue<String, GenericRecord>> actualValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(CONSUMER_CONFIG,
                OUTPUT_TOPIC, inputValues.size());
        streams.stop();
        assertEquals(inputValues.get(0).value.get("txn_id"), actualValues.get(0).value.get("txn_id"));
    }

    @Test
    public void transactionWithInvalidIdShouldGoToErrorTopic() throws Exception {
        String invalidId = "bad";

        // Step 1: Configure and start the processor topology.
        ChargeStream streams = new ChargeStream(transactionValidator, chargeCalculator);
        streams.start(CHARGE_STREAM_CONFIG, INPUT_TOPIC, OUTPUT_TOPIC, ERROR_TOPIC);

        // Step 2: Produce some input data to the input topic.
        GenericRecord record = new GenericData.Record(TRANSACTION_SCHEMA);
        record.put("txn_id", "lulz");
        List<KeyValue<String, GenericRecord>> inputValues = Collections.singletonList(new KeyValue<>(invalidId, record));
        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, inputValues, PRODUCER_CONFIG);

        // Step 3: Verify the application's output data.
        List<KeyValue<String, GenericRecord>> actualValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(CONSUMER_CONFIG,
                ERROR_TOPIC, inputValues.size());
        streams.stop();
        assertEquals(inputValues.get(0).value.get("txn_id"), actualValues.get(0).value.get("txn_id"));
        assertEquals(TransactionValidator.BAD_KEY_MESSAGE, actualValues.get(0).value.get("error"));
    }

    private static Properties createProducerConfig(String bootstrapServers, String registryUrl) {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        return producerConfig;
    }

    private static Properties createConsumerConfig(String bootstrapServers, String registryUrl) {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "generic-avro-integration-test-standard-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        return consumerConfig;
    }
}
