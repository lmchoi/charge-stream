package com.doinkey.cg.app;

import com.doinkey.cg.app.domain.ChargeCalculator;
import com.doinkey.cg.app.domain.TransactionValidator;
import com.doinkey.cg.streams.Topic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

class ChargeStream {

    private KafkaStreams streams;

    private final TransactionValidator transactionValidator;
    private final ChargeCalculator chargeCalculator;

    ChargeStream(TransactionValidator transactionValidator, ChargeCalculator chargeCalculator) {
        this.transactionValidator = transactionValidator;
        this.chargeCalculator = chargeCalculator;
    }

    void start(Properties streamsConfiguration,
               Topic<String, Transaction> inputTopic,
               Topic<String, Charge> outputTopic,
               Topic<String, FailedTransaction> errorTopic) {
        streams = createProcessTopology(streamsConfiguration, inputTopic, outputTopic, errorTopic);
        streams.start();
    }

    private KafkaStreams createProcessTopology(Properties streamsConfiguration,
                                               Topic<String, Transaction> inputTopic,
                                               Topic<String, Charge> outputTopic,
                                               Topic<String, FailedTransaction> errorTopic) {
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, Transaction> transactionStream = inputTopic.stream(builder);

        KStream<String, Transaction>[] validatedTransactions = transactionStream.branch(
                (k, v) -> transactionValidator.isKeyBad(k),
                (k, v) -> true);

        errorTopic.send(validatedTransactions[0]
                .mapValues(t -> new FailedTransaction(t.getTxnId(), TransactionValidator.BAD_KEY_MESSAGE)));

        outputTopic.send(validatedTransactions[1]
                .mapValues(chargeCalculator::calculate));

        return new KafkaStreams(builder, streamsConfiguration);
    }

    void stop() {
        if (streams != null) {
            streams.close();
        }
    }
}
