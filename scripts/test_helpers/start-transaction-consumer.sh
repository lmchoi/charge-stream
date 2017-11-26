#!/bin/bash
# PLEASE CHANGE ME
CONFLUENT_HOME=/opt/confluent-3.3.1

${CONFLUENT_HOME}/bin/kafka-console-consumer \
    --zookeeper localhost:2181 \
    --topic transactions-test \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property key.separator=: \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer