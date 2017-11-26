#!/bin/bash

CONFLUENT_HOME=/opt/confluent-3.3.1

# WARNING: either this script or the data in transaction.input may be DODGY,
# the deserializer in the code does not seem to play well with it...
${CONFLUENT_HOME}/bin/kafka-console-producer \
    --broker-list localhost:9092 \
    --topic transactions-test \
    --property parse.key=true \
    --property key.separator=: \
    --property key.schema='{"type":"string"}' \
    --property value.schema='{"type":"record","name":"Transaction","fields":[{"name":"txn_id","type":"string"}]}'
