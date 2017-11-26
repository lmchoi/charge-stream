#!/bin/bash
CONFLUENT_HOME=/opt/confluent-3.3.1

${CONFLUENT_HOME}/bin/kafka-topics \
 --zookeeper localhost:2181 \
 --create --topic transactions-test \
 --partitions 1 \
 --replication-factor 1 \
 --config cleanup.policy=compact

${CONFLUENT_HOME}/bin/kafka-topics \
 --zookeeper localhost:2181 \
 --create --topic charges-test \
 --partitions 1 \
 --replication-factor 1 \
 --config cleanup.policy=compact

${CONFLUENT_HOME}/bin/kafka-topics \
 --zookeeper localhost:2181 \
 --create --topic failed-transactions-test \
 --partitions 1 \
 --replication-factor 1 \
 --config cleanup.policy=compact

