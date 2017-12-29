#!/usr/bin/env bash

# need fat jar
java -cp target/basic-spark.jar kafka.producer.KafkaProducer test-topic-name "test 1 test 1"