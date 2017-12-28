#!/usr/bin/env bash

# need fat jar
java -cp target/basic-spark.jar kafka.producer.KafkaProducerJava topic-name "test 2 test 2"