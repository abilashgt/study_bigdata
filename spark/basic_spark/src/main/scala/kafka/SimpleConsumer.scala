package kafka;


import java.util.{Arrays, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConversions._


/**
  * Created by 984620 on 30/1/17.
  * Environment:
  * * Apache Spark 2.10
  * * Kaf
  */
object SimpleConsumer {
  def main(args: Array[String]): Unit ={
    //Kafka consumer configuration settings
    val props = new Properties();
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "test")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer = new KafkaConsumer[String, String](props)

    // topic
    val topicName = "Hello-Kafka" //args(0).toString
    consumer.subscribe(Arrays.asList(topicName))
    println("Subscribed to topic " + topicName)

    while (true) {
      val records = consumer.poll(100)

      for (record<-records){
        // print the offset,key and value for the consumer records.
        println("record="+record)
        println("offset = "+record.offset()+", key = "+record.key()+", value = "+record.value()+"\n")
      }
    }
  }
}
