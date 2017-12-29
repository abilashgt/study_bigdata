package kafka.producer

import java.util.Properties
import org.apache.kafka.clients.producer._


object KafkaProducer extends App {

  if(args.length<2){
    println("Please enter arguments: topic, message")
    sys.exit()
  }

  val  properties = new Properties()
  properties.put("bootstrap.servers", "localhost:9092")
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](properties)
  val record = new ProducerRecord(args(0), "key", args(1))
  producer.send(record)

  producer.close()
}
