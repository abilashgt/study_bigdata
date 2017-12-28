package kafka

import conf.ASparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

/**
  * Created by Abilash George Thomas on 6/2/17.
  * Prerequesites:
  * * Code is for Local machine setup only
  * * Kafka and Hadoop should be started
  * * Kafka Group: test
  * * Kafka Topic: Hello-Kafka
  */
object KafkaWordCount {
  def main(args: Array[String]) {
    if (args.length < 4) {
      // e.g localhost:2181 group1 topic-name 2
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args
    val ssc = new StreamingContext(ASparkConf.sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    println("--- start consumer ---")
    ssc.start()
    ssc.awaitTermination()
  }
}