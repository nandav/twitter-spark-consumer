
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import org.apache.spark.sql

import kafka.serializer.StringDecoder // this has to come before streaming.kafka import
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

import scala.collection.JavaConversions._
import java.util.List

import java.util.Date


object KafkaSparkStreaming {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaSparkStreaming")

    val kafka_broker = sparkConf.get("spark.kafka.broker")
    val kafka_topic = sparkConf.get("spark.kafka.topic")
   
   // Create spark streaming context with 10 second batch interval
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    
    
    // Create direct kafka stream with brokers and topics
    val topicsSet = Set[String] (kafka_topic)
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafka_broker)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)


    // Get the lines, split them into words, count the words and print
    val wordCounts = messages.map(_._2) // split the message into lines
      .flatMap(_.split(" ")) //split into words
      .filter(_.startsWith("#"))


    val topCounts60 = wordCounts.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))

    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })


    // start processing
    ssc.start() // start the streaming context
    ssc.awaitTermination() // block while the context is running (until it's stopped by the timer)
  }
}
