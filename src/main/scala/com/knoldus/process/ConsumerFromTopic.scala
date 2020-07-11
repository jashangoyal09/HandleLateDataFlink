package com.knoldus.process

import java.sql.Timestamp
import java.util.Properties

import com.knoldus.models.{News, NewsDeserializer}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object ConsumerFromTopic extends App {
//  implicit val typeInfo = TypeInformation.of(classOf[News])
//  implicit val typeInfo1 = TypeInformation.of(classOf[Timestamp])
//  implicit val typeInfo112 = TypeInformation.of(classOf[String])
//  implicit val typeInfo11 = TypeInformation.of(classOf[Int])
  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.setProperty("zookeeper.connect", "localhost:2181")
  properties.setProperty("group.id", "test")
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(10)
  val stream = env
    .addSource(new FlinkKafkaConsumer[News]("my-topic", new NewsDeserializer(), properties))
    .keyBy(x=>x.priority)
    stream.countWindow(50).max("priority").setParallelism(10)
    .print()
  env.execute("Consume News from Topic")
}
