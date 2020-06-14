package flink_kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer


object MyFlinkKafkaConsumer {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "node100:9092")

    properties.setProperty("group.id", "test")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env
      .addSource(new FlinkKafkaConsumer[String]("demo02", new SimpleStringSchema(), properties))

    stream.print()

    env.execute("read kafka")
  }
}
