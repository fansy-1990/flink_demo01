package flink_kafka

import java.util.{Optional, Properties}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

object MyFlinkKafkaProducer {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "node100:9092")

    properties.setProperty("group.id", "test")


//    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val env = StreamExecutionEnvironment.createRemoteEnvironment("node110",8081,"C:\\Users\\yhsm\\.m2\\repository\\org\\apache\\flink\\flink-connector-kafka_2.11\\1.10.1\\flink-connector-kafka_2.11-1.10.1.jar",
    "C:\\Users\\yhsm\\.m2\\repository\\org\\apache\\flink\\flink-connector-kafka-base_2.11\\1.10.1\\flink-connector-kafka-base_2.11-1.10.1.jar",
    "C:\\Users\\yhsm\\.m2\\repository\\org\\apache\\kafka\\kafka-clients\\2.2.0\\kafka-clients-2.2.0.jar")
    val stream = env
      .addSource(new FlinkKafkaConsumer[String]("demo02", new SimpleStringSchema(), properties))

    val myProducer = new FlinkKafkaProducer[String](
      "demo01", // target topic
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()), //serialization schema
      properties,
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE)   //

    // versions 0.10+ allow attaching the records' event timestamp when writing them to Kafka;
    // this method is not available for earlier Kafka versions
    myProducer.setWriteTimestampToKafka(true)

    stream.addSink(myProducer)

    env.execute("read kafka and write to kafka ")
  }
}
