package flink_datastream_demo

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowWordCount {
  def main(args: Array[String]) {
//
//    if(args.length != 3){
//      println("Usage: WindowWordCount <socketHost> <socketPort> <windowSeconds>")
//      System.exit(1)
//    }
//
//    val host = args(0)
//    val port = args(1).toInt
//    val windowSeconds = args(2).toInt
    val host = "node110"
    val port = 9999
    val windowSeconds = 5

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream(host, port)

    val counts = text
      .flatMap { line =>  line.toLowerCase.split("\\W+").filter ( word => word.nonEmpty ) }
      .map { word => (word, 1) }
      .keyBy(0)

      .timeWindow(Time.seconds(windowSeconds))

      .sum(1)


    counts.print()
    text.writeToSocket("",1,new SimpleStringSchema())
    env.execute("Window Stream WordCount")
  }
}