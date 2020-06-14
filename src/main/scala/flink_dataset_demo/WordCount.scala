package flink_dataset_demo

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object WordCount {
  def main(args: Array[String]) {

//    if(args.length != 2){
//      println("Usage : WordCount <input> <output>")
//      System.exit(1)
//    }
//
//    val input = args(0)
//    val output = args(1)

    val input = "D:\\classes\\flink\\projects\\src\\main\\resources\\a.txt"
    val output = "D:\\classes\\flink\\projects\\target\\output00"


    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 获取输入数据
    val text = env.readTextFile(input)

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)


    counts.writeAsCsv(output, "\n", " ")
    env.execute("Batch WordCount")
  }
}