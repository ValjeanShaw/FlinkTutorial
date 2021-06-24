package com.lucky.wc

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

//批处理的wordcount
object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    //从文件中读取数据
    val inputpath = "/Users/xiaoran/meituancode/FlinkTutorial/src/main/resources/hellowordcount.txt"
    val dataset = env.readTextFile(inputpath)

    val resultDataSet = dataset
      .flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)

    resultDataSet.print()
  }
}
