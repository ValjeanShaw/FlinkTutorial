package com.lucky.apitest.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object SourceFromFile {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataFile = env.readTextFile("/Users/xiaoran/meituancode/FlinkTutorial/src/main/scala/com/lucky/apitest/source/data.log")
    dataFile.print()
    env.execute()
  }
}
