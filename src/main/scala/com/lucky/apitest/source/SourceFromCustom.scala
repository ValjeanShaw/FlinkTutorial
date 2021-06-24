package com.lucky.apitest.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object SourceFromCustom {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //自定义source
    val dataSourcePrivate = env.addSource(new MySensorSource())

    dataSourcePrivate.print()

    env.execute()
  }
}
