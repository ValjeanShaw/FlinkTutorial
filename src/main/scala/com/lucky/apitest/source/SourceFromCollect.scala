package com.lucky.apitest.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object SourceFromCollect {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从集合中读取数据
    val data1 = env.fromCollection(List("hello","leetcode","abc"))
    data1.print()

    //执行
    env.execute()
  }
}
