package com.lucky.apitest.transform

import com.lucky.apitest.Sensor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object KeyedStreamTransform {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //读取数据
    val inputFile = "/Users/xiaoran/meituancode/FlinkTutorial/src/main/scala/com/lucky/apitest/sensor.txt"
    val inputStream = env.readTextFile(inputFile)

    //先转换成样例类类型
    val dataStream = inputStream.map{
      row=>
        val arr = row.split(",")
        Sensor(arr(0),arr(1).toInt,arr(2).toDouble)
    }

    //分组聚合，keyBy
    val aggStream = dataStream.keyBy("name")


    //sum的效果
    println("------min-------")
    val sumStream = aggStream.sum("temperature")
    sumStream.print()

    //min的效果
    println("------min-------")
    val minStream = aggStream.min("temperature")
    minStream.print()

    println("------minBy-------")
    val minByStream = aggStream.minBy("temperature")
    minByStream.print()

    println("------max-------")
    val maxStream = aggStream.max("temperature")
    maxStream.print()

    println("------maxBy-------")
    val maxByStream = aggStream.maxBy("temperature")
    maxByStream.print()

    env.execute()
  }
}
