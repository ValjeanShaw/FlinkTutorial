package com.lucky.apitest.transform

import com.lucky.apitest.Sensor
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object ReduceStreamTransform {
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

    //使用reduce方法做计算   计算出当前最小的温度值，以及最近的时间
    val resultStream = dataStream.keyBy("name")
      .reduce((curState,newData)=>
        Sensor(curState.name,newData.date,curState.temperature.min(newData.temperature))
      )

    resultStream.print()

    env.execute()
  }
}

/**
 * 自定义reduce
 */
class MyReduceFunction extends ReduceFunction[Sensor] {
  override def reduce(t: Sensor, t1: Sensor): Sensor =
    Sensor(t.name,t1.date,t1.temperature.min(t.temperature))
}