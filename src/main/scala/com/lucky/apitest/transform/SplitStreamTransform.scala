package com.lucky.apitest.transform

import com.lucky.apitest.Sensor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object SplitStreamTransform {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val inputFile = "/Users/xiaoran/meituancode/FlinkTutorial/src/main/scala/com/lucky/apitest/sensor.txt"
    val inputStream = env.readTextFile(inputFile)

    //先转换成样例类类型
    val dataStream = inputStream.map{
      row=>
        val arr = row.split(",")
        Sensor(arr(0),arr(1).toInt,arr(2).toDouble)
    }

    //示例，按照0度划分成两个问题
    val splitStream = dataStream.split(data=>
      if(data.temperature>0) Seq("high") else Seq("low")
    )

    val highTempStream = splitStream.select("high")
    val lowTempStream = splitStream.select("low")

    //逗号分隔
    val allTempStream = splitStream.select("high","low")

    highTempStream.print("high")
    lowTempStream.print("low")
    allTempStream.print("all")

    env.execute()

  }
}
