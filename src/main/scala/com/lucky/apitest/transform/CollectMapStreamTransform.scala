package com.lucky.apitest.transform

import com.lucky.apitest.Sensor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object CollectMapStreamTransform {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val inputFile = "/Users/xiaoran/meituancode/FlinkTutorial/src/main/scala/com/lucky/apitest/sensor.txt"
    val inputStream = env.readTextFile(inputFile)

    /**
     * 转换两个结构不同的DataStream
     *
     */
    val dataStream = inputStream.map{
      row=>
        val arr = row.split(",")
        Sensor(arr(0),arr(1).toInt,arr(2).toDouble)
    }

    val highDataStream = dataStream.filter{
      row=>
        row.temperature>0
    }

    val lowDataStream = dataStream.filter{
      row=>
        row.temperature<=0
    }.map{
      row=>
        (row.name,row.date,row.temperature,"结冰报警")
    }

    //合流
    val connectedStreams = highDataStream.connect(lowDataStream)
    //合流后的coMap处理
    val coMapResultStream = connectedStreams.map(
      highData => (highData.date,highData.temperature),
      lowData => (lowData._2,lowData._4)
    )

    coMapResultStream.print("connect stream")





    //lowDataStream.union(highDataStream,)


    env.execute()
  }
}
