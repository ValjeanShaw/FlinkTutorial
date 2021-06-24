package com.lucky.tableapitest

import com.lucky.apitest.Sensor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._




object InitTable {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //env.setParallelism(1)

    //读取数据
    val inputFile = "/Users/xiaoran/meituancode/FlinkTutorial/src/main/scala/com/lucky/apitest/sensor.txt"
    val inputStream:DataStream[String] = env.readTextFile(inputFile)

    //先转换成样例类类型
    val dataStream = inputStream.map{
      row=>
        val arr = row.split(",")
        Sensor(arr(0),arr(1).toInt,arr(2).toDouble)
    }


    //创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)
    //从stream中转换成为table
    val tableTemp = tableEnv.fromDataStream(dataStream)


    val result= tableTemp.select("name,date")
      .filter("name=='sensor1'")


    result.toAppendStream[(String,Int)].print()
    env.execute()
  }
}
