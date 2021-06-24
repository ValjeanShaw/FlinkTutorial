package com.lucky.apitest.sink

import com.lucky.apitest.Sensor
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * sink 落成file
 */
object AddSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputFile = "/Users/xiaoran/meituancode/FlinkTutorial/src/main/scala/com/lucky/apitest/sensor.txt"
    val inputStream = env.readTextFile(inputFile)

    env.enableCheckpointing(60000)
    env.getCheckpointConfig
    //先转换成样例类类型
    val dataStream = inputStream.map {
      row =>
        val arr = row.split(",")
        Sensor(arr(0), arr(1).toInt, arr(2).toDouble)
    }

    dataStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path("sinkfile/sensor"),
        new SimpleStringEncoder[Sensor]
      ).build()
    ).setParallelism(1)

    val dataStringStream = inputStream.map {
      row =>
        val arr = row.split(",")
        (arr(0), arr(1).toInt, arr(2).toDouble).toString
    }

    dataStringStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path("sinkfile/sensorString"),
        new SimpleStringEncoder[String]
      ).build()
    ).setParallelism(1)

    env.execute()
  }
}
