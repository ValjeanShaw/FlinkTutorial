package com.lucky.apitest.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random


/**
 * 自定义SourceFunction
 */
class MySensorSource extends SourceFunction[WaterSensor] {
  var flg = true

  override def run(ctx: SourceFunction.SourceContext[WaterSensor]): Unit = {
    while (flg) {
      // 采集数据
      ctx.collect(
        WaterSensor(
          "sensor_" + new Random().nextInt(3),
          1577844001
        )
      )
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = {
    flg = false
  }
}

case class WaterSensor(sensorName:String,time:Long)
