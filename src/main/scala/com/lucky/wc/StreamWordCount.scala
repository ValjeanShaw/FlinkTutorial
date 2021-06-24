package com.lucky.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * 流式处理的word count
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //创建流式处理的运行环境  StreamExecutionEnvironment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //flink util方式传参数
    val paramTool = ParameterTool.fromArgs(args)
    val host = paramTool.get("host")
    val port = paramTool.getInt("port")

    val inputDataStream = env.socketTextStream(host,port)

    //并行计算可以设置并行度，默认为当前机器cpu核数
    //env.setParallelism()

    //进行转换统计计算规则
    val resultDataStream = inputDataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .sum(1)


    resultDataStream.print().setParallelism(1)

    //开始执行    有始无终计算方式
    env.execute("stream word count job")

  }
}
