package com.lucky.apitest.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}


object SinkKafka{
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从kafka中读取数据
    val propertieConsumer = new Properties()
    propertieConsumer.setProperty("bootstrap.servers", "localhost:9092")
    propertieConsumer.setProperty("group.id","f-k")
    propertieConsumer.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    propertieConsumer.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    propertieConsumer.setProperty("auto.offset.reset", "latest")
    val dataKafka = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), propertieConsumer))


    val dataKafkaSink = dataKafka.map{
      row=>
        "flink transform the result: "+row
    }


    val propertieProducer = new Properties()
    propertieProducer.setProperty("bootstrap.servers", "localhost:9092")
    propertieProducer.setProperty("group.id","f-k")
    propertieProducer.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    propertieProducer.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    propertieProducer.setProperty("auto.offset.reset", "latest")

    dataKafkaSink.addSink(new FlinkKafkaProducer011[String]("sensor-producer",new SimpleStringSchema(),propertieProducer))

    env.execute("flink stream kafka source and sink")
  }
}
