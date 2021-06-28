package com.lucky.wc;

import com.lucky.wc.function.MyFlatMapper;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountDataStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        String inputPath = "/Users/xiaoran/meituancode/FlinkTutorial/src/main/resources/hellowordcount.txt";
        DataStream<String> dataStream = env.readTextFile(inputPath);

        //基于流式数据进行转换计算
        DataStream<Tuple2<String, Integer>> streamResult = dataStream.flatMap(new MyFlatMapper())
                .keyBy(0)
                .sum(1);

        streamResult.print();

        env.execute();
    }
}
