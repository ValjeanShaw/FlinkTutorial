package com.lucky.wc.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] arr = s.split(" ");
        //遍历所有word，组合成二元组输出
        for (String word : arr) {
            collector.collect(new Tuple2<>(word,1));
        }
    }
}
