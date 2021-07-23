package com.lucky.window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        DataStream<Tuple2<String,Integer>> dataStream = environment.fromElements(Tuple2.of("class1", 0),
                Tuple2.of("class1", 1),
                Tuple2.of("class1", 2),
                Tuple2.of("class2", 3),
                Tuple2.of("class2", 4),
                Tuple2.of("class2", 5));
        
        //滚动时间窗口
        dataStream.keyBy(new UidKeySelector()).window(TumblingProcessingTimeWindows.of(Time.seconds(1))).sum(1).print();
        
//        //滑动时间窗口
        dataStream.keyBy(new UidKeySelector()).window(SlidingProcessingTimeWindows.of(Time.seconds(1),Time.seconds(1))).sum(1).print();

        //会话时间窗口
        dataStream.keyBy(new UidKeySelector()).window(EventTimeSessionWindows.withGap(Time.seconds(5))).sum(1).print();

        //滚动计数窗口
        dataStream.keyBy(new UidKeySelector()).countWindow(1).sum(1).print();

        //滑动计数窗口
        dataStream.keyBy(new UidKeySelector()).countWindow(2,1).sum(1).print();

        environment.execute();
    }


}

