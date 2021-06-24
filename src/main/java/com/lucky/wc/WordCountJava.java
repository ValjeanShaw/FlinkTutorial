package com.lucky.wc;

import com.lucky.wc.function.MyFlatMapper;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 批处理word count
 */
public class WordCountJava {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        //把数据读取成为dataset数据集
        String inputPath = "/Users/xiaoran/meituancode/FlinkTutorial/src/main/resources/hellowordcount.txt";
        DataSet<String> dataSet = executionEnvironment.readTextFile(inputPath);

        //对数据集进行处理
        DataSet<Tuple2<String,Integer>> resultSet = dataSet.flatMap(new MyFlatMapper())
                .groupBy(0)     //按照位置进行分组
                .sum(1);          //将第二个位置的数据求和

        resultSet.print();

    }

}

