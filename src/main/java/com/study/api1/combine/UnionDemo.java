package com.study.api1.combine;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * union:合并数据流
 * 1、流的数据类型必须一致
 * 2、一次可以合并多条流
 */
public class UnionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> s1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> s2 = env.fromElements(6, 7, 8, 9, 10);
        DataStreamSource<Integer> s3 = env.fromElements(6, 7, 8, 9, 10);
        DataStreamSource<String> s13 = env.fromElements("1", "2", "3", "4", "5");
        s1.union(s2,s3).print("union:");
        s13.print("single:");
        env.execute();
    }
}
