package com.study.api1.socket;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 从socket中读取数据，一般用于流数据的测试
 */
public class SocketRead {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketed = env.socketTextStream("localhost", 9999);
        socketed.flatMap((String v, Collector<Tuple2<String, Integer>> out)->{
            String[] strings = v.split(" ");
            for (String string : strings) {
                out.collect(Tuple2.of(string,1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t->t.f0).sum(1).print();

        env.execute();
    }
}
