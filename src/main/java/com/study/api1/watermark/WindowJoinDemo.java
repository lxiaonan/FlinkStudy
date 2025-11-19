package com.study.api1.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 窗口连接
 */
public class WindowJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> stream1 = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 11),
                Tuple2.of("b", 4),
                Tuple2.of("c", 5),
                Tuple2.of("d", 11)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.f1 * 1000L)
        );
        SingleOutputStreamOperator<Tuple3<String, Integer,Integer>> stream2 = env.fromElements(
                Tuple3.of("a", 1,1),
                Tuple3.of("b", 4,44),
                Tuple3.of("c", 5,55),
                Tuple3.of("d", 14,66)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.f1 * 1000L)
        );
        DataStream<String> apply = stream1.join(stream2)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new FlatJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    @Override
                    public void join(Tuple2<String, Integer> first, Tuple3<String, Integer, Integer> second, Collector<String> out) throws Exception {
                        out.collect("s1" + first + "s2" + second);
                    }
                });
        apply.print();
        env.execute();
    }
}
