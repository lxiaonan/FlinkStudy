package com.study.api1.combine;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 连接两条流，输出能根据id匹配上的数据(类似inner join效果)
 * 1、流数据类型可以不一致
 * 2、一次只能合并两条流
 * 3、连接后可以调用 map、flatmap、process来处理，但是各处理各的
 */
public class ConnectKeyByDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Integer, String>> tuple2DataStreamSource = env.fromElements(
                new Tuple2<Integer, String>(1, "a")
                , new Tuple2<Integer, String>(1, "b")
                , new Tuple2<Integer, String>(2, "c")
                , new Tuple2<Integer, String>(3, "d"));
        DataStreamSource<Tuple3<Integer, String, Integer>> tuple3DataStreamSource = env.fromElements(
                new Tuple3<Integer, String, Integer>(1, "aa", 18)
                , new Tuple3<Integer, String, Integer>(1, "bb", 19)
                , new Tuple3<Integer, String, Integer>(2, "cc", 19)
                , new Tuple3<Integer, String, Integer>(3, "dd", 20));
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connectedStreams = tuple2DataStreamSource.connect(tuple3DataStreamSource)
        //多并行度下，需要根据 关联条件进行 keyby，才能保证 key相同的数据到一起去，才能匹配上
                // 没有分组的话，可能相同的数据会分到不同的子任务，导致数据无法匹配上
        .keyBy(k1 -> k1.f0, k2 -> k2.f0);
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> process = connectedStreams.process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, Tuple4<Integer, String, String, Integer>>() {
            Map<Integer, List<Tuple2<Integer, String>>> map2 = new HashMap<>();
            Map<Integer, List<Tuple3<Integer, String, Integer>>> map3 = new HashMap<>();

            @Override
            public void processElement1(Tuple2<Integer, String> tuple2, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, Tuple4<Integer, String, String, Integer>>.Context context, Collector<Tuple4<Integer, String, String, Integer>> collector) throws Exception {
                Integer f0 = tuple2.f0;
                if (map2.containsKey(f0)) {
                    map2.get(f0).add(tuple2);
                } else {
                    List<Tuple2<Integer, String>> list = new ArrayList<>();
                    map2.put(f0, list);
                    list.add(tuple2);
                }
                if (map3.containsKey(f0)) {
                    for (Tuple3<Integer, String, Integer> tuple3 : map3.get(f0)) {
                        collector.collect(new Tuple4<>(f0, tuple2.f1, tuple3.f1, tuple3.f2));
                    }
                }
            }

            @Override
            public void processElement2(Tuple3<Integer, String, Integer> tuple3, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, Tuple4<Integer, String, String, Integer>>.Context context, Collector<Tuple4<Integer, String, String, Integer>> collector) throws Exception {
                Integer f0 = tuple3.f0;
                if (map3.containsKey(f0)) {
                    map3.get(f0).add(tuple3);
                } else {
                    List<Tuple3<Integer, String, Integer>> list = new ArrayList<>();
                    map3.put(f0, list);
                    list.add(tuple3);
                }
                if (map2.containsKey(f0)) {
                    for (Tuple2<Integer, String> tuple2 : map2.get(f0)) {
                        collector.collect(new Tuple4<>(f0, tuple2.f1, tuple3.f1, tuple3.f2));
                    }
                }
            }
        });
        process.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
