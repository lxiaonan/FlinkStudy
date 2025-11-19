package com.study.api1.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 处理乱序的连接
 */
public class IntervalJoinWithLateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> stream1 = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] split = value.split(",");
                        return Tuple2.of(split[0], Integer.parseInt(split[1]));
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))// 允许乱序3s
                                .withTimestampAssigner((element, recordTimestamp) -> element.f1 * 1000L)// 字段二为事件事件
                );
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> stream2 = env.socketTextStream("localhost", 8888)
                .map(new MapFunction<String, Tuple3<String,Integer,Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(String value) throws Exception {
                        String[] split = value.split(",");
                        return Tuple3.of(split[0], Integer.parseInt(split[1]), Integer.parseInt(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))// 允许乱序3s
                                .withTimestampAssigner((element, recordTimestamp) -> element.f1 * 1000L)// 字段二为事件事件
                );
        KeyedStream<Tuple2<String, Integer>, String> k1 = stream1.keyBy(t -> t.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> k2 = stream2.keyBy(t -> t.f0);
        OutputTag<Tuple2<String, Integer>> left = new OutputTag<>("late_left", Types.TUPLE(Types.STRING, Types.INT));
        OutputTag<Tuple3<String, Integer, Integer>> right = new OutputTag<>("late_right", Types.TUPLE(Types.STRING, Types.INT, Types.INT));
        /**
         * 1、只支持事件时间
         * 2、指定上界、下界的偏移、负号代表时间往前，正号代表时间往后
         * 3、process中，只能处理join上的数据
         * 4、两条流关联后的watermark，以两条流中最小的为准
         * 5、如果 当前数据的事件时间<当前的watermark，就是迟到数据主流的 process不处理
         * = > between后，指定将 左流或右流的迟到数据 放入侧输出流
         */

        SingleOutputStreamOperator<String> process = k1
                .intervalJoin(k2)
                .between(Time.seconds(-3), Time.seconds(3))// 允许匹配上界加3s和下界减3s的数据
                .sideOutputLeftLateData(left)// k1延迟数据
                .sideOutputRightLateData(right)// k2延迟数据
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    /**
                     * Processes the elements of the two joined streams.
                     * @param left The left element of the joined pair.
                     * @param right The right element of the joined pair.
                     * @param ctx A context that allows querying the timestamps of the left, right and joined pair.
                     *     In addition, this context allows to emit elements on a side output.
                     * @param out The collector to emit resulting elements to.
                     * @throws Exception
                     */
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect("左流：" + left + " + 右流：" + right);
                    }
                });
        process.getSideOutput(left).printToErr("k1迟到");
        process.getSideOutput(right).printToErr("k2迟到");
        process.print();
        env.execute();
    }
}
