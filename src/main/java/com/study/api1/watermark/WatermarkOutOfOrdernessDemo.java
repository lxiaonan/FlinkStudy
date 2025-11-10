package com.study.api1.watermark;

import com.study.api1.transformation.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 乱序数据源
 */
public class WatermarkOutOfOrdernessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketed = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<WaterSensor> map = socketed.map(s -> {
            String[] split = s.split(",");
            return new WaterSensor(split[0], split[1]);
        });
        // 设置事件事件
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy
                // 乱序数据，等待3秒时间
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                // 自定义时间分配器，从数据中提取
                .withTimestampAssigner((element, recordTimestamp) -> {
                    System.out.println("element:" + element + " recordTimestamp:" + recordTimestamp);
                    // 时间单位：毫秒
                    return Long.parseLong(element.getTs()) * 1000;
                });
        SingleOutputStreamOperator<WaterSensor> singleOutputStreamOperator = map.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);
        singleOutputStreamOperator
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        String startTime = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
                        String endTime = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");
                        out.collect("key:" + s + "  " + startTime + "  " + endTime + "数量为：" + elements.spliterator().estimateSize() + "  元素：" + elements);
                    }
                }).print();
        env.execute();
    }
}
