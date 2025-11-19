package com.study.api1.process;

import com.study.api1.transformation.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class KeyProcessTimeDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> singleOutputStreamOperator = env.socketTextStream("localhost", 9999).map(line -> {
            String[] fields = line.split(",");
            return new WaterSensor(fields[0], fields[1]);
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, recordTimestamp) -> Long.parseLong(element.getTs()) * 1000)
        );
        KeyedStream<WaterSensor, String> keyBy = singleOutputStreamOperator.keyBy(WaterSensor::getId);
        keyBy.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                Long timestamp = ctx.timestamp();// 获取的是事件时间

                TimerService timerService = ctx.timerService();
                timerService.currentProcessingTime();// 获取当前系统时间
                // 注册一个5秒的定时器 事件时间
                timerService.registerEventTimeTimer(5000L);
                long watermark = timerService.currentWatermark();// 获取当前Watermark

                System.out.println("当前时间是：" + timestamp + "," + value.getId() + ":注册了一个5s的定时器" + "当前Watermark是：" + watermark);
                // 注册一个1秒的定时器 底层是处理时间
//                timerService.registerProcessingTimeTimer(1000L);
                // 删除定时器 事件时间
//                timerService.deleteEventTimeTimer(5000L);
                // 删除定时器 处理时间
//                timerService.deleteProcessingTimeTimer(1000L);
//                timerService.currentProcessingTime();// 获取当前系统时间
            }

            /**
             * 定时器触发 多个key会触发多次
             * 同一个key的相同定时器会合并
             * @throws Exception
             */
            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                System.out.println(ctx.getCurrentKey() + ":触发定时器");
            }
        });
        env.execute();
    }
}
