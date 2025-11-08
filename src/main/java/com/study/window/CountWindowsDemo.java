package com.study.window;

import com.study.api1.transformation.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class CountWindowsDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketed = env.socketTextStream("localhost", 9999);
        KeyedStream<WaterSensor, String> kb = socketed.map(s -> {
            String[] split = s.split(",");
            return new WaterSensor(split[0], split[1]);
        }).keyBy(WaterSensor::getId);
        WindowedStream<WaterSensor, String, GlobalWindow> globalWindowWindowedStream =
                kb
//                        .countWindow(5) // 滚动窗口5个元素
                        .countWindow(5, 2)// 滑动窗口5个元素，2个元素滑动一次；(每经过一个步长，都有一个窗口触发输出，第一次输出在第2条数据来的时候）
                ;
        SingleOutputStreamOperator<String> process = globalWindowWindowedStream.process(new ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                long processingTime = context.currentProcessingTime();
                String format = DateFormatUtils.format(processingTime, "yyyy-MM-dd HH:mm:ss");
                out.collect("key:" + s + "  " + format + "元素：" + elements);
            }
        });
        process.print();
        env.execute();
    }
}
