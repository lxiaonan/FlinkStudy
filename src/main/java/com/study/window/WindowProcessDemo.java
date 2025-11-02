package com.study.window;

import com.study.api1.transformation.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.DateFormat;

public class WindowProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> map = env.socketTextStream("localhost", 9999).map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                return new WaterSensor(value.split(",")[0], value.split(",")[1]);
            }
        });
        KeyedStream<WaterSensor, String> ks = map.keyBy(WaterSensor::getId);
        WindowedStream<WaterSensor, String, TimeWindow> window = ks.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        /**
         * 第1个参数:输入的数据类型
         * 第二个参数:输出的数据类型
         * 第三个参数:key的类型
         * 第四个参数:窗口类型
         */
        window.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            /**
             * 窗口触发计算时调用
             * @param key 窗口的key
             * @param context 上下文，里面能拿到窗口信息等
             * @param elements 窗口中的元素
             * @param out 采集器
             * @throws Exception
             */
            @Override
            public void process(String key, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                System.out.println("key:" + key);
                long start = context.window().getStart();
                long end = context.window().getEnd();
                String startTime = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
                String endTime = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");
                long size = elements.spliterator().estimateSize();
                String msg = "窗口:" + startTime + "~" + endTime + "  元素个数:" + size + "  元素:" + elements;
                out.collect(msg);
            }
        }).print();

        env.execute();
    }
}
