package com.study.window;

import com.study.api1.transformation.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TimeWindowsDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketed = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<WaterSensor> map = socketed.map(s -> {
            String[] split = s.split(",");
            return new WaterSensor(split[0], split[1]);
        });
        KeyedStream<WaterSensor, String> kb = map.keyBy(WaterSensor::getId);
        WindowedStream<WaterSensor, String, TimeWindow> window =
                kb
//                        .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))// 滚动窗口10秒
//                        .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))// 滑动窗口10秒，5秒滑动一次
//                        .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))) // 会话窗口,5秒内没有数据，则认为会话结束
                        .window(ProcessingTimeSessionWindows.withDynamicGap(w->{
                            return Integer.parseInt(w.getTs()) * 1000L;// 时间单位是毫秒
                        }))// 会话窗口，动态设置会话间隔
                ;
        SingleOutputStreamOperator<String> process = window.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                long start = context.window().getStart();
                long end = context.window().getEnd();
                String startTime = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
                String endTime = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");
                out.collect("key:" + s + "窗口:" + startTime + "~" + endTime + "数据:" + elements);
            }
        });
        process.print();
        env.execute();
    }
}
