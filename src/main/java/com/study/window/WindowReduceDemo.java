package com.study.window;

import com.study.api1.transformation.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class WindowReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> map = executionEnvironment.socketTextStream("localhost", 9999)
                .map(s -> {
                    String[] split = s.split(",");
                    return new WaterSensor(split[0], split[1]);
                });

        KeyedStream<WaterSensor, String> ke = map.keyBy(s -> s.getId());
        WindowedStream<WaterSensor, String, TimeWindow> window = ke.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<WaterSensor> reduce = window.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                System.out.println("v1:" + value1 + " v2:" + value2);
                return new WaterSensor(value1.getId(), value1.getTs() + value2.getTs());
            }
        });
        reduce.print();
        executionEnvironment.execute();
    }
}
