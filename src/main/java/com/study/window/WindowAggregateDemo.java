package com.study.window;

import com.study.api1.transformation.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 窗口函数:增量聚合 Aggregate
 * 属于本窗口的第一条数据来，创建窗口，创建累加器
 * 增量聚合:来一条计算一条，调用一次add方法
 * 窗口输出时 调用一次getresult方法
 * 输入、中间累加器、输出 类型可以不一样。非常灵活
 */
public class WindowAggregateDemo {
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
         * 第一个类型:输入数据的类型
         * 第二个类型:累加器的类型，存储的中间计舞结果的类型
         * 第三个类型:输出的类型
         */
        window.aggregate(new AggregateFunction<WaterSensor, Integer, String>() {
            @Override
            public Integer createAccumulator() {
                System.out.println("创建累加器");
                return 0;
            }

            @Override
            public Integer add(WaterSensor value, Integer accumulator) {
                // 具体操作
                System.out.println(value);
                return Integer.parseInt(value.getTs()) + accumulator;
            }

            @Override
            public String getResult(Integer accumulator) {
                return "输出结果" + accumulator;
            }

            @Override
            public Integer merge(Integer a, Integer b) {
                // 只有会话窗口才会调用
                return 0;
            }
        }).print();
        env.execute();
    }
}
