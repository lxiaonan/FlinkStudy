package com.study.window;

import com.study.api1.transformation.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 增量聚合函数和窗口函数结合:
 * 增量聚合Aggregate+全口process
 * 1、增量聚合函数处理数据:来一条计算一条
 * 2、窗口触发时, 增量聚合的结果(只有一条)传递给全窗口函数
 * 3、经过全窗口函数的处理包装后、输出
 * 结合两着的优点:
 * 1、增量聚合: 来一条计算一条。存储中间的计算结果。占用的空间少
 * 2、全窗口函数:可以通过上下文实现灵活的功能
 */
public class WindowAggregateAndProcessDemo {
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
        window.aggregate(new MyAdd(), new MyProcess()).print();
//        window.reduce(new MyReduce(), new MyProcessR());
        env.execute();
    }
    public static class MyAdd implements AggregateFunction<WaterSensor, Integer, String> {
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
    }

    public static class MyReduce implements ReduceFunction<WaterSensor> {
        @Override
        public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
            return new WaterSensor(value1.getId(), value1.getTs() + value2.getTs());
        }
    }
    public static class MyProcessR extends ProcessWindowFunction<WaterSensor, String, String, TimeWindow> {
        /**
         * 每个key的elements只有一条数据
         */
        @Override
        public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
            System.out.println("key:" + key);
            long start = context.window().getStart();
            long end = context.window().getEnd();
            String startTime = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
            String endTime = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");
            long size = elements.spliterator().estimateSize();
            String msg = "窗口:" + startTime + "~" + endTime + "  元素个数:" + size + "  元素:" + elements;
            out.collect(msg);
        }
    }

    /**
     * 输入就是聚合结果的输出
     */
    public static class MyProcess extends ProcessWindowFunction<String, String, String, TimeWindow> {
        /**
         * 每个key的elements只有一条数据
         */
        @Override
        public void process(String key, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
            System.out.println("key:" + key);
            long start = context.window().getStart();
            long end = context.window().getEnd();
            String startTime = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
            String endTime = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");
            long size = elements.spliterator().estimateSize();
            String msg = "窗口:" + startTime + "~" + endTime + "  元素个数:" + size + "  元素:" + elements;
            out.collect(msg);
        }
    }
}
