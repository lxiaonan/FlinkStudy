package com.study.api1.process;

import com.study.api1.transformation.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * 需要 求一个窗口内 水位线 出现次数的 top榜单
 */
public class CountNoDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> socket = env.socketTextStream("localhost", 9999).map(line -> {
            String[] fields = line.split(",");
            return new WaterSensor(fields[0], fields[1], Integer.valueOf(fields[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<WaterSensor>forMonotonousTimestamps()// 有序流
                .withTimestampAssigner((element, recordTimestamp) -> Long.parseLong(element.getTs()) * 1000))// 取输入为事件时间
                ;
        // 得到的是key,count,windowEndTime的三元组流
        /**
         * 先得到水位线，出现次数，归属时间窗口 这种格式的数据流
         */
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> aggregate = socket
                .keyBy(WaterSensor::getVc)// 根据水位线分组
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))// 滚动窗口，没有keyBy的是AllWindow全窗口
                .aggregate(new CountNoAggregate(), new CountNoWindowProcess())// 增量聚合，开窗后又变回普通的流，聚合之后是变成一条数据
                ;
        /**
         * 根据时间分组，来处理每一个时间段的数据top榜单
         */
        aggregate
                .keyBy(t -> t.f2)// 根据时间分组
                .process(new CountNoKeyProcess(2))// 处理每一个时间段数据
                .print();

        env.execute();
    }

    /**
     * 统计每一个key（水位线）出现的次数
     */
    public static class CountNoAggregate implements AggregateFunction<WaterSensor, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return 0;
        }
    }

    public static class CountNoWindowProcess extends ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow> {

        /**
         * 聚合之后的数据，窗口触发时，会调用这个方法
         *
         * @param key      The key for which this window is evaluated.
         * @param context  The context in which the window is being evaluated.
         * @param elements The elements in the window being evaluated.
         * @param out      A collector for emitting elements.
         * @throws Exception
         */
        @Override
        public void process(Integer key, ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>.Context context, Iterable<Integer> elements, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
//            keyBy之后的聚合开窗：是一条总的数据
            Integer count = elements.iterator().next();
            long endTime = context.window().getEnd();// 获取的是窗口的结束时间（归属的窗口时间）
            out.collect(Tuple3.of(key, count, endTime));// 输出
        }
    }

    public static class CountNoKeyProcess extends KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String> {
        // key: 数据的归属窗口时间 value: 数据
        Map<Long, List<Tuple3<Integer, Integer, Long>>> map;
        Integer topN;


        public CountNoKeyProcess(int topN) {
            this.map = new HashMap<>();
            this.topN = topN;
        }

        @Override
        public void processElement(Tuple3<Integer, Integer, Long> value, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.Context ctx, Collector<String> out) throws Exception {
//            System.err.println(ctx.getCurrentKey());
            Long windowEndTime = value.f2;// 获取的是窗口的结束时间（归属的窗口时间）
            // 装着数据：水位线，次数，归属时间
            List<Tuple3<Integer, Integer, Long>> tuple3s = map.get(windowEndTime);
            if (tuple3s == null) {
                tuple3s = new ArrayList<>();
                tuple3s.add(value);
                map.put(windowEndTime, tuple3s);
            }else{
                tuple3s.add(value);
            }
            // 注册一个定时器
            TimerService timerService = ctx.timerService();
            // 时间为大于窗口时间+1毫秒就触发
            timerService.registerEventTimeTimer(windowEndTime + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 获取当前的key，也就是窗口的结束时间
            Long windowEndTime = ctx.getCurrentKey();
            // 拿到里面的数据
            List<Tuple3<Integer, Integer, Long>> tuple3s = map.get(windowEndTime);
            // 排序
            tuple3s.sort(new Comparator<Tuple3<Integer, Integer, Long>>() {
                @Override
                public int compare(Tuple3<Integer, Integer, Long> o1, Tuple3<Integer, Integer, Long> o2) {
                    return o2.f1 - o1.f1;
                }
            });
            // 输出
            StringBuilder sb = new StringBuilder();
            sb.append("窗口：").append(windowEndTime / 1000).append("s").append("\n");
            for(int i = 0; i < Math.min(tuple3s.size(),topN); i++){
                Tuple3<Integer, Integer, Long> tuple3 = tuple3s.get(i);
                sb.append("top").append(i + 1).append(" 水位线：").append(tuple3.f0).append(" 次数：").append(tuple3.f1).append("\n");
            }
            out.collect(sb.toString());
        }

    }
}
