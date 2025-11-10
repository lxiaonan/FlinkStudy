package com.study.api1.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class FilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> source = env.fromElements(new WaterSensor("s1", "19"), new WaterSensor("s2", "20"));
//        source.map(new MapFunction<WaterSensor, String>() {
//            @Override
//            public String map(WaterSensor waterSensor) throws Exception {
//                return waterSensor.getId();
//            }
//        })
        source.flatMap(new FlatMapFunction<WaterSensor, String>() {
                    @Override
                    public void flatMap(WaterSensor waterSensor, Collector<String> collector) throws Exception {
                        if (waterSensor.getId().equals("s1")) {
                            collector.collect(waterSensor.getId());
                        } else if (waterSensor.getId().equals("s2")) {
                            collector.collect(waterSensor.getId());
                            collector.collect(waterSensor.getTs());
                        }
                    }
                })
                .print();
        env.execute();
    }
}
