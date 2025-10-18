package com.study.api1.transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> source = env.fromElements(new WaterSensor("s1", "19"), new WaterSensor("s2", "20"));
//        source.map(new MapFunction<WaterSensor, String>() {
//            @Override
//            public String map(WaterSensor waterSensor) throws Exception {
//                return waterSensor.getId();
//            }
//        })
        source.map(WaterSensor::getId)
                .print();
        env.execute();
    }
}
