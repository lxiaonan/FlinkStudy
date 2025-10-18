package com.study.api1.aggreage;

import com.study.api1.transformation.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class KeyByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<WaterSensor> source = env.fromElements(new WaterSensor("s1", "19")
                , new WaterSensor("s2", "20")
                , new WaterSensor("s2", "21")
                , new WaterSensor("s3", "21"));
        DataStreamSink<WaterSensor> print = source.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {
                return waterSensor.getAge();
            }
        }).print();
        env.execute();
    }
}
