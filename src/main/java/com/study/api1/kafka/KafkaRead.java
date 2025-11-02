package com.study.api1.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaRead {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> build = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("sink")//  topic
                .setGroupId("test-group")
                .setValueOnlyDeserializer(new SimpleStringSchema())// 值反序列化
//                .setStartingOffsets(OffsetsInitializer.earliest())// 读最早的消息
                .setStartingOffsets(OffsetsInitializer.earliest())// 读最新的消息
                .build();
        env.fromSource(build, WatermarkStrategy.noWatermarks(),"kafkaTest").print();
        env.execute();
    }
}
