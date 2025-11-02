package com.study.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * Kafka sink:
 * 注意:如果要使用 精准一次 写入Kafka，需要满足以下条件，缺一不可米
 * 1、开启checkpoint  env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
 * 2、设置事务前缀  .setTransactionalIdPrefix("sink-")
 * 3、设置事务超时时间:checkpoint间隔 < 事务超时时间 < max的15分钟  .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,10*60*1000+"")
 */
public class KafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        DataStreamSource<String> sock = env.socketTextStream("localhost", 9999);
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")// kafka地址
                // 设置序列化器,里面包括设置topic、具体的序列化器
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("sink")
                                .setValueSerializationSchema(new SimpleStringSchema())// 值序列化器
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)// 写到kafka的一致性级别:精准一次、至少一次
                .setTransactionalIdPrefix("sink-")// 如果是 精准一次，必须设置 事务id前缀
                // 如果是精准一次，必须设置 事务超时时间:大于checkpoint间隔，小于 max 15分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,10*60*1000+"")
                .build();
        sock.sinkTo(kafkaSink);
        env.execute();
    }
}
