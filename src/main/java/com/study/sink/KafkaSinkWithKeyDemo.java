package com.study.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * Kafka sink:指定key的数据写入kafka
  */
public class KafkaSinkWithKeyDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        DataStreamSource<String> sock = env.socketTextStream("localhost", 9999);
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")// kafka地址
                // 自定义序列化器
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<String>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(String s, KafkaSinkContext kafkaSinkContext, Long aLong) {
                                String[] split = s.split(",");
                                byte[] key = split[0].getBytes(StandardCharsets.UTF_8);
                                byte[] value = split[1].getBytes(StandardCharsets.UTF_8);
                                return new ProducerRecord<>("sink", key, value);
                            }
                        }
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
