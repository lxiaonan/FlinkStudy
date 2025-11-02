package com.study.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;


public class FileSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        // 没有设置文件的状态不会改变为可读
        env.enableCheckpointing(200, CheckpointingMode.AT_LEAST_ONCE);// 启动检查点,状态检查点之间的时间间隔（毫秒）
        DataGeneratorSource<String> stringDataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long aLong) throws Exception {
                return "ces" + aLong;
            }
        }, Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1024),
                Types.STRING
        );
        DataStreamSource<String> stringDataStreamSource = env.fromSource(stringDataGeneratorSource, WatermarkStrategy.noWatermarks(), "FileSinkDemo");
        FileSink<String> fileSink = FileSink.<String>forRowFormat(new Path("D:/Flink"), new SimpleStringEncoder<>("UTF-8"))
                .withOutputFileConfig(new OutputFileConfig("part-", ".txt"))// 文件的前缀和后缀
                // 按照目录分桶
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        // 两个是OR的关系
                        .withRolloverInterval(Duration.ofSeconds(60))// 滚动时间间隔：每个文件写入60s
                        .withMaxPartSize(new MemorySize(1024 * 5))// 滚动文件大小：预计上限，会超过一点
                        .build()
                ).build();
        stringDataStreamSource.sinkTo(fileSink);
        env.execute();
    }
}
