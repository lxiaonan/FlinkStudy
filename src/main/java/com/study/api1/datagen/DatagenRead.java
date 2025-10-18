package com.study.api1.datagen;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DatagenRead {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 如果有n个并行度，最大值设为a
        // 将数值 均分成 n份 a/n，
        // 比如，最大100，并行度2，每个并行度生成50个
        // 其中一个是0-49，另一个50-99
        env.setParallelism(2);// 设置并行度,n个算子，每个算子生成 总数据量/n
        DataGeneratorSource<String> stringDataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long aLong) throws Exception {
                return "number" + aLong;
            }
        },
                1000,// 生成1000条数据，从0开始自增
                RateLimiterStrategy.perSecond(1000),// 每秒生成1000条数据
                Types.STRING// 指定数据类型
        );

        env.fromSource(stringDataGeneratorSource, WatermarkStrategy.noWatermarks(),"DatagenRead").print();
        env.execute();
    }
}
