package com.study.api1.file;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 新的source写法:
 * env.fromSource(Source的实现类，Watermark，名字)
 */
public class FileResource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FileSource<String> build = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(),
                        new Path("D:\\JavaProject\\FlinkStudy\\src\\main\\resources\\hello.txt"))
                .build();
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);// 设置为批处理模式
        env.fromSource(build, WatermarkStrategy.noWatermarks(),"FileRead")
                .flatMap((String s, Collector<Tuple2<String, Integer>> collector) -> {
                    for (String string : s.split(" ")) {
                        collector.collect(Tuple2.of(string,1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(t->t.f0).sum(1).print();

        env.execute();
    }
}
