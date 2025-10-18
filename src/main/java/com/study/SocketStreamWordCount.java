package com.study;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SocketStreamWordCount {
    public static void main(String[] args) throws Exception {
        main2();
//        main1();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> localhost = env.socketTextStream("localhost", 9999);
//        localhost.flatMap((String v, Collector<Tuple2<String, Integer>> out ) -> {
//            String[] strings = v.split(" ");
//            for (String string : strings) {
//                out.collect(Tuple2.of(string, 1));
//            }
//        }).returns(Types.TUPLE(Types.STRING, Types.INT))
//                .keyBy(t->t.f0).sum(1).print();
//
//        env.execute();
    }

    public static void main3() throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = executionEnvironment.readTextFile("D:\\JavaProject\\FlinkStudy\\src\\main\\resources\\hello.txt");
        dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String string : s.split(" ")) {
                    collector.collect(Tuple2.of(string,1));
                }

            }
        }).groupBy(0).sum(1).print();
    }
    /**
     * 从文件中读取数据，进行单词计数
     * @throws Exception
     */
    public static void main1() throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("D:\\JavaProject\\FlinkStudy\\src\\main\\resources\\hello.txt");
        stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] strings = s.split(" ");
                for (String string : strings){
                    collector.collect(Tuple2.of(string,1));
                }
            }
        }).keyBy(t->t.f0).sum(1).print();
        executionEnvironment.execute();
    }
    /**
     * 从socket中读取数据，进行单词计数
     * @throws Exception
     */
    public static void main2() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.BIND_PORT,"8082");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
//        env.setParallelism(1);// 设置整体的并行度
        // 优先级：单独算子的并行度指定> 全局的并行度 > 提交时指定 > 配置文件
        DataStreamSource<String> localhost = env.socketTextStream("localhost", 9999);
        localhost.flatMap((String v, Collector<Tuple2<String, Integer>> out ) -> {
            String[] strings = v.split(" ");
            for (String string : strings) {
                out.collect(Tuple2.of(string, 1));
            }
        })
//                .setParallelism(2)// 设置flatMap的并行度
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t->t.f0).sum(1)
                .disableChaining()// 禁用算子链，keyBy不会与前后算子形成连接
                .print();

        env.execute();
    }
}
