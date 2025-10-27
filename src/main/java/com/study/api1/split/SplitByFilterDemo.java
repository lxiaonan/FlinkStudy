package com.study.api1.split;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 分流：将奇数和偶数 拆分成不同的流
 */
public class SplitByFilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketed = env.socketTextStream("localhost", 9999);
        socketed.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String s, int i) {
                if(check(Integer.valueOf(s))){
                    return 0;
                }else {
                    return 1;
                }
            }

            private boolean check(Integer integer) {
                return integer % 2 == 0;
            }
        },t->t).print();
        env.execute();
    }
}
