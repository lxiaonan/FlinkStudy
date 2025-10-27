package com.study.api1.split;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 侧输出流
 * 需求:watersensor的数据，s1、s2的数据分别分开长
 * 步骤:
 * 1、使用process算子
 * 2、定义 outputTag对象
 * 3、调用 ctx.output
 * 4、通过主流 获取 测流
 */
public class SideOutPutStreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketed = env.socketTextStream("localhost", 9999);
        // 输入String 输出：转换成WaterSender
        SingleOutputStreamOperator<WaterSender> map = socketed.map((MapFunction<String, WaterSender>) s -> {
            String[] strings = s.split(",");
            return new WaterSender(strings[0], strings[1]);
        });
        /**
         * 创建侧输出流:参数一是侧输出流的名称，参数二是侧输出流的数据类型
         * 如果是普通数据类型，则不需要指定类型
         */
        OutputTag<WaterSender> s1 = new OutputTag<>("s1", Types.POJO(WaterSender.class));
        OutputTag<WaterSender> s2 = new OutputTag<>("s2", Types.POJO(WaterSender.class));
        SingleOutputStreamOperator<WaterSender> process = map.process(new ProcessFunction<WaterSender, WaterSender>() {
            @Override
            public void processElement(WaterSender waterSender, ProcessFunction<WaterSender, WaterSender>.Context context, Collector<WaterSender> collector) throws Exception {
                if (waterSender.id.equals("s1")) {
                    // 输出到侧输出流1
                    context.output(s1, waterSender);
                } else if (waterSender.id.equals("s2")) {
                    // 输出到侧输出流2
                    context.output(s2, waterSender);
                } else {
                    // 输出到主流
                    collector.collect(waterSender);
                }
            }
        });
        process.print();// 打印的是主流

        process.getSideOutput(s1).printToErr();// 侧输出流1
        process.getSideOutput(s2).printToErr();// 侧输出流2
        env.execute();
    }
}


