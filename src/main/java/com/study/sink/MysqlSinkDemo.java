package com.study.sink;

import com.study.api1.transformation.WaterSensor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 写入mysql
 */
public class MysqlSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> map = env.socketTextStream("localhost", 9999).map(s -> {
            String[] split = s.split(",");
            return new WaterSensor(split[0], split[1]);
        });
    // 使用JdbcSink创建MySQL数据接收器
        SinkFunction<WaterSensor> sink = JdbcSink.sink(
                // 定义插入SQL语句，向ws表插入两个字段的值
                "INSERT INTO ws value (?,?)" //执行sql
                // 定义如何将WaterSensor对象映射到SQL参数
                , new JdbcStatementBuilder<WaterSensor>() {
                    // 替换sql中的参数
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor o) throws SQLException {
                        // 将WaterSensor的第一个字段设置为第一个参数
                        preparedStatement.setString(1, o.getId());
                        // 将WaterSensor的第二个字段设置为第二个参数
                        preparedStatement.setString(2, o.getTs());
                    }
                },
                // 配置JDBC执行选项
                new JdbcExecutionOptions.Builder()// 执行参数可以使用默认的
                        .withBatchIntervalMs(3000)// 设置批量执行时间间隔为3000毫秒
                        .withBatchSize(3)// 设置批量大小为3条记录，与时间间隔是or的关系
                        .withMaxRetries(3)// 设置失败时最大重试次数为3次
                        .build()
                // 配置JDBC连接选项
                , new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/map?useUnicode=true&characterEncoding=utf-8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai") // 数据库连接URL
                        .withUsername("root") // 数据库用户名
                        .withPassword("200358") // 数据库密码
                        .withConnectionCheckTimeoutSeconds(60) // 设置连接检查超时时间为60秒
                        .build());

        map.addSink(sink);
        env.execute();
    }
}
