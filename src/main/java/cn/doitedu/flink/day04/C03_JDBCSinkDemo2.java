package cn.doitedu.flink.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class C03_JDBCSinkDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //o1,300,手机
        //o2,400,化妆品
        //o2,400,化妆品
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple3<String, Double, String>> tpStream = lines.map(new MapFunction<String, Tuple3<String, Double, String>>() {
            @Override
            public Tuple3<String, Double, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[1], Double.parseDouble(fields[2]), fields[3]);
            }
        });

        SinkFunction<Tuple3<String, Double, String>> mysqlSink = JdbcSink.sink(
                "INSERT INTO tb_order_v2 values (?, ?, ?) on duolicate key update money = ?, name = ?",
                new JdbcStatementBuilder<Tuple3<String, Double, String>>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Tuple3<String, Double, String> tp) throws SQLException {
                        preparedStatement.setString(1, tp.f0);
                        preparedStatement.setDouble(2, tp.f1);
                        preparedStatement.setString(3, tp.f2);
                        preparedStatement.setDouble(4, tp.f1);
                        preparedStatement.setString(5, tp.f2);
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://doitedu:3306/doit?characterEncoding=utf-8")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("root")
                        .build()

        );

        tpStream.addSink(mysqlSink);
        env.execute();

    }
}
