package cn.doitedu.flink.day08;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

/**
 * 通过各个游戏、各个分区充值金额，并且将数据写入到MySQL
 * 1.数据倾斜问题（加盐）
 * 2.频繁写入MySQL导致写入延迟太高（划分窗口）
 */
public class C04_GameCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);
        /**
         * g001,z01,u001,300
         * g001,z02,u011,100
         * g002,z01,u001,100
         * g001,z03,u012,100
         * g001,z03,u012,100
         * g001,z03,u013,100
         */

        DataStreamSource<String> lines = env.socketTextStream("doitedu", 8888);

        SingleOutputStreamOperator<Tuple4<String, String, Integer, Double>> tpStream = lines.map(new MapFunction<String, Tuple4<String, String, Integer, Double>>() {
            private Random random = new Random();

            @Override
            public Tuple4<String, String, Integer, Double> map(String value) throws Exception {

                String[] fields = value.split(",");
                int i = random.nextInt(8);
                return Tuple4.of(fields[0], fields[1], i, Double.parseDouble(fields[3]));
            }
        });

        KeyedStream<Tuple4<String, String, Integer, Double>, Tuple3<String, String, Integer>> keyedStream1 = tpStream.keyBy(new KeySelector<Tuple4<String, String, Integer, Double>, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> getKey(Tuple4<String, String, Integer, Double> value) throws Exception {
                return Tuple3.of(value.f0, value.f1, value.f2);
            }
        });

        WindowedStream<Tuple4<String, String, Integer, Double>, Tuple3<String, String, Integer>, TimeWindow> windowedStream = keyedStream1.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<Tuple4<String, String, Integer, Double>> sumStream = windowedStream.sum(3);
        SingleOutputStreamOperator<Tuple3<String, String, Double>> gidZidAndMoneyStream = sumStream.map(new MapFunction<Tuple4<String, String, Integer, Double>, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(Tuple4<String, String, Integer, Double> value) throws Exception {
                return Tuple3.of(value.f0, value.f1, value.f3);
            }
        });

        KeyedStream<Tuple3<String, String, Double>, Tuple2<String, String>> keyedStream2 = gidZidAndMoneyStream.keyBy(new KeySelector<Tuple3<String, String, Double>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, Double> value) throws Exception {
                return Tuple2.of(value.f0, value.f1);
            }
        });

        SingleOutputStreamOperator<Tuple3<String, String, Double>> res = keyedStream2.sum(2);


        SinkFunction<Tuple3<String, String, Double>> mysqlSink = JdbcSink.sink(
                "INSERT INTO tb_game_income_v2 (gid, zid, money) values (?, ?, ?) on duplicate key update money = ?",
                new JdbcStatementBuilder<Tuple3<String, String, Double>>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Tuple3<String, String, Double> tp) throws SQLException {
                        preparedStatement.setString(1, tp.f0);
                        preparedStatement.setString(2, tp.f1);
                        preparedStatement.setDouble(3, tp.f2);
                        preparedStatement.setDouble(4, tp.f2);


                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://doitedu:3306/doit?characterEncoding=utf-")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("root")
                        .build()
        );

        res.addSink(mysqlSink);

        env.execute();




    }
}
