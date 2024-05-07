package cn.doitedu.flink.day09;

import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.protocol.types.Field;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class C02_JDBCExactlyOnceSinkDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        conf.setInteger(RestOptions.PORT, 18083);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //开启checkpoint
        env.enableCheckpointing(30000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/ckpt");

        //kafka数据源
        KafkaSource<String> soure = KafkaSource.<String>builder()
                .setBootstrapServers("doitedu:9092")
                .setTopics("tp-users")
                .setGroupId("mygroup02")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setProperty("commit.offsets.on.checkpoint", "false")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();



        DataStreamSource<String> lines = env.fromSource(soure, WatermarkStrategy.noWatermarks(), "Kafka Source");

        lines.print("kafka");

        //socket数据源
        DataStreamSource<String> lines2 = env.socketTextStream("doitedu", 8888);

        lines2.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if(value.contains("error")){
                    throw new RuntimeException("出异常了");
                }

                return value;
            }
        });

        DataStream<String> unionStream = lines.union(lines2);

        unionStream.print("unoin");

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> tpStream = unionStream.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                long id = Long.parseLong(fields[0]);
                String name = fields[1];
                int age = Integer.parseInt(fields[2]);
                return Tuple3.of(id, name, age);
            }
        });

        SinkFunction<Tuple3<Long, String, Integer>> mysqlSink = JdbcSink.exactlyOnceSink(
                "insert into tb_users (id, name, age) values (?, ?, ?)",
                new JdbcStatementBuilder<Tuple3<Long, String, Integer>>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Tuple3<Long, String, Integer> tp) throws SQLException {
                        preparedStatement.setLong(1, tp.f0);
                        preparedStatement.setString(2, tp.f1);
                        preparedStatement.setInt(3, tp.f2);

                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(0)
                        .build(),
                JdbcExactlyOnceOptions.builder().withTransactionPerConnection(true).build()
                , new SerializableSupplier<XADataSource>() {
                    @Override
                    public XADataSource get() {
                        MysqlXADataSource xaDataSource = new MysqlXADataSource();
                        xaDataSource.setUrl("jdbc:mysql://doitedu:3306/doit?serverTimezone=UTC&characterEncoding=UTF-8&useSSL=false&allowPublicKeyRetrieval=true");
                        xaDataSource.setUser("root");
                        xaDataSource.setPassword("root");
                        return xaDataSource;
                    }
                }

        );

        tpStream.addSink(mysqlSink);

        env.execute();


    }


}
