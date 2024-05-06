package cn.doitedu.flink.day09;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;


public class C01_KafkaToKafkaExactlyOnceDemo {
    public static void main(String[] args) throws Exception {
        //开启checkpoint
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("D:\\ckpt");
        DataStreamSource<String> errorLines = env.socketTextStream("doitedu", 8888);
        //数据源
        SingleOutputStreamOperator<String> upperStream = errorLines.map(new MapFunction<String, String>() {

            @Override
            public String map(String value) throws Exception {
                if(value.equals("error")){
                    throw new RuntimeException("有错误数据，抛异常");
                }
                return value.toUpperCase();
            }
        });
        //kafka读取数据
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"doitedu:9092");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"earlist");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"g3701");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("topic-a", new SimpleStringSchema(), properties);
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);
        DataStreamSource<String> lines = env.addSource(kafkaConsumer);

        //数据etl
        SingleOutputStreamOperator<String> filtered = lines.filter(line -> !line.startsWith("error"));

        properties.setProperty("transaction.timeout.ms","600000");

        DataStream<String> res = upperStream.union(filtered);

        FlinkKafkaProducer<String> KafkaProducer = new FlinkKafkaProducer<String>("topic-b", new MyKafkaSerializationSchema("topic-b"), properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        res.addSink(KafkaProducer);

        env.execute();

    }
}
