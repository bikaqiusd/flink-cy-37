package cn.doitedu.flink.day09;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
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
//        Configuration configuration = new Configuration();
//        configuration.setInteger(RestOptions.PORT, 18083);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/ckpt");
        DataStreamSource<String> errorLines = env.socketTextStream("doitedu", 8888);
        //数据源
        SingleOutputStreamOperator<String> upperStream = errorLines.map((MapFunction<String, String>) value -> {
            if(value.equals("error")){
                throw new RuntimeException("有错误数据，抛异常");
            }
            return value.toUpperCase();
        });
        //kafka读取数据
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"doitedu:9092");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"g3701");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("topic-a", new SimpleStringSchema(), properties);
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);
        DataStreamSource<String> lines = env.addSource(kafkaConsumer);

        //数据etl
        SingleOutputStreamOperator<String> filtered = lines.filter(line -> !line.startsWith("error"));
        //事务超时时间改为10分钟
        properties.setProperty("transaction.timeout.ms","600000");

        DataStream<String> res = upperStream.union(filtered);


        FlinkKafkaProducer<String> KafkaProducer = new FlinkKafkaProducer<String>("topic-b", new MyKafkaSerializationSchema("topic-b"), properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        Thread.sleep(1000);
        res.addSink(KafkaProducer);

        env.execute();

    }
}
