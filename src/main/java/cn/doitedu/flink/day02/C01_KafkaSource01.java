package cn.doitedu.flink.day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Locale;
import java.util.Properties;

public class C01_KafkaSource01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"doitedu:9092");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earlist");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"g3701");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "wordcount",
                new SimpleStringSchema(),
                properties
        );

        DataStreamSource<String> lines = env.addSource(kafkaConsumer);

        SingleOutputStreamOperator<String> res = lines.map(line -> line.toUpperCase(Locale.ROOT));

        res.print();

        env.execute();


    }
}
