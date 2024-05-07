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

public class TestKafkaSource {
    public static void main(String[] args) throws Exception {

    Configuration configuration = new Configuration();
    configuration.setInteger(RestOptions.PORT, 18081);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/ckpt");


        //kafka读取数据
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"doitedu:9092");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"g3701");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("topic-a", new SimpleStringSchema(), properties);
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);
        DataStreamSource<String> lines = env.addSource(kafkaConsumer);

        lines.print();

        env.execute();
    }
}
