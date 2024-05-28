package cn.doitedu.flink.day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Locale;
import java.util.Properties;
import java.util.logging.SimpleFormatter;

public class C01_KafkaDource01 {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 18083);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(1000);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"doitedu:9092");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"g3702");
        FlinkKafkaConsumerBase<String> KafkaConsumer = new FlinkKafkaConsumer<String>(
                "WordCount",
                new SimpleStringSchema(),
                properties
        );
        DataStreamSource<String> lines = env.addSource(KafkaConsumer);
        SingleOutputStreamOperator<String> upperStream = lines.map(line -> line.toUpperCase());
        upperStream.print();
        env.execute();
    }
}
