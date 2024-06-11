package cn.doitedu.flink.day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class C20_KafkaSink01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("doitedu", 8888);

        SingleOutputStreamOperator<String> filtered = lines.filter(e -> !e.startsWith("error"));

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "doitedu:9092",
                "wordcount",
                new SimpleStringSchema()
        );

        filtered.addSink(kafkaProducer);
        env.execute();

    }
}
