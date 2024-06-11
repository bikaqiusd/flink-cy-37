package cn.doitedu.flink.day02;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Locale;

public class C02_KafkaSource02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("doitedu:9092")
                .setTopics("wordcount")
                .setGroupId("g0721")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("enable.auto.commit", "true")
                .build();

        DataStreamSource<String> lines = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-Source");

        SingleOutputStreamOperator<String> res = lines.map(t -> t.toUpperCase(Locale.ROOT));
        res.print();

        env.execute();
    }
}
