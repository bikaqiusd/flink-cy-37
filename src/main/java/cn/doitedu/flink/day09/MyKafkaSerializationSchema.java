package cn.doitedu.flink.day09;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class MyKafkaSerializationSchema implements KafkaSerializationSchema<String> {
    private String topic;
    public MyKafkaSerializationSchema(String topic){
        this.topic = topic;
    }
    @Override
    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long aLong) {
        return new ProducerRecord<>(topic,element.getBytes(StandardCharsets.UTF_8));
    }
}
