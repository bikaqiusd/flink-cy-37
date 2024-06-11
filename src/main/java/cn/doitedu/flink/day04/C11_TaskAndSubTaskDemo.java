package cn.doitedu.flink.day04;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class C11_TaskAndSubTaskDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置执行环境的并行度为2
        env.setParallelism(2);
        //KakfaSource（多并行的Source）
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("doitedu:9092")
                .setTopics("wordcount")
                .setGroupId("g01")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema()) //反序列化实现类
                .build();

        DataStreamSource<String> lines = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafkaSource");



        SingleOutputStreamOperator<Tuple2<String, Integer>> res = lines.map(w -> Tuple2.of(w, 1)).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .sum(1);

        res.print().setParallelism(1);

        env.execute();



    }
}
