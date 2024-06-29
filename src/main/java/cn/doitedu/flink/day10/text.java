package cn.doitedu.flink.day10;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class text {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(30000);

        //1000,spark,1
        DataStreamSource<String> lines = env.socketTextStream("doitedu", 8888);

        WatermarkStrategy<String> stringWatermarkStrategy = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                String[] fields = element.split(",");

                return Long.parseLong(fields[0]);
            }
        });

        SingleOutputStreamOperator<String> linesWithWaterMark = lines.assignTimestampsAndWatermarks(stringWatermarkStrategy);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = linesWithWaterMark.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                if (value.contains("error")) {
                    throw new RuntimeException("有错误数据，抛出异常！");
                }
                String[] fields = value.split(",");
                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
            }
        });

        tpStream.print();

        env.execute();
    }
}
