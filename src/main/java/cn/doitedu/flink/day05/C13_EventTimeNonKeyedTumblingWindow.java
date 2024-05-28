package cn.doitedu.flink.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

public class C13_EventTimeNonKeyedTumblingWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("doitedu", 9999);
        lines.print();

        SingleOutputStreamOperator<String> linesWithWaterMark = lines.assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(
                        new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String element, long l) {
                                String[] fields = element.split(",");

                                return Long.parseLong(fields[0]);
                            }
                        }
                )
        );

        SingleOutputStreamOperator<Integer> numStream = linesWithWaterMark.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                String[] fields = value.split(",");

                return Integer.parseInt(fields[1]);
            }
        });

        AllWindowedStream<Integer, TimeWindow> windowedStream = numStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<Integer> rest = windowedStream.sum(0);

        rest.print();
        env.execute();

    }
}
