package cn.doitedu.flink.day10;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.planner.expressions.In;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 使用侧流输出，获取窗口中的迟到数据
 */
public class C08_GetEventTimeWindowLateData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1000,spark,1
        //1000,hive,2
        //2000,spark,3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> linesWithWatermark = lines.assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(
                        new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String element, long recordTimestamp) {
                                String[] fields = element.split(",");
                                return Long.parseLong(fields[0]);
                            }
                        }
                )
        );

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = linesWithWatermark.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));

            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        OutputTag<Tuple2<String, Integer>> lateTag = new OutputTag<Tuple2<String, Integer>>("late-data") {
        };

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(lateTag);

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.sum(1);

        DataStream<Tuple2<String, Integer>> lateStream = res.getSideOutput(lateTag);

        lateStream.print("late-data");

        res.print();

        env.execute();

    }
}
