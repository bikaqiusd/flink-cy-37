package cn.doitedu.flink.day10;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class C05_KeyedProcessFunctionRegisterEventTimeDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.enableCheckpointing(5000);
        env.setParallelism(2);

        //1000,spark,1
        DataStreamSource<String> lines = env.socketTextStream("doitedu", 8888);

        SingleOutputStreamOperator<String> linesWithWaterMark = lines.assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                String[] fields = element.split(",");

                return Long.parseLong(fields[0]);
            }
        }));

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = linesWithWaterMark.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                if (value.contains("errer")) {
                    throw new RuntimeException("有错误数据，抛出异常！");
                }
                String[] fields = value.split(",");
                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                long currentWatermark = ctx.timerService().currentWatermark();

                long triggerTimer = currentWatermark + 10000;

                if (currentWatermark > Long.MIN_VALUE) {
                    System.out.println("当前的key：" + ctx.getCurrentKey() + "，当前的WaterMark是：" + currentWatermark + "，触发的时间：" + triggerTimer);
                    //注册定时器
                    ctx.timerService().registerEventTimeTimer(triggerTimer);
                }
            }
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                System.out.println("当前额key：" + ctx.getCurrentKey() + ", 触发的时间：" + timestamp);
            }
        });

        res.print();

        env.execute();


    }
}
