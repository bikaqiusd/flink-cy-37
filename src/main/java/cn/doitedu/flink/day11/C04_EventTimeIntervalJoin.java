package cn.doitedu.flink.day11;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BaseBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 使用EventTime，实现Interval Join（即按照时间范围）
 *
 *
 * 先数据第二个流中的数据：9999端口的
 * 5000,d1,o1,手机,1000,1
 * 14999,d1,o1,手机,1000,1
 * 15000,d1,o1,手机,1000,1
 *
 * 第一个流中的数据
 * 10000,o1,已下单,2000
 *
 *
 * Interval Join 底层实现
 *
 * 将两个流按照相同的条件进行keyBy，然后connect到一起（共享状态）
 * 第一个流来数据了，将数据缓存到一个流对应的buffer，并设置定时器（超时就清除），
 * 然后到第二个流对应buffer中去找，如果有，就join上了
 *
 */
public class C04_EventTimeIntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //订单主表的流
        //10000,o1,已下单,2000
        DataStreamSource<String> lines1 = env.socketTextStream("localhost", 8888);

        //提取EventTime，生成WaterMark
        SingleOutputStreamOperator<String> orderMainLinesWithWaterMark = lines1.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner((element, recordTimestamp) -> {
            String[] fields = element.split(",");
            return Long.parseLong(fields[0]);
        }));

        SingleOutputStreamOperator<Tuple3<String, String, Double>> orderMainStream = orderMainLinesWithWaterMark.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[1], fields[2], Double.parseDouble(fields[3]));
            }
        });

        //5000,d1,o1,手机,1000,1
        //14999,d1,o1,手机,1000,1
        //15000,d1,o1,手机,1000,1
        DataStreamSource<String> lines2 = env.socketTextStream("localhost", 9999);

        //提取EventTime，生成WaterMark
        SingleOutputStreamOperator<String> orderDetailLinesWithWaterMark = lines2.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner((element, recordTimestamp) -> {
            String[] fields = element.split(",");
            return Long.parseLong(fields[0]);
        }));

        SingleOutputStreamOperator<Tuple5<String, String, String, Double, Integer>> orderDetailStream = orderDetailLinesWithWaterMark.map(new MapFunction<String, Tuple5<String, String, String, Double, Integer>>() {
            @Override
            public Tuple5<String, String, String, Double, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple5.of(fields[1], fields[2], fields[3], Double.parseDouble(fields[4]), Integer.parseInt(fields[5]));
            }
        });

        SingleOutputStreamOperator<Tuple6<String, String, String, Double, Integer, String>> res = orderMainStream.keyBy(t -> t.f0)
                .intervalJoin(orderDetailStream.keyBy(t -> t.f1))
                .between(Time.seconds(-5), Time.seconds(5))
                .upperBoundExclusive()
                .process(new ProcessJoinFunction<Tuple3<String, String, Double>, Tuple5<String, String, String, Double, Integer>, Tuple6<String, String, String, Double, Integer, String>>() {
                    @Override
                    public void processElement(Tuple3<String, String, Double> left, Tuple5<String, String, String, Double, Integer> right, Context ctx, Collector<Tuple6<String, String, String, Double, Integer, String>> out) throws Exception {
                        out.collect(Tuple6.of(right.f0, right.f1, right.f2, right.f3, right.f4, left.f1));
                    }
                });

        res.print();
        env.execute();


    }
}
