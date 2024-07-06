package cn.doitedu.flink.day11;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.planner.expressions.In;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * EventTime类型滚动窗口进行LeftOuteroin
 *
 * 两个数据流：
 *
 * 第一个流：9999端口 (通常是将数据量比较多的流当成左表)：订单明细表(时间,订单明细id, 订单id, 分类名称，单价，数量)
 *  1000,d1,o1,手机,1000,1
 *  1001,d2,o1,玩具,500,2
 *  2002,d3,o2,电脑,2500,1
 *  2003,d4,o2,玩具,500,1
 *  10001,d5,o3,玩具,500,6
 * 第二个流：8888端口（右表）：订单主表（时间,订单Id，订单状态，订单总金额）
 *  1000,o1,已下单,2000
 *  2000,o2,已下单,3000
 *  10001,o3,已下单,3000
 *
 *
 * 以后实时统计各个商品、各种订单状态对应的金额
 */
public class C03_TumblingEventTimeWindowFuntion {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //订单主表的流
        //1000,o1,已下单,2000
        DataStreamSource<String> lines1 = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<String> orderMainLinesWithWaterMark = lines1.assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner((element, recordTimestamp) -> {
                    String[] fields = element.split(",");
                    return Long.parseLong(fields[0]);
                }));
        SingleOutputStreamOperator<Tuple3<String, String, Double>> orderMainStream = orderMainLinesWithWaterMark.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] filelds = value.split(",");
                return Tuple3.of(filelds[1], filelds[2], Double.parseDouble(filelds[3]));
            }
        });


        //1000,d1,o1,手机,1000,1
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

        DataStream<Tuple6<String, String, String, Double, Integer, String>> res = orderDetailStream.coGroup(orderMainStream)
                .where(t -> t.f1)
                .equalTo(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<Tuple5<String, String, String, Double, Integer>, Tuple3<String, String, Double>, Tuple6<String, String, String, Double, Integer, String>>() {
                    @Override
                    public void coGroup(Iterable<Tuple5<String, String, String, Double, Integer>> first, Iterable<Tuple3<String, String, Double>> second, Collector<Tuple6<String, String, String, Double, Integer, String>> out) throws Exception {
                        boolean isJoined = false;
                        for (Tuple5<String, String, String, Double, Integer> left : first) {
                            for (Tuple3<String, String, Double> right : second) {
                                isJoined = true;
                            }
                            if (!isJoined) {
                                out.collect(Tuple6.of(left.f0, left.f1, left.f2, left.f3, left.f4, null));
                            }
                        }

                    }
                });

        res.print();

        env.execute();


    }

}
