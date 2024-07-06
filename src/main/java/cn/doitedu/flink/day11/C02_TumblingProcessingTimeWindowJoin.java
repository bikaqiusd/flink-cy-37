package cn.doitedu.flink.day11;

import org.apache.flink.api.common.functions.JoinFunction;
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

public class C02_TumblingProcessingTimeWindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //订单主表的流
        //o1,已下单,2000
        DataStreamSource<String> lines1 = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, Double>> orderMainStream = lines1.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], Double.parseDouble(fields[2]));
            }
        });

        //d1,o1,手机,1000,1
        DataStreamSource<String> lines2 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple5<String, String, String, Double, Integer>> orderDetailStream = lines2.map(new MapFunction<String, Tuple5<String, String, String, Double, Integer>>() {
            @Override
            public Tuple5<String, String, String, Double, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple5.of(fields[0], fields[1], fields[2], Double.parseDouble(fields[3]), Integer.parseInt(fields[4]));
            }
        });

        DataStream<Tuple6<String, String, String, Double, Integer, String>> res = orderMainStream.join(orderDetailStream)
                .where(t -> t.f0)
                .equalTo(t -> t.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .apply(new JoinFunction<Tuple3<String, String, Double>, Tuple5<String, String, String, Double, Integer>, Tuple6<String, String, String, Double, Integer, String>>() {
                    @Override
                    public Tuple6<String, String, String, Double, Integer, String> join(
                            Tuple3<String, String, Double> first, Tuple5<String, String, String, Double, Integer> second) throws Exception {
                        return Tuple6.of(second.f0, second.f1, second.f2, second.f3, second.f4, first.f1);
                    }
                });

        res.print();
        env.execute();

    }
}
