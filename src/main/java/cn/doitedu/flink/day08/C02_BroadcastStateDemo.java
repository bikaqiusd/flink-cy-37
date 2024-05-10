package cn.doitedu.flink.day08;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.table.planner.expressions.In;
import org.apache.flink.util.Collector;

public class C02_BroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        //维度数据
        //INSERT,1,图书
        //INSERT,2,家具
        //INSERT,3,服装
        //UPDATE,3,女装
        //DELETE,3,女装
        DataStreamSource<String> lines1 = env.socketTextStream("doitedu", 8888);

        SingleOutputStreamOperator<Tuple3<String, Integer, String>> tpStream = lines1.map(new MapFunction<String, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], Integer.parseInt(fields[1]), fields[2]);
            }
        });
        //广播数据  广播后的数据，在下游是以Map的形式进行存储的
        MapStateDescriptor<Integer, String> stateDescriptor = new MapStateDescriptor<>("broadcast-state", Integer.class, String.class);
        BroadcastStream<Tuple3<String, Integer, String>> broadcastStream = tpStream.broadcast(stateDescriptor);

        //用户行为数据
        //o01,1,300
        //o02,2,2000
        //o03,3,1000
        //o04,3,1000
        //o05,3,1000
        DataStreamSource<String> lines2 = env.socketTextStream("doitedu", 9999);

        SingleOutputStreamOperator<Tuple3<String, Integer, Double>> orderStream = lines2.map(new MapFunction<String, Tuple3<String, Integer, Double>>() {
            @Override
            public Tuple3<String, Integer, Double> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], Integer.parseInt(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        SingleOutputStreamOperator<Tuple4<String, Integer, Double, String>> res = orderStream.connect(broadcastStream).process(new BroadcastProcessFunction<Tuple3<String, Integer, Double>, Tuple3<String, Integer, String>, Tuple4<String, Integer, Double, String>>() {


           @Override
           public void processElement(Tuple3<String, Integer, Double> value, ReadOnlyContext ctx, Collector<Tuple4<String, Integer, Double, String>> out) throws Exception {
               ReadOnlyBroadcastState<Integer, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
               String name = broadcastState.get(value.f1);
               out.collect(Tuple4.of(value.f0, value.f1, value.f2, name));
           }

           @Override
           public void processBroadcastElement(Tuple3<String, Integer, String> value, Context ctx, Collector<Tuple4<String, Integer, Double, String>> out) throws Exception {
               System.out.println(getRuntimeContext().getIndexOfThisSubtask() + " processBroadcastElement method invoked !!!");
               String type = value.f0;
               BroadcastState<Integer, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
               if ("DELETE".equals(type)) {
                   broadcastState.remove(value.f1);

               } else {
                   broadcastState.put(value.f1, value.f2);

               }
           }
                                                                                                                               }
        );
        res.print();
        env.execute();
    }
}
