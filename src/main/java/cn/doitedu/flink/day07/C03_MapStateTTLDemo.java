package cn.doitedu.flink.day07;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class C03_MapStateTTLDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        DataStreamSource<String> linses = env.socketTextStream("doitedu", 9999);

        SingleOutputStreamOperator<Tuple3<String, String, Double>> tpStream = linses.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                if (value.startsWith("error")) {
                    int i = 1 / 0;
                }

                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], Double.parseDouble(fields[3]));
            }
        });

        KeyedStream<Tuple3<String, String, Double>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple4<String, Double, String, Double>> res = keyedStream.process(new MapStateTTlFunction());

        res.print();

        env.execute();

    }

    public static class MapStateTTlFunction extends KeyedProcessFunction<String,Tuple3<String,String,Double>, Tuple4<String,Double,String,Double>>{
        private MapState<String,Double> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(30)).build();

            MapStateDescriptor<String, Double> stateDescriptor = new MapStateDescriptor<>("income-state", String.class, Double.class);
            stateDescriptor.enableTimeToLive(ttlConfig);
            mapState = getRuntimeContext().getMapState(stateDescriptor);
        }

        @Override
        public void processElement(Tuple3<String, String, Double> tp, Context ctx, Collector<Tuple4<String, Double, String, Double>> out) throws Exception {
            String city = tp.f1;
            Double money = mapState.get(city);
            if(money == null){
                money = 0.0;
            }
            money+=tp.f2;

            mapState.put(city,money);

            double total = 0.0;
            for(Double value : mapState.values()){

                total+= value;
            }
            out.collect(Tuple4.of(ctx.getCurrentKey(),total,city,money));


        }
    }

}

