package cn.doitedu.flink.day07;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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

import java.util.ArrayList;
import java.util.List;

public class C04_ListStateTTLDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        DataStreamSource<String> lines = env.socketTextStream("doitedu", 8888);

        SingleOutputStreamOperator<Tuple2<String, String>> tpStream = lines.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], fields[1]);
            }
        });

        KeyedStream<Tuple2<String, String>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, List<String>>> res = keyedStream.process(new ListStateTTLFunction());

        res.print();

        env.execute();


    }

    public static class ListStateTTLFunction extends KeyedProcessFunction<String,Tuple2<String,String>,Tuple2<String, List<String>>>{
        private ListState<String> listState;
        @Override
        public void open(Configuration parameters) throws Exception {
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(30)).build();
            ListStateDescriptor<String> stateDescriptor = new ListStateDescriptor<>("event-state", String.class);
            stateDescriptor.enableTimeToLive(ttlConfig);
            listState = getRuntimeContext().getListState(stateDescriptor);
        }

        @Override
        public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, List<String>>> out) throws Exception {
            listState.add(value.f1);
            Iterable<String> iterable = listState.get();
            ArrayList<String> list = new ArrayList<>();
            for(String event : iterable){
                list.add(event);
            }
            out.collect(Tuple2.of(ctx.getCurrentKey(),list));

        }
    }
}
