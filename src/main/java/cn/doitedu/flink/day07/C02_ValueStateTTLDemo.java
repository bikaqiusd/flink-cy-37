package cn.doitedu.flink.day07;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.tools.nsc.doc.model.Val;

public class C02_ValueStateTTLDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        DataStreamSource<String> lines = env.socketTextStream("doitedu", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                if (value.equals("error")) {
                    throw new RuntimeException("有错误数据，抛出异常！");
                }
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyedStream.map(new ValueStateTTlFunction());

        res.print();

        env.execute();


    }

    public static class ValueStateTTlFunction extends RichMapFunction<Tuple2<String,Integer>,Tuple2<String,Integer>>{
        private transient ValueState<Integer> countState;
        @Override
        public void open(Configuration parameters) throws Exception {
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(30))
                    //设置状态对应时间的更新方式：(默认)是当创建、或修改最近的数据，会修改其最近的访问时间
                    //.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    //当创建、修改、读取指定的数据，都会改变其最近访问的时间
                    //.setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    //设置状态的可见性：(默认)只要状态超时，就无法使用了，即使状态没有被清除
                    //.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    //只要状态没有被清除，即使超时了，也可以使用
                    //.setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("count-state", Integer.class);
            stateDescriptor.enableTimeToLive(ttlConfig);

            countState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> tp) throws Exception {
            Integer count = countState.value();
            if(count == null){
                count = 0;
            }
            count+= tp.f1;
            countState.update(count);

            tp.f1=count;

            return tp;
        }
    }

}


