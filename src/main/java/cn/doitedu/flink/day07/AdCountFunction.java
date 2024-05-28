package cn.doitedu.flink.day07;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


import java.util.HashSet;

public class AdCountFunction extends KeyedProcessFunction<Tuple2<String,String>, Tuple3<String,String,String>, Tuple4<String,String,Integer,Integer>> {
    private transient ValueState<Integer> countState;
    private transient ValueState<HashSet<String>> uidState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Integer> countStateDescriptor = new ValueStateDescriptor<>("count-state", Integer.class);
        countState= getRuntimeContext().getState(countStateDescriptor);
        ValueStateDescriptor<HashSet<String>> uidStateDescriptor = new ValueStateDescriptor<>("uid-state", TypeInformation.of(new TypeHint<HashSet<String>>() {
        }));
         uidState= getRuntimeContext().getState(uidStateDescriptor);
    }

    @Override
    public void processElement(Tuple3<String, String, String> value, Context ctx, Collector<Tuple4<String, String, Integer, Integer>> out) throws Exception {
        Integer count = countState.value();
        if(count == null){
            count = 0;
        }
        count+=1;
        countState.update(count);

        HashSet<String> uidSet = uidState.value();
        if(uidSet == null){
            uidSet = new HashSet<>();
        }
        uidSet.add(value.f2);

        uidState.update(uidSet);

        out.collect(Tuple4.of(value.f0, value.f1, uidSet.size(), count));

    }
}
