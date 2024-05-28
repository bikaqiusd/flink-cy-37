package cn.doitedu.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class LineToTupleFunction implements FlatMapFunction<String, Tuple2<String,Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        String[] fields = value.split(",");
        for(String word:fields){
            out.collect(Tuple2.of(word,1));
        }
    }
}
