package cn.doitedu.flink.day06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.expressions.In;

import java.util.HashMap;

public class C09_MyValueStateDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

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

        //不使用sum、reduce等方法，自己实现类似的功能
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyedStream.map(new MyValueStateFunction2());

        res.print();

        env.execute();


    }

    public static class  MyValueStateFunction2 extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
        private HashMap<String, Integer> state = new HashMap<>();


        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> tp) throws Exception {
            String word = tp.f0;

            Integer count = state.get(word);
            if(count == null){
                count =0;
            }

            count+=tp.f1;

            state.put(word,count);

            tp.f1 = count;

            return tp;

        }
    }

}
