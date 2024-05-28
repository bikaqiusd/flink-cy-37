package cn.doitedu.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class StreamingWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("doitedu", 8888);
        SingleOutputStreamOperator<String> wordsStream = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String in, Collector<String> out) throws Exception {
                String[] words = in.split(" ");
                for (String word : words) {
                    out.collect(word);
                }

            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneStream = wordsStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String in) throws Exception {
                return Tuple2.of(in, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOneStream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> in) throws Exception {
                return in.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyedStream.sum(1);

        res.print();

        env.execute();

    }
}
