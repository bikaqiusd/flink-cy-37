package cn.doitedu.flink.day02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class C15_FlatMapDemo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("doitedu", 8888);
        SingleOutputStreamOperator<String> words = lines.flatMap((String line, Collector<String> out) -> {
            for (String word : line.split(" ")) {
                out.collect(word);
            }
        }).returns(Types.STRING);

        words.print();

        env.execute();
    }
}
