package cn.doitedu.flink.day01;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingWordCountV3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream(args[0], Integer.parseInt(args[1]));
        lines.flatMap(new LineToTupleFunction())
                .keyBy(t -> t.f0)
                .sum(1)
                .print();
        env.execute();
    }

}
