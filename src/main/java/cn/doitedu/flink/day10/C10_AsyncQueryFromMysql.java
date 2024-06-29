package cn.doitedu.flink.day10;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class C10_AsyncQueryFromMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1
        //2
        DataStreamSource<String> lines = env.socketTextStream("doitedu", 8888);
        int capacity = 20;
        SingleOutputStreamOperator<Tuple2<String,String>> result = AsyncDataStream.orderedWait(
                lines,
                new MySQlAsyncFunction(capacity),
                5000,
                TimeUnit.SECONDS,
                capacity
        );

        result.print();
        env.execute();
    }
}
