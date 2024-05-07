package cn.doitedu.flink.day09;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestNcSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> errorLines = env.socketTextStream("doitedu", 8888);
        errorLines.print();
        env.execute();
    }
}
