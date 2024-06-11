package cn.doitedu.flink.day02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class C13_MapDemo05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //spark,1
        //flink,3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = lines.map(value -> {
            String[] fields = value.split(",");
            return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        tpStream.print();
        env.execute();
    }
}
