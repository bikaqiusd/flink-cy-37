package cn.doitedu.flink.day02;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.expressions.In;


public class C11_MapDemo04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("doitedu", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = lines.map(new StringToTupleFunction());

        tpStream.print();

        env.execute();

    }

    public static class StringToTupleFunction extends RichMapFunction<String, Tuple2<String,Integer>>{
        @Override
        public Tuple2<String, Integer> map(String value) throws Exception {
            String[] fields = value.split(",");

            return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
        }
    }
}
