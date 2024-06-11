package cn.doitedu.flink.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class C06_KeyByDemo07 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //山东省,济南市,3000
        //山东省,青岛市,2000
        //河北省,廊坊市,1000
        DataStreamSource<String> lines = env.socketTextStream("doitedu", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> tpStream = (SingleOutputStreamOperator<Tuple3<String, String, Integer>>) lines.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], Integer.parseInt(fields[2]));
            }
        });

        KeyedStream<Tuple3<String, String, Integer>, Tuple2<String, String>> keyedStream = tpStream.keyBy(
                tp->Tuple2.of(tp.f0, tp.f1),
                Types.TUPLE(Types.STRING, Types.STRING) //指定key的类型
        );

        keyedStream.print();

        env.execute();

    }
}
