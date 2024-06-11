package cn.doitedu.flink.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * 演示keyBy的作用（按照key的HashCode进行分区）
 * 如果DataStream中的类型是Bean(POJO)类型，可以使用Bean中字段的名称
 */
public class C04_KeyByDemo04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //spark,1
        //flink,3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<WCBean> tpStream = lines.map(new MapFunction<String, WCBean>() {
            @Override
            public WCBean map(String value) throws Exception {
                String[] fields = value.split(",");

                return new WCBean(fields[0], Integer.parseInt(fields[1]));
            }
        });

        KeyedStream<WCBean, Tuple> keyedStream = tpStream.keyBy("word");

        keyedStream.print();

        env.execute();
    }
}
