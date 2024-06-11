package cn.doitedu.flink.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * 演示keyBy的作用（按照key的HashCode进行分区）
 *
 * keyBy的底层实现方式
 * 1.每来一条数据，调用一次selectChannel
 * 2.然后根据自定义可KeySelector提取出key的值
 * 3.然后继续Key的HashCode值（可能是负的）
 * 4.使用MurmurHash进行散列（将hashcode负的值变成正的，让散列相对更加均匀）
 * 5.计算keyGroupId
 * 6.计算下游分区Index
 *
 *
 */
public class C01_KeyByDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //spark,1
        //flink,3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });

        //按照指定的Key进行分区
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

            @Override
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0; //将数据的key提取出来，根据这个key计算分区编号
            }
        });

        keyedStream.print();


        env.execute();
    }
}
