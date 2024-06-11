package cn.doitedu.flink.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 *  mimBy与min、maxBy与max是有区别的
 *
 *  数据中只有keyBy的字段和参与比较的字段，那么max和maxBy效果一样
 *
 *  maxBy和max的区别：
 *  maxBy：如果数据中有KeyBy的字段和参与比较的字段，还有其他的字段，maxBy会返回最大值所在数据的全部字段
 */
public class C13_MinByMaxByDemo2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //山东省,济南市,3000
        //

        //山东省,烟台市,5000
        //河北省,廊坊市,5000
        //山东省,泰安市,5000
        DataStreamSource<String> lines = env.socketTextStream("doitedu", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], Integer.parseInt(fields[2]));
            }
        });

        //按照省份进行KeyBy
        KeyedStream<Tuple3<String, String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        //第二个参数可以传入一个boolean值，如果参与比较的字段相等：返回第一次出现的
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> res = keyedStream.maxBy(2, true);

        res.print();

        env.execute();


    }



}
