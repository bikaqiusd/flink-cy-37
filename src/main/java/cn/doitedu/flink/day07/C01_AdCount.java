package cn.doitedu.flink.day07;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class C01_AdCount {
    public static void main(String[] args) throws Exception {
        /**
         * 1.统计每个广告各种事件对应的人数(uv,按照用户id去重)和次数(pv不去重)
         *
         * 数据格式如下：
         *
         * 用户id,行为id,广告id
         * u01,view,a01
         * u01,click,a01
         * u02,view,a01
         * u02,view,a01
         * u02,click,a01
         * u01,view,a02
         * u01,click,a02
         *
         *
         * 统计结果如下
         * a01,view,3,2
         * a01,click,2,2
         * a02,view,1,1
         * a02,click,1,1
         *
         *
         * 需求分析：
         *   1.按照广告ID、和事件ID进行KeyBy
         *   2.将用一个用户的ID保存到HashSet中（去重、无序）
         *
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);

        DataStreamSource<String> lines = env.socketTextStream("doitedu", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[2], fields[1], fields[0]);
            }
        });

        KeyedStream<Tuple3<String, String, String>, Tuple2<String, String>> keyedStream = tpStream.keyBy(new KeySelector<Tuple3<String, String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, String> tp) throws Exception {
                return Tuple2.of(tp.f0, tp.f1);
            }
        });

        SingleOutputStreamOperator<Tuple4<String, String, Integer, Integer>> res = keyedStream.process(new AdCountFunction());

        res.print();

        env.execute();

    }
}
