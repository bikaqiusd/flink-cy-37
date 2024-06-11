package cn.doitedu.flink.day02;

import cn.doitedu.flink.day02.udf.QueryCategoryNameFunctionV2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class C12_QueryCategoryNameFromMysql {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //o01,c11,3000
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple4<String, String, Double, String>> res = lines.map(new QueryCategoryNameFunctionV2());

        res.print();

        env.execute();

    }
}
