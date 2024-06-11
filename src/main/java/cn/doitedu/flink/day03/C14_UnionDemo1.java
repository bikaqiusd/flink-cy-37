package cn.doitedu.flink.day03;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * union是将多个类型一样的DataStream合并到一起，然后按照统一的方式进行处理
 */
public class C14_UnionDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //执行环境的并行为为4

        DataStreamSource<String> lines1 = env.socketTextStream("doitedu", 8888);

        SingleOutputStreamOperator<Integer> numStream1 = lines1
                .map(i -> Integer.parseInt(i))
                .setParallelism(3); //numStream1并行度为3

        DataStreamSource<String> lines2 = env.socketTextStream("doitedu", 9999);

        SingleOutputStreamOperator<Integer> numStream2 = lines2
                .map(Integer::parseInt)
                .setParallelism(2); //numStream2并行度为2

        //不同类型的DataStream不能union
        //lines1.union(numStream2)

        //将多个数量类型一样的流合并到一起
        DataStream<Integer> union = numStream1.union(numStream2);

        //按照统一的操作进行处理
        SingleOutputStreamOperator<Integer> res = union.map(i -> i * 10);

        res.print();

        env.execute();
    }
}
