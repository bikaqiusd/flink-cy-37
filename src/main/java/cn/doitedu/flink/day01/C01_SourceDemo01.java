package cn.doitedu.flink.day01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * 演示Flink的Source，即Flink以后从哪里获取数据创建DataStream
 *
 * 该例子是演示基于集合的Source，基于集合的Source是有限数据流，仅适用于测试环境
 *
 */
public class C01_SourceDemo01 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int parallelism = env.getParallelism();
        System.out.println("执行环境的并行度为：" + parallelism);
        //调用基于集合的Source
        List<Integer> numList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        DataStreamSource<Integer> numStream = env.fromCollection(numList);
        System.out.println("调用fromCollection这个Source方法生成的DataStream的并行度：" + numStream.getParallelism());
        SingleOutputStreamOperator<Integer> resStream = numStream.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer in) throws Exception {
                return in * 10;
            }
        });
        System.out.println("调用完map方法返回的DataStream的并行度为：" + resStream.getParallelism());
        resStream.print();
        env.execute();



    }
}
