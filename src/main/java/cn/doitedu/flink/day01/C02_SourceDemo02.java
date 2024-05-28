package cn.doitedu.flink.day01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.LongValueSequenceIterator;

/**
 * 演示Flink的Source，即Flink以后从哪里获取数据创建DataStream
 *
 * 该例子是演示基于集合的多并行的Source，基于集合的Source是有限数据流，仅适用于测试环境
 *
 *
 */
public class C02_SourceDemo02 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int parallelism = env.getParallelism();
        System.out.println("执行环境的并行度为：" + parallelism);
        //调用基于集合的多并行Source
        DataStreamSource<LongValue> numStream = env.fromParallelCollection(new LongValueSequenceIterator(1, 20), LongValue.class);
        System.out.println("调用fromCollection这个Source方法生成的DataStream的并行度：" + numStream.getParallelism());
        SingleOutputStreamOperator<Long> resStream = numStream.map(new MapFunction<LongValue, Long>() {
            @Override
            public Long map(LongValue in) throws Exception {
                return in.getValue() * 10;
            }
        });
        System.out.println("调用完map方法返回的DataStream的并行度为：" + resStream.getParallelism());
        resStream.print();
        env.execute();



    }
}
