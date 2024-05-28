package cn.doitedu.flink.day01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 演示SocketSource，无限数据流
 *
 */
public class C03_SourceDemo03 {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        int parallelism = env.getParallelism();
        System.out.println("执行环境的并行度为：" + parallelism);
        //调用基于Socket端口的Source
        //1
        //2
        DataStreamSource<String> lines = env.socketTextStream("doitedu", 8888);

        System.out.println("调用socketTextStream这个Source方法生成的DataStream的并行度：" + lines.getParallelism());
        SingleOutputStreamOperator<Integer> numStream = lines.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value) * 10;
            }
        });

        System.out.println("调用完map方法返回的DataStream的并行度为：" + numStream.getParallelism());

        //调用Sink
        DataStreamSink<Integer> print = numStream.print();
        int parallelism1 = print.getTransformation().getParallelism();
        System.out.println("print sink 算子的并许多为：" + parallelism1);

        env.execute();



    }
}
