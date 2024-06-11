package cn.doitedu.flink.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.kafka.common.protocol.types.Field;

public class C03_CustomSource01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println("执行环境的并行度："+env.getParallelism());
        DataStreamSource<String> lines = env.addSource(new MyCustomSource01());

        lines.print();

        System.out.println("自定义的MyCustomSource01的并行度：" + lines.getParallelism());

        env.execute();
    }

    public static class MyCustomSource01 implements SourceFunction<String>{

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            for (int i = 1; i <= 100; i++) {
                ctx.collect("doitedu" + i);
            }
        }
        @Override
        public void cancel() {

        }
    }
}
