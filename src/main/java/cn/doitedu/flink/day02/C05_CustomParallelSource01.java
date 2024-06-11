package cn.doitedu.flink.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.UUID;

public class C05_CustomParallelSource01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println("执行环境的并行度：" + env.getParallelism());
        DataStreamSource<String> lines = env.addSource(new MyCustomParallelSource01());
        System.out.println("自定义的MyCustomParallelSource01的并行度：" + lines.getParallelism());
        lines.print();

        env.execute();
    }

    public static class MyCustomParallelSource01 implements ParallelSourceFunction<String> {
        private boolean flag = true;


        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (flag) {
                ctx.collect(UUID.randomUUID().toString());
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            //认为停止job时，先会调用cancel方法
            flag = false;
        }
    }
}
