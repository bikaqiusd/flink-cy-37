package cn.doitedu.flink.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.UUID;

public class C04_CustomSource02 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        System.out.println("执行环境的并行度"+env.getParallelism());
        DataStreamSource<String> lines = env.addSource(new MyCustomSource02());

    }

    public static class MyCustomSource02 implements SourceFunction<String>{
        private boolean flag = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while(flag){
                ctx.collect(UUID.randomUUID().toString());
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            flag=false;

        }
    }
}
