package cn.doitedu.flink.day02;

import cn.doitedu.flink.day02.C06_CustomParallelSource02;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.UUID;

public class C06_CustomParalleSource02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println("执行环境的并行度：" + env.getParallelism());
        DataStreamSource<String> lines = env.addSource(new C06_CustomParallelSource02.MyCustomParallelSource02());
        System.out.println("自定义的MyCustomParallelSource02的并行度：" + lines.getParallelism());
        lines.print();

        env.execute();
    }

    /**
     * 生命周期方法：open -> run -> cancel -> close
     */
    public static class MyCustomParallelSource02 extends RichParallelSourceFunction<String> {

        private boolean flag = true;

        private int indexOfThisSubtask;


        @Override
        public void run(SourceContext<String> ctx) throws Exception {

            System.out.println("run method invoked !!!!" + indexOfThisSubtask);
            while (flag) {
                ctx.collect(UUID.randomUUID().toString());
                Thread.sleep(1000);
            }

        }

        @Override
        public void cancel() {
            System.out.println("cancel method invoked @@@@" + indexOfThisSubtask);
            flag = false;
        }

        /**
         * 通常做一些初始化工作（比如建立连接）
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            //获取当前subtask的分区编号
            indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("open method invoked $$$$" + indexOfThisSubtask);
        }

        /**
         * 通常做一些收尾或释放资源的工作
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            System.out.println("close method invoked %%%%" + indexOfThisSubtask);
        }
    }
}
