package cn.doitedu.flink.day12;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.UUID;

public class C05_BackPressure {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //1
        //2
        DataStreamSource<String> lines = env.addSource(new RichSourceFunction<String>() {

            private boolean flag = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (flag) {
                    ctx.collect(UUID.randomUUID().toString());
                    Thread.sleep(5);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

        SingleOutputStreamOperator<String> res = lines.map(new RichMapFunction<String, String>() {

            private int indexOfThisSubtask;

            @Override
            public void open(Configuration parameters) throws Exception {
                indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            }

            @Override
            public String map(String value) throws Exception {
                if (indexOfThisSubtask == 1) {
                    Thread.sleep(5000);
                }
                return value;
            }
        });

        res.print();

        env.execute();
    }
}
