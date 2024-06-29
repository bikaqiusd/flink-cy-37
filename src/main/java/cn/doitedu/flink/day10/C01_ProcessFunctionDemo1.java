package cn.doitedu.flink.day10;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class C01_ProcessFunctionDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1
        //a
        //2
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Integer> res = lines.process(new ProcessFunction<String, Integer>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Integer> out) throws Exception {
                try {
                    int i = Integer.parseInt(value);
                    if (i % 2 == 0) {
                        out.collect(i);
                    }
                } catch (NumberFormatException e) {
                }
            }

        });

        res.print();

        env.execute();

    }
}