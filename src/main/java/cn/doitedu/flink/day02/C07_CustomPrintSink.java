package cn.doitedu.flink.day02;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Locale;

public class C07_CustomPrintSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("doitedu", 8888);
        SingleOutputStreamOperator<String> upperStream = lines.map(line -> line.toUpperCase(Locale.ROOT));

        upperStream.addSink(new MyPrintSink());

        env.execute();
    }

    public static class MyPrintSink extends RichSinkFunction<String>{
        private int indexOfThisSubtask;

        @Override
        public void open(Configuration parameters) throws Exception {
            indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            System.out.println(indexOfThisSubtask +  " > " + value);
        }
    }
}
