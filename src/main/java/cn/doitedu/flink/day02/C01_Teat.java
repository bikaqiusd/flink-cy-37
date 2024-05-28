package cn.doitedu.flink.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class C01_Teat {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 18083);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(1000);
        DataStreamSource<String> lines= env.socketTextStream("doitedu", 8888);

        SingleOutputStreamOperator<String> numStream = lines.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toLowerCase();
            }
        });

        numStream.print();
        env.execute();
    }
}
