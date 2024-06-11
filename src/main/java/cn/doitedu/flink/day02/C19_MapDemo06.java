package cn.doitedu.flink.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamMap;

import java.util.Locale;

public class C19_MapDemo06 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("doitedu", 8888);

        MapFunction<String, String> mapFunction = new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase(Locale.ROOT);
            }
        };

        SingleOutputStreamOperator<String> res = lines.transform("MyMap", TypeInformation.of(String.class), new StreamMap<String, String>(mapFunction));

        res.print();

        env.execute();
    }
}
