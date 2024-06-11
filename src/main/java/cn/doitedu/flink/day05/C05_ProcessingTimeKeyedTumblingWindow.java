package cn.doitedu.flink.day05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 先keyBy，然后按照ProcessingTime划分滚动窗口，得到窗口是KeyedWindow
 *
 * KeyedWindow的特点：Window和WindowOperator所在的DataStream并行度可以是多个
 *
 * 窗口会按照系统时间定期的滚动，有数据就对窗口内的数据进行运算，窗口触发后，每个组都会输出结果
 * （即使再一段时间内没有数据产生，窗口也照样生成，只不过没数据输出而已）
 */
public class C05_ProcessingTimeKeyedTumblingWindow {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //spark,1
        //hive,2
        //spark,3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });

        //先KeyBy，返回一个KeyedStream
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        //然后调用window方法划分KeyedWindow
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        //调用WindowOperator对窗口中的数据进行运算
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.sum(1);

        res.print();

        env.execute();



    }
}
