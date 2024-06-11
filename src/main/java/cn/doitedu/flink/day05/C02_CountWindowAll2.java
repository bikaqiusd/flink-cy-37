package cn.doitedu.flink.day05;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class C02_CountWindowAll2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1
        //2
        //3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Integer> numStream = lines.map(Integer::parseInt);

        //按照条数划分窗口
        //整个窗口内最多保存10条数据，每5条滑动一次
        AllWindowedStream<Integer, GlobalWindow> windowedStream = numStream.countWindowAll(10, 5);

        //对窗口内的数据进行计算
        SingleOutputStreamOperator<Integer> res = windowedStream.sum(0);

        res.print();

        env.execute();
    }
}
