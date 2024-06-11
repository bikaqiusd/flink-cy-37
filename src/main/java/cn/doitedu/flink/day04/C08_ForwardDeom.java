package cn.doitedu.flink.day04;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * 两个算子的并行度一样，算子链被断开，使用传输数据的方式为forward直传（没有跨节点传输数据）
 *
 * 举个例子：上游的0号分区的subtask将数据传递给下游0号分区的subtask
 */
public class C08_ForwardDeom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("doitedu" , 8888);

        SingleOutputStreamOperator<String> upStream = lines.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                return value + " -> up : " + indexOfThisSubtask;
            }
        }).disableChaining(); //断开该算子前后的算子链

        //forward直传(没有起作用，是因为flink有默认的优化策略)
        DataStream<String> forward = upStream.forward();

        forward.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                System.out.println(value + " -> down : " + indexOfThisSubtask);
            }
        });

        env.execute();

    }
}
