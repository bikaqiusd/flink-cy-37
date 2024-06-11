package cn.doitedu.flink.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

/**
 * 不使用flatMap方法，而是使用transform实现类似的功能
 */
public class C21_FlatmapDemo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //spark hive flink
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        FlatMapFunction<String, String> flatMapFunction = new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        };

        SingleOutputStreamOperator<String> res = lines.transform("MyFlatMap", TypeInformation.of(String.class), new MyStreamMap(flatMapFunction));

        res.print();

        env.execute();

    }

    public static class MyStreamMap extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String> {

        private FlatMapFunction<String, String> flatMapFunction;
        private TimestampedCollector<String> collector;

        public MyStreamMap(FlatMapFunction<String, String> flatMapFunction) {
            this.flatMapFunction = flatMapFunction;
        }

        @Override
        public void open() throws Exception {
            collector = new TimestampedCollector<>(output);
        }

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            String in = element.getValue();
            flatMapFunction.flatMap(in, collector);
        }
    }


}
