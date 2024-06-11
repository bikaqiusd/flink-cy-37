package cn.doitedu.flink.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class C10_PartitonCustomDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //spark,3
        DataStreamSource<String> lines = env.socketTextStream("doitedu", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });

        //然后自定义分区
        DataStream<Tuple2<String, Integer>> res = tpStream.partitionCustom(
                new MyPartitioner(),
                t -> t.f0
        );

        res.print();

        env.execute();

    }

    public static class MyPartitioner implements Partitioner<String> {

        @Override
        public int partition(String key, int numPartitions) {
            int partition = 0;
            if ("spark".equals(key)) {
                partition = 1;
            } else if ("hive".equals(key)) {
                partition = 2;
            } else if ("flink".equals(key)){
                partition = 3;
            }
            return partition;
        }
    }

}
