package cn.doitedu.flink.day04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class C15_SlotSharingGroupDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> lines = env.socketTextStream(args[0], Integer.parseInt(args[1]));
        //对数据进行切分
        DataStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                Arrays.stream(line.split(" ")).forEach(out::collect);
            }
        }); //将该算子前面和后面的算子链都禁用（断开）

        //过滤数据
        DataStream<String> filtered = words
                .filter(line -> line.startsWith("ERROR"))
                .disableChaining() //将前的链和后面的链都断开
                .slotSharingGroup("doit") //设置该算子对应的共享资源池组名称
                ;

        //将单词和1组合到元组中
        DataStream<Tuple2<String, Integer>> wordAndOne = filtered.map(w -> Tuple2.of(w, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .slotSharingGroup("default")
                ;
        //分组、聚合
        DataStream<Tuple2<String, Integer>> summed = wordAndOne
                .keyBy(t -> t.f0)
                .sum(1);
        //打印
        summed.print();

        //执行
        env.execute();
    }
}
