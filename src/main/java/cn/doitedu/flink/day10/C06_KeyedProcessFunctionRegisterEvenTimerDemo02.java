package cn.doitedu.flink.day10;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class C06_KeyedProcessFunctionRegisterEvenTimerDemo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(30000);

        //1000,spark,1
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> linesWithWaterMark = lines.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[0]);
            }
        }));

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = linesWithWaterMark.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                if (value.contains("error")) {
                    throw new RuntimeException("有错误数据，抛出异常！");
                }
                String[] fields = value.split(",");
                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
            }
        });

        //先KeyBy
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);


        //再调用process方法，传入KeyedProcessFunction
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            private transient ValueState<List<Tuple2<String, Integer>>> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<List<Tuple2<String, Integer>>> descriptor = new ValueStateDescriptor<>("list-state", TypeInformation.of(new TypeHint<List<Tuple2<String, Integer>>>() {
                }));

                valueState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                List<Tuple2<String, Integer>> list = valueState.value();
                if (list == null) {
                    list = new ArrayList<>();
                }
                list.add(value);
                valueState.update(list);

                long currentWatermark = ctx.timerService().currentWatermark();

                if (currentWatermark > Long.MIN_VALUE) {
                    long triggerTime = currentWatermark - currentWatermark % 10000 + 10000 - 1;

                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                List<Tuple2<String, Integer>> list = valueState.value();
                list.sort((a, b) -> a.f1 - b.f1);
                for (int i = 0; i < Math.min(3, list.size()); i++) {
                    out.collect(list.get(i));
                }
                valueState.clear();
            }
        });

        res.print();

        env.execute();
    }
}
