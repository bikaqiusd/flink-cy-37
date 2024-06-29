package cn.doitedu.flink.day11;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

/**
        * 统计一段时间内，热门商品的TopN
        *
        * 时间,用户id,事件类型,商品id,商品分类id
        *
        * 1000,u1001,view,prudoct01,category001
        * 3000,u1002,view,prudoct01,category001
        * 3200,u1002,view,prudoct02,category012
        * 3300,u1002,addCart,prudoct02,category012
        * 4000,u1002,addCart,prudoct01,category001
        * 5000,u1003,view,prudoct02,category001
        * 6000,u1004,view,prudoct02,category001
        * 7000,u1005,view,prudoct02,category001
        * 8000,u1005,addCart,prudoct02,category001
        *
        * 需求：
        * 统计60秒内的各种事件、各种商品分类下的热门商品TopN，每10秒出一次结果
        *
        * 思路：划分什么样的窗口（窗口类型，时间类型），数据如何聚合、排序
        * EventTime类型的滑动窗口、增量聚合、排序要将聚合后的数据攒起来（全量）在排序
        *
        * [0,10000) 手机,view
        *               1,华为p50,3
        *               2,ip14,2
        *           电脑,view
        *               1,MacBookPro,4
        *               2,xiaomi,3
        */
public class C01_HotGoodsTopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(30000);

        //1000,u1001,view,prudoct01,category001
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //提取EventTime，生成WaterMark
        SingleOutputStreamOperator<String> linesWithWaterMark = lines.assignTimestampsAndWatermarks
                (WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(
                        (element, recordTimestamp) -> {
                            String[] fields = element.split(",");
                            return Long.parseLong(fields[0]);
                        }));


        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = linesWithWaterMark.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[2], fields[3], fields[4]);
            }
        });
        KeyedStream<Tuple3<String, String, String>, Tuple3<String, String, String>> keyedStream = tpStream.keyBy(new KeySelector<Tuple3<String, String, String>, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> getKey(Tuple3<String, String, String> value) throws Exception {
                return value;
            }
        });

        WindowedStream<Tuple3<String, String, String>, Tuple3<String, String, String>, TimeWindow> windowedStream = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(10)));

        SingleOutputStreamOperator<ResultBean> aggStream = windowedStream.aggregate(
                new HotGoodsAggFunction(),
                new HotGoodsWinFunction()

        );

        KeyedStream<ResultBean, Tuple4<Long, Long, String, String>> keyedStream2 = aggStream.keyBy(new KeySelector<ResultBean, Tuple4<Long, Long, String, String>>() {
            @Override
            public Tuple4<Long, Long, String, String> getKey(ResultBean value) throws Exception {
                return Tuple4.of(value.winStart, value.winEnd, value.cid, value.eid);
            }
        });

        SingleOutputStreamOperator<ResultBean> res = keyedStream2.process(new HotGoodsTopNFunction());

        res.print();
        env.execute();

    }
    public static class HotGoodsAggFunction implements AggregateFunction<Tuple3<String,String,String>,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple3<String, String, String> value, Long accumulator) {
            return accumulator += 1;
        }

        @Override
        public Long getResult(Long accumulate) {
            return accumulate;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class HotGoodsWinFunction implements WindowFunction<Long,ResultBean,Tuple3<String,String,String>,TimeWindow>{

        @Override
        public void apply(Tuple3<String, String, String> key, TimeWindow window, Iterable<Long> input, Collector<ResultBean> out) throws Exception {
            Long count = input.iterator().next();
            long winStart = window.getStart();
            long winEnd = window.getEnd();
            ResultBean resultBean = new ResultBean(key.f2, key.f0, key.f1, count, winStart, winEnd);
            out.collect(resultBean);
        }
    }

    public static class HotGoodsTopNFunction extends KeyedProcessFunction<Tuple4<Long,Long,String,String>,ResultBean,ResultBean>{
        private transient ListState<ResultBean> eventsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<ResultBean> stateDescriptor = new ListStateDescriptor<>("events-state", ResultBean.class);
            eventsState = getRuntimeContext().getListState(stateDescriptor);
        }

        @Override
        public void processElement(ResultBean value, Context ctx, Collector<ResultBean> out) throws Exception {
            eventsState.add(value);
            ctx.timerService().registerEventTimeTimer(value.winEnd +1);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<ResultBean> out) throws Exception {
            ArrayList<ResultBean> list = (ArrayList<ResultBean>)eventsState.get();
            list.sort((a,b)-> Long.compare(b.counts,a.counts));

            for (int i = 0; i < Math.min(3, list.size()); i++) {
                ResultBean resultBean = list.get(i);
                resultBean.ord = i+1;
                out.collect(resultBean);

            }
            eventsState.clear();
        }
    }
}
