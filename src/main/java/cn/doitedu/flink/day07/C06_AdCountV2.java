package cn.doitedu.flink.day07;

import cn.doitedu.flink.day01.LineToTupleFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Optional;
import java.util.Properties;

public class C06_AdCountV2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        DataStreamSource<String> lines = env.socketTextStream("doitedu", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                String time = fields[0];
                String timeRange = time.substring(0, 13);

                return Tuple3.of(timeRange, fields[1], fields[2]);
            }
        });

        KeyedStream<Tuple3<String, String, String>, Tuple3<String, String, String>> keyedStream = tpStream.keyBy(new KeySelector<Tuple3<String, String, String>, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> getKey(Tuple3<String, String, String> value) throws Exception {
                return value;
            }
        });

        SingleOutputStreamOperator<Tuple4<String, String, String, Integer>> res = keyedStream.process(new AdProcessFunction());

        res.print();


        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("doitedu")
                .setPort(6379)
                .setDatabase(9)
                .build();

        res.addSink(new RedisSink<>(conf,new AdcountMapperr()));

        env.execute();


    }



    public static class AdProcessFunction extends KeyedProcessFunction<Tuple3<String, String, String>,Tuple3<String, String, String>, Tuple4<String, String, String,Integer>>{
        private ValueState<Integer> valueState;
        @Override
        public void open(Configuration parameters) throws Exception {
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(2)).build();
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("count-state", Integer.class);
            stateDescriptor.enableTimeToLive(ttlConfig);
            valueState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void processElement(Tuple3<String, String, String> value, Context ctx, Collector<Tuple4<String, String, String, Integer>> out) throws Exception {
            Integer count = valueState.value();
            if(count == null){
                count = 0;
            }
            count += 1;
            valueState.update(count);
            out.collect(Tuple4.of(value.f0,value.f1,value.f2,count));
        }
    }

    public static class AdcountMapperr implements RedisMapper<Tuple4<String,String,String,Integer>>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"test");
        }

        @Override
        public Optional<String> getAdditionalKey(Tuple4<String, String, String, Integer> tp) {
            return Optional.of(tp.f1+ "-" +tp.f2);
        }

        @Override
        public String getKeyFromData(Tuple4<String, String, String, Integer> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple4<String, String, String, Integer> data) {
            return data.f3.toString();
        }
    }


}
