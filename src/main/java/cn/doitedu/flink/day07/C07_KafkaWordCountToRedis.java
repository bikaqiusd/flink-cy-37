package cn.doitedu.flink.day07;

import cn.doitedu.flink.day01.LineToTupleFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;


    public class C07_KafkaWordCountToRedis {
        public static void main(String[] args) throws Exception {

            Configuration configuration = new Configuration();
            configuration.setInteger(RestOptions.PORT, 18084);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
            env.enableCheckpointing(10000);
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"doitedu:9092");
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"g3703");
            FlinkKafkaConsumerBase<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
                    "WordCount",
                    new SimpleStringSchema(),
                    properties
            );

            DataStreamSource<String> lines = env.addSource(kafkaConsumer);
            SingleOutputStreamOperator<String> lowerStream= lines.map(line -> line.toLowerCase());
            lowerStream.print("kafka数据源");

            lines.print("结果kfka");

            SingleOutputStreamOperator<Tuple2<String, Integer>> res = lines.flatMap(new LineToTupleFunction())
                    .keyBy(t->t.f0)
                    .sum(1);

           res.print("结果etl打印");

            FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                    .setHost("doitedu")
                    .setPort(6379)
                    .setDatabase(10)
                    .build();

            res.addSink(new RedisSink<>(conf,new WordCountMapper()));

            env.execute();


        }

        public static class WordCountMapper implements RedisMapper<Tuple2<String, Integer>> {

            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET,"WordCount");
            }

            @Override
            public String getKeyFromData(Tuple2<String, Integer> data) {
                return data.f0;
            }

            @Override
            public String getValueFromData(Tuple2<String, Integer> data) {
                return data.f1.toString();
            }
        }
    }

