package cn.doitedu.flink.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 演示keyBy的作用（按照key的HashCode进行分区）
 * 如果DataStream中的类型是Bean(POJO)类型，使用匿名内部类或lambda表达式
 */
public class C05_KeyByDemo05 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //spark,1
        //flink,3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //WCBean
        SingleOutputStreamOperator<WCBean> beanStream = lines.map(new MapFunction<String, WCBean>() {
            @Override
            public WCBean map(String value) throws Exception {
                String[] fields = value.split(",");
                return new WCBean(fields[0], Integer.parseInt(fields[1]));
            }
        });

        //按照指定的Key进行分区
        KeyedStream<WCBean, String> keyedStream = beanStream.keyBy(new KeySelector<WCBean, String>() {
            @Override
            public String getKey(WCBean value) throws Exception {
                return value.getWord();
            }
        });


        //按照指定的Key进行分区
        //KeyedStream<WCBean, String> keyedStream2 = beanStream.keyBy(bean -> bean.getWord());
        //KeyedStream<WCBean, String> keyedStream2 = beanStream.keyBy(WCBean::getWord);
        KeyedStream<WCBean, String> keyedStream2 = beanStream.keyBy(bean -> bean.word);

        keyedStream.print();


        env.execute();


    }



}
