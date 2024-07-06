package cn.doitedu.flink.day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.expressions.In;

public class C07_SQLWordCount2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //为了保证程序能够容错，要开启checkpoint
        env.enableCheckpointing(15000);

        //StreamTableEnvironment是原来的StreamExecutionEnvironment包装类，进行了增强，就可以写SQL
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        //spark,1
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<DataBean> tpStream = lines.map(new MapFunction<String, DataBean>() {
            @Override
            public DataBean map(String value) throws Exception {
                if (value.startsWith("error")) {
                    throw new RuntimeException("有错误数据！");
                }
                String[] fields = value.split(",");
                return DataBean.of(fields[0], Integer.parseInt(fields[1]));
            }
        });

        tEnv.createTemporaryView("v_data",tpStream);

        TableResult tableResult = tEnv.executeSql("select word, sum(counts) counts from v_data group by word");

        tableResult.print();

        env.execute();


    }

    public static class DataBean{
        public String word;
        public Integer counts;

        public DataBean(String word, Integer counts) {
            this.word = word;
            this.counts = counts;
        }

        public DataBean() {
        }

        @Override
        public String toString() {
            return "DataBean{" +
                    "word='" + word + '\'' +
                    ", counts=" + counts +
                    '}';
        }

        public static DataBean of(String word,Integer counts){return new DataBean(word,counts);}
    }
}
