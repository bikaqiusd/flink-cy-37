package cn.doitedu.flink.day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
/**
 * 使用FlinkSQL实现广告各种事件的次数和次数统计
 *
 * 用户id,行为id,广告id
 * u01,view,a01
 * u01,click,a01
 * u02,view,a01
 * u02,view,a01
 * u02,click,a01
 * u01,view,a02
 * u01,click,a02
 */
public class C08_SQLAdcount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10000);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        //u01,view,a01
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        //tpStream数据流
        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                if (value.startsWith("error")) {
                    throw new RuntimeException("有错误数据！");
                }
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });

        tEnv.createTemporaryView("v_user_log", tpStream);

        TableResult tableResult = tEnv.executeSql("select f2 aid, f1 eid, count(*) counts, count(distinct(f0)) dis_counts from v_user_log group by f1, f2");

        tableResult.print();

        env.execute();
    }
}
