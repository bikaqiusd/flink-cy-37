package cn.doitedu.flink.day12;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class C10_KafkaSourcceTable2 {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                "CREATE TABLE tb_users_2 (\n" +
                        "  `id` STRING,\n" +
                        "  `name` String,\n" +
                        "  `age` int\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'tp-users',\n" +
                        "  'properties.bootstrap.servers' = 'node-1.51doit.cn:9092,node-2.51doit.cn:9092',\n" +
                        "  'properties.group.id' = 'testGroup2',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'csv',\n" +
                        "  'csv.ignore-parse-errors' = 'true'\n" +
                        ")"
        );




    }
}
