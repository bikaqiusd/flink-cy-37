package cn.doitedu.flink.day12;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class C11_PrintSinkTable2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //创建一个Source表，即指定以后从Kafka中读取数据，然后将Kafka中的数据关联schema，映射成表
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

        //定义一个Sink表，即以后的数据输出到哪里
        tEnv.executeSql(
                "CREATE TABLE tb_res (\n" +
                        "  id STRING,\n" +
                        "  name String,\n" +
                        "  age int\n" +
                        ") WITH (\n" +
                        "  'connector' = 'print'\n" +
                        ")"
        );

        //从Source表中读取数据，然后插入到Sink表中
        tEnv.executeSql("insert into tb_res select * from tb_users_2 where age > 10");


        //如果没有调用编程的API，可以不用写下面的代码
        //env.execute();
    }
}
