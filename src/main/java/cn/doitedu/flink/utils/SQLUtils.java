package cn.doitedu.flink.utils;

import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;


public class SQLUtils {
    public static StreamTableEnvironment tEnv;


    public static void init() {
        Configuration conf = new Configuration();
        //设置空闲超时时间
        conf.setString("table.exec.source.idle-timeout", "60000 ms");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(4);

        tEnv = StreamTableEnvironment.create(env);


    }

    public static void execute(String sqlFile) throws Exception{
        String content = FileUtils.readFileToString(new File(sqlFile), "UTF-8");

        String[] sqls = content.split(";");

        for(String sql:sqls){
            tEnv.executeSql(sql);
        }

    }



}
