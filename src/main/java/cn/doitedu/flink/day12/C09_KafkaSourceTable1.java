package cn.doitedu.flink.day12;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class C09_KafkaSourceTable1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.createTable("tb_user", TableDescriptor
                .forConnector("kafka")
                .format("csv")
                .option("topic","tp-user")
                .option("properties.bootstrap.servers","doitedu:9092")
                .option("properties.group.id","testGroup")
                .option("scan.startup.mode","latest-offsetr")
                .option("csv.ignore-parse-errors", "true") //忽略解析 出错的数据，转成NULL
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.STRING())
                                        .column("name",DataTypes.STRING())
                                        .column("age",DataTypes.INT())
                                .build())
                .build());

        TableResult tableResult = tEnv.executeSql("select * from tb_user where age > 10");
        tableResult.print();

        env.execute();


    }
}
