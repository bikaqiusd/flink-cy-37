package cn.doitedu.flink.day02.udf;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;


import java.sql.*;
import java.util.HashMap;

public class QueryCategoryNameFunctionV2 extends RichMapFunction<String, Tuple4<String,String,Double,String>> {
    private Connection connection;
    private PreparedStatement preparedStatement;
    private static HashMap<String,String> idToName = new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection("jdbc:mysql://doitedu:3306/doit?characterEncoding=utf-8", "root", "root");

        preparedStatement = connection.prepareStatement("select name from tb_category_v2 where cid = ?");

    }


    @Override
    public Tuple4<String, String, Double, String> map(String value) throws Exception {

        String[] fields = value.split(",");
        String oid = fields[0];
        String cid = fields[1];
        double money = Double.parseDouble(fields[2]);
        String name = idToName.get(cid);
        if(name == null){
            preparedStatement.setString(1,cid);
            ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                name = resultSet.getString(1);
                idToName.put(cid,name);

            }
            resultSet.close();
        }
        return Tuple4.of(oid,cid,money,name);
    }

    @Override
    public void close() throws Exception {
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(connection != null){
            connection.close();
        }
    }
}
