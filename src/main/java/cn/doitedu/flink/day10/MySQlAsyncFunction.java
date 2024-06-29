package cn.doitedu.flink.day10;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class MySQlAsyncFunction extends RichAsyncFunction<String, Tuple2<String,String>> {
    private transient DruidDataSource dataSource;
    private transient ExecutorService executorService;
    private int maxConnTotal;


    public MySQlAsyncFunction(int maxConnTotal) {
        this.maxConnTotal = maxConnTotal;
    }

    @Override
    public void asyncInvoke(String id, ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {
        //将一个查询请求丢入的线程池中
        Future<String> future = executorService.submit(new Callable<String>() {

            @Override
            public String call() throws Exception {
                //调用查询数据库的方法
                return queryFromMySql(id);
            }
        });

        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    return future.get();
                } catch (Exception e) {
                    return null;
                }
            }
        }).thenAccept((String result) -> {
            resultFuture.complete(Collections.singleton(Tuple2.of(id, result)));
        });

    }

    private String queryFromMySql(String param) throws SQLException {
        String sql = "SELECT id, name FROM t_data WHERE id = ?";
        String result = null;
        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            //从连接池中取出一个连接对象
            connection = dataSource.getConnection();
            stmt = connection.prepareStatement(sql);
            stmt.setString(1, param);
            rs = stmt.executeQuery();
            while (rs.next()) {
                result = rs.getString("name");
            }
        }finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (connection != null) {
                //如果使用了了连接池，而是将连接还回连接池
                connection.close();
            }
        }
        return result;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        //创建一个线程池（为了实现并发请求的）
        executorService = Executors.newFixedThreadPool(maxConnTotal);
        //创建连接池（异步IO 一个请求就是一个线程，一个请求对应一个连接）
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUsername("root");
        dataSource.setPassword("root");
        dataSource.setUrl("jdbc:mysql://doitedu:3306/doit38?characterEncoding=UTF-8");
        dataSource.setInitialSize(10);
        dataSource.setKeepAlive(true);
        dataSource.setMinIdle(10);
        dataSource.setMaxActive(maxConnTotal);
    }

    @Override
    public void close() throws Exception {
        dataSource.close();
        executorService.shutdown();
    }
}
