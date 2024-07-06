package cn.doitedu.flink.day13;

import cn.doitedu.flink.utils.SQLUtils;

/**
 * 演示flinksql的jdbc connect，将数据upsert到mysql中
 */
public class C03_JDBCSink02 {

    public static void main(String[] args) throws Exception {

        SQLUtils.init();
        SQLUtils.execute("sqls/03_JdbcSink.sql");

    }
}
