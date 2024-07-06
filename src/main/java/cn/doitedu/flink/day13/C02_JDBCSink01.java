package cn.doitedu.flink.day13;

import cn.doitedu.flink.utils.SQLUtils;

/**
 * 演示flinksql的jdbc connect，将数据追加到mysql中
 */
public class C02_JDBCSink01 {

    public static void main(String[] args) throws Exception {

        SQLUtils.init();
        SQLUtils.execute("sqls/02_JdbcSink.sql");

    }
}
