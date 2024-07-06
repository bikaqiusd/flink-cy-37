package cn.doitedu.flink.day13;

import cn.doitedu.flink.utils.SQLUtils;

public class C01_TestSQLUtils {
    public static void main(String[] args) throws Exception {
        SQLUtils.init();
        SQLUtils.execute("sqls/01_Demo1.sql");

    }
}
