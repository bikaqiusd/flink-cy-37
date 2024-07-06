package cn.doitedu.flink.day14;

import cn.doitedu.flink.utils.SQLUtils;

public class C01_CumulateWindow {
    public static void main(String[] args) throws Exception {
        SQLUtils.init();
        SQLUtils.execute("sqls/13_CumulateWindow.sql");
    }
}
