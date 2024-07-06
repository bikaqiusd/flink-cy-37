package cn.doitedu.flink.day14;

import cn.doitedu.flink.utils.SQLUtils;
/**
 * 实现lookup join
 * 数据来了，先在内存中进行查找，如果内存中有，就使用内存中的，没有就查询外部数据库中的数据，然后再缓存的内存中
 *
 */
public class C08_LookupJoinDemo {
    public static void main(String[] args) throws Exception {
        SQLUtils.init();
        SQLUtils.execute("sqls/20_LookupJoinDemo.sql");
    }
}
