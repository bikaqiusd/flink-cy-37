-- 创建Source表
CREATE TABLE kafka_events (
  `oid` STRING,
  `cid` BIGINT,
  `money` double,
  `proctime` AS PROCTIME() -- 取出当前的系统时间作为processingTime
) WITH (
  'connector' = 'kafka', -- 指定connector类型
  'topic' = 'tp-users', -- 指定从Kafka中读取数据的topic
  'properties.bootstrap.servers' = 'node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092', -- 指定kafka的地址
  'properties.group.id' = 'testGroup', -- 指定消费者组ID
  'scan.startup.mode' = 'latest-offset', -- 指定消费的起始偏移量
  'format' = 'csv', -- 读取csv格式，默认的分隔符为","
  'csv.ignore-parse-errors' = 'true' -- 忽略解析失败的数据
)
;
-- 创建维表
CREATE TABLE tb_dimemsion (
  id BIGINT,
  name STRING
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://node-1.51doit.cn:3306/doit?characterEncoding=utf-8',
  'table-name' = 'tb_category', -- 在MySQL中实际对应的表
  'username' = 'root',
  'password' = '123456',
  'lookup.cache.max-rows' = '500', -- 缓存数据的最大行数
  'lookup.cache.ttl' = '2min' -- 缓存数据的时长
)
;
-- 创建Sink表
CREATE TABLE result_sink (
  `oid` STRING,
  `cid` BIGINT,
  `money` double,
  name STRING
) WITH (
  'connector' = 'print' -- 指定connector类型
)
;
-- 查询语句并插入到sink表中
INSERT INTO result_sink
SELECT
  oid, cid, money, name
FROM
  kafka_events
LEFT JOIN
  tb_dimemsion FOR SYSTEM_TIME AS OF kafka_events.proctime -- 以kafka_events表中的proctime做为缓存的时间判断标准
ON
  kafka_events.cid = tb_dimemsion.id