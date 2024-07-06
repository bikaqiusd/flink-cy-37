-- kafka Source 表
CREATE TABLE tb_events (
  uid String,
  e_type String
) WITH (
  'connector' = 'kafka',
  'topic' = 'tp-users',
  'properties.bootstrap.servers' = 'node-1.51doit.cn:9092,node-2.51doit.cn:9092',
  'properties.group.id' = 'testGroup2',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'csv',
  'csv.ignore-parse-errors' = 'true'
)
;

-- 定义Mysql Sink标签, 支持追加，允许数据重复
CREATE TABLE mysql_sink (
  user_id String,
  event_type String
)  WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://node-1.51doit.cn:3306/doit?characterEncoding=utf-8',
  'table-name' = 'tb_user_events',
  'username' = 'root',
  'password' = '123456'
)
;

-- 从Source表中查询，然后将结果插入到Sink表中
insert into mysql_sink select uid user_id, e_type event_type from tb_events where e_type <> 'view'