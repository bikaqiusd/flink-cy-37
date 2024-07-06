-- kafka Source 表
CREATE TABLE tb_events (
  uid String, -- 用户ID
  aid String, -- 广告ID
  eid String  -- 事件ID
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

-- 定义Mysql Sink标签, 支持upsert(主键相同的数据，没有就插入，有就更新)
CREATE TABLE mysql_sink (
  aid String, -- 广告ID
  eid String, -- 事件ID
  pv  BIGINT, -- 次数
  uv  BIGINT,  -- 人数
  PRIMARY KEY (aid, eid) NOT ENFORCED
)  WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://node-1.51doit.cn:3306/doit?characterEncoding=utf-8',
  'table-name' = 'tb_ad_count',
  'username' = 'root',
  'password' = '123456'
)
;

-- 从Source表中查询，然后将结果插入到Sink表中
insert into mysql_sink select aid, eid, count(*) pv, count(distinct(uid)) uv from tb_events where aid is not null and eid is not null group by aid, eid