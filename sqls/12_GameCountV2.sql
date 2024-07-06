-- 创建一个Kafka Source
CREATE TABLE KafkaGameLog (
  `gid` STRING, -- 游戏id
  `zid` STRING, -- 分区id
  `uid` STRING, -- 用户id
  `money` Double,
  `ptime` AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'tp-users',
  'properties.bootstrap.servers' = 'node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092',
  'properties.group.id' = 'g1',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'csv',
  'csv.ignore-parse-errors' = 'true'
)
;
-- 创建一个Sink表
CREATE TABLE Res_Table (
  `gid` STRING, -- 游戏id
  `zid` STRING, -- 分区id
  `money` Double,
  PRIMARY KEY (gid, zid) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://node-1.51doit.cn:3306/doit?characterEncoding=utf-8',
   'table-name' = 'tb_game_count',
   'username' = 'root',
   'password' = '123456'
)
;
--CREATE TABLE Test_Table (
--  `gid` STRING, -- 游戏id
--  `zid` STRING, -- 分区id
--  `money` Double
--) WITH (
--   'connector' = 'print'
--)


-- 创建临时的视图
CREATE TEMPORARY VIEW v_temp AS
select
  gid,
  zid,
  sum(money) money
from
  KafkaGameLog
where
  gid is not null and zid is not null
group by
  gid, zid, RAND_INTEGER(8), tumble(ptime, interval '30' second)
;

insert into Res_Table
select
  gid,
  zid,
  sum(money) money
from
  v_temp
group by
  gid, zid
