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
  `rank` BIGINT
) WITH (
   'connector' = 'print'
)
;

-- 将同一个游戏、同一个分区的充值进行进行聚合累加
insert into Res_Table
select
  gid,
  zid,
  money,
  `rank`
from
(
  select
    gid,
    zid,
    money,
    row_number() over(partition by gid order by money desc) `rank`
  from
  (
    select
      gid,
      zid,
      sum(money) money
    from
      KafkaGameLog
    where
      gid is not null and zid is not null
    group by
      gid, zid
  )
)
where `rank` <= 3