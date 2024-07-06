-- 创建一个Kafka Source, 窗口topN目前不支持ProcessingTime类型的窗口，即使使用了EventTime，也仅支持滚动窗口求TopN，不支持滑动窗口
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
  window_start timestamp(3),
  window_end timestamp(3),
  `rk` BIGINT
) WITH (
   'connector' = 'print'
)
;

-- 将同一个游戏、同一个分区，按照一定的时间划分窗口，然后将充值金额聚合累加
insert into Res_Table
select
  gid,
  zid,
  money,
  window_start,
  window_end,
  rk
from
(
  select
    gid,
    zid,
    window_start,
    window_end,
    money,
    row_number() over(partition by gid, window_start, window_end order by money desc) rk
  from
  (
    select
      gid,
      zid,
      window_start,
      window_end,
      sum(money) money
    from
      table(
        tumble(table KafkaGameLog, descriptor(ptime), interval '10' second)
      )
    group by
      gid, zid, window_start, window_end
  )
)
where rk <= 3