--kafka Source表
CREATE TABLE game_events (
  gid String, --游戏id
  zid String, --分区id
  uid String, --用户id
  money Double, --充值金额
  ptime as proctime() --当前时间
)
WITH (
  'connector' = 'kafka',
  'topic' = 'tp-users',
  'properties.bootstrap.servers' = 'node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092',
  'properties.group.id' = 'testGroup1',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'csv',
  'csv.ignore-parse-errors' = 'true'
)
;
--sink表
CREATE TABLE mysql_sink_gamecount (
  gid String, --游戏id
  zid String, --分区id
  sum_money Double,  --充值金额
  rownum bigint,
  win_start timestamp (3),
  win_end timestamp (3)
) with(
  'connector' = 'print'
)
;

insert into mysql_sink_gamecount
select
  gid,
  zid,
  sum_money,
  rownum,
  win_start,
  win_end
from
(
  select
    gid,
    zid,
    sum_money,
    win_start,
    win_end,
    row_number()over(partition by gid, win_start, win_end order by sum_money desc) rownum
  from
  (
    select
      gid,
      zid,
      sum(money) sum_money,
      tumble_start(ptime,interval '60' second ) win_start,
      tumble_end(ptime,interval '60' second ) win_end
    from
      game_events
    where
      gid is not null and zid is not null
    group by
      gid,
      zid,
      tumble(ptime,interval '60' second)
  )
)
where rownum <=3;
