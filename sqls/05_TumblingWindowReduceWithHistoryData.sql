-- 划分滚动窗口，在窗口内实现增量聚合，并且与历史数据进行聚合
CREATE TABLE tb_events (
  uid String, -- 用户ID
  aid String, -- 广告ID
  eid String, -- 事件ID
  ptime as PROCTIME() -- 逻辑字段（运算字段），获取当前的系统时间
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

-- 定义print sink标签
CREATE TABLE res_sink (
  aid String, -- 广告ID
  eid String, -- 事件ID
  pv  BIGINT  -- 次数
)  WITH (
  'connector' = 'print'
)
;

-- 从Source表中查询，然后将结果插入到Sink表中
insert into res_sink
select
  aid,
  eid,
  sum(pv) pv
from
(
  select
    aid,
    eid,
    count(*) pv,
    tumble_start(ptime, INTERVAL '30' SECONDS) win_start, -- 获取滚动窗口的起始时间
    tumble_end(ptime, INTERVAL '30' SECONDS) win_end    -- 获取滚动窗口的结束数据
  from
    tb_events
  where
    aid is not null and eid is not null
  group by
    tumble(ptime, INTERVAL '30' SECONDS),        -- 按照滚动窗口分组
    aid,
    eid
)
group by
  aid,
  eid
