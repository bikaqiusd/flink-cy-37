-- 使用广告的数据，实现滑动窗口，使用ProcessingTime划分窗口，窗口长度为30秒，滑动步长为10秒
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
  pv  BIGINT, -- 次数
  win_start TIMESTAMP(3),
  win_end TIMESTAMP(3)
)  WITH (
  'connector' = 'print'
)
;

-- 从Source表中查询，然后将结果插入到Sink表中
insert into res_sink
select
  aid,
  eid,
  count(*) pv,
  hop_start(ptime, INTERVAL '10' SECONDS, INTERVAL '30' SECONDS) win_start, -- 获取滑动窗口的起始时间
  hop_end(ptime, INTERVAL '10' SECONDS, INTERVAL '30' SECONDS) win_end      -- 获取滑动窗口的结束数据
from
  tb_events
where
  aid is not null and eid is not null
group by
  -- 按照系统时间划分滑窗口，第一个时间为滑动步长，第二个时间为窗口长度
  hop(ptime, INTERVAL '10' SECONDS, INTERVAL '30' SECONDS),
  aid,
  eid
