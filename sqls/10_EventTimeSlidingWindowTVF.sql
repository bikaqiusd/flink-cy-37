-- 使用广告的数据，实现滚动窗口(新的API)，使用ProcessingTime划分窗口
CREATE TABLE tb_events (
  ts bigint,  -- lang类型的时长，精确到毫秒
  uid String, -- 用户ID
  aid String, -- 广告ID
  eid String, -- 事件ID
  etime AS to_timestamp_ltz(ts, 3),
  watermark for etime as etime - interval '2' second
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
  window_start,
  window_end
from
  table(
    hop(table tb_events, descriptor(etime), interval '10' second, interval '30' second)
  )
where
  aid is not null and eid is not null
group by
  window_start,
  window_end,
  aid,
  eid
