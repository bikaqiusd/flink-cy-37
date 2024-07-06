-- 使用广告的数据，实现滚动窗口，使用ProcessingTime划分窗口
CREATE TABLE tb_events (
  etime bigint,  -- 数据中的时间，格式为 1678086000000
  uid String, -- 用户ID
  aid String, -- 广告ID
  eid String,  -- 事件ID
  ts as TO_TIMESTAMP_LTZ(etime, 3),  -- 将lang类型的时间转成timestamp(3)格式
  WATERMARK FOR ts AS ts - INTERVAL '2' SECOND -- 使用数据中的etime - 2秒
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
  tumble_start(ts, INTERVAL '30' SECONDS) win_start, -- 获取滚动窗口的起始时间
  tumble_end(ts, INTERVAL '30' SECONDS) win_end    -- 获取滚动窗口的结束数据
from
  tb_events
where
  aid is not null and eid is not null
group by
  tumble(ts, INTERVAL '30' SECONDS),        -- 按照滚动窗口分组
  aid,
  eid
