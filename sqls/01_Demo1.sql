-- 定义Source表 {"id": "10", "name": "tom", "age": 21, "salary": 10000}
CREATE TABLE tb_users_3 (
  `id` STRING,      -- 物理字段（physical column, 数据中所携带的字段）
  `name` String,    -- 物理字段
  `age` int,        -- 物理字段
  `salary` double,  -- 物理字段
  `income` as salary * 12,  -- 运算字段(computed column，经过一些运算，得到额外的字段)
  `event_time` TIMESTAMP(3) METADATA FROM 'timestamp', -- 元数据字段（metadata column），数据中没有，而是从Kafka中额外获取的
  `topic` STRING METADATA FROM 'topic', -- 当元数据的key与你定义表字段名称相同，可以省略from后面的内容
  `partition` BIGINT METADATA VIRTUAL,
  `offset` BIGINT METADATA VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'tp-users',
  'properties.bootstrap.servers' = 'node-1.51doit.cn:9092,node-2.51doit.cn:9092',
  'properties.group.id' = 'testGroup2',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
)
;

-- 定义Sink标签
CREATE TABLE print_table (
  `id` STRING,
  `name` String,
  `age` int,
  `salary` double,
  `income` double,
  `event_time` TIMESTAMP(3),
  `topic` STRING,
  `partition` BIGINT,
  `offset` BIGINT
)  WITH (
  'connector' = 'print'
)
;

-- 从Source表中查询，然后将结果插入到Sink表中
insert into print_table select * from tb_users_3