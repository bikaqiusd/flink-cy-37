-- flink SQL 支持按照条件创建cube（多维立方体，即所有维度的组合）
-- cube包含所有条件的组合，grouping set 是cube的子集，即可以选择指定哪些维度，
-- 按照 商品分类、品牌创建cube
-- p0001,手机,华为,1000
-- p0002,手机,苹果,2000
-- p0003,电脑,华为,5000

CREATE TABLE tb_events (
  `pid` string, --商品ID
  `cid` string , -- 商品分类ID
  `brand` string, -- 品牌ID
  `money` DOUBLE , -- 金额
  `ts` as PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'tp-users',
  'properties.bootstrap.servers' = 'node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'csv',
  'csv.ignore-parse-errors' = 'true'
)
;

-- MySQL sink表
CREATE TABLE tb_result (
  `brand` string,
  `cid` string ,
  `money` DOUBLE,
  PRIMARY KEY (cid, brand) NOT ENFORCED -- 将cid和品牌作为联合主键
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://node-1.51doit.cn:3306/doit?characterEncoding=utf-8',
  'table-name' = 'tb_cube', -- 写入的mysql数据库对应的表
  'username' = 'root',
  'password' = '123456'
)
;
--CREATE TABLE tb_result (
--  `brand` string,
--  `cid` string ,
--  `money` DOUBLE
--) WITH (
--  'connector' = 'print'
--)

insert into tb_result
select
  IFNULL(brand, 'NULL') brand,
  IFNULL(cid, 'NULL') cid,
  sum(money) money
from
  tb_events
group by grouping sets(
  (brand, cid),
  (brand),
  (cid)
)

