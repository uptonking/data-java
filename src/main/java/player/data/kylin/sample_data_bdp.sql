
DROP TABLE IF EXISTS DEFAULT.bdp_customer_behavior;

CREATE TABLE DEFAULT.bdp_customer_behavior
(
user_id string
,register_date date comment '注册日期'
,gender string
,age smallint
,province string
,city string
,telecom string  comment '运营商'
,login_count int comment '登录次数'
,last_login_date date comment '最后一次登录日期'
,consume_count int comment '购买次数'
,repeated_consume_count int comment '重复购买次数'
,total_consume_money int comment '累计支付金额'
,last_consume_date date comment '最后一次购买日期'
,has_consumed_on_register string comment '注册当天是否购买'
,consume_on_register int  comment '注册当天支付金额'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

DROP TABLE IF EXISTS DEFAULT.bdp_date_1416;

-- 测试的时间范围为20140101-20160320

CREATE TABLE DEFAULT.bdp_date_1416
(
cal_dt date COMMENT 'Date, PK'
,year_beg_dt date COMMENT 'YEAR Begin Date'
,month_beg_dt date COMMENT 'Month Begin Date'
,week_beg_dt date COMMENT 'Week Begin Date 周始日期'
,week_num_of_year smallint
,day_of_month smallint
,day_of_week int
)
COMMENT 'Date Dimension Table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/root/Documents/dataseed/sampledata/bdp_customer_behavior.csv' OVERWRITE INTO TABLE DEFAULT.bdp_customer_behavior;
LOAD DATA LOCAL INPATH '/root/Documents/dataseed/sampledata/bdp_date_1416.csv' OVERWRITE INTO TABLE DEFAULT.bdp_date_1416;

