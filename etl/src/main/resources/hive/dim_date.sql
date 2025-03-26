-- 日期维度表
CREATE TABLE IF NOT EXISTS dim.dim_date (
    date_id STRING COMMENT '日期ID',
    year INT COMMENT '年份',
    month INT COMMENT '月份',
    day INT COMMENT '日',
    quarter INT COMMENT '季度',
    week_of_year INT COMMENT '年中第几周',
    day_of_week INT COMMENT '周中第几天',
    day_name STRING COMMENT '星期名称',
    month_name STRING COMMENT '月份名称',
    season STRING COMMENT '季节',
    is_workday BOOLEAN COMMENT '是否工作日',
    is_holiday BOOLEAN COMMENT '是否节假日',
    year_month STRING COMMENT '年月',
    year_quarter STRING COMMENT '年季度',
    dt STRING COMMENT '分区字段'
)
COMMENT '日期维度表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET; 