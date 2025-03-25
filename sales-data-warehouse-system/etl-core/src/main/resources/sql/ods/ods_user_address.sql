-- 用户地址表
CREATE TABLE IF NOT EXISTS ods.ods_user_address (
    address_id BIGINT COMMENT '地址ID',
    user_id BIGINT COMMENT '用户ID',
    receiver_name STRING COMMENT '收货人姓名',
    receiver_phone STRING COMMENT '收货人电话',
    province STRING COMMENT '省份',
    city STRING COMMENT '城市',
    district STRING COMMENT '区县',
    detailed_address STRING COMMENT '详细地址',
    is_default BOOLEAN COMMENT '是否默认地址',
    postal_code STRING COMMENT '邮政编码',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
) 
COMMENT '用户地址表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET; 