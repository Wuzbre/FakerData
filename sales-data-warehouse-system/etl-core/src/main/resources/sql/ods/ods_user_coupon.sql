-- 用户优惠券表
CREATE TABLE IF NOT EXISTS ods.ods_user_coupon (
    id BIGINT COMMENT '主键ID',
    user_id BIGINT COMMENT '用户ID',
    coupon_id BIGINT COMMENT '优惠券ID',
    coupon_code STRING COMMENT '优惠券码',
    get_type STRING COMMENT '获取方式',
    status STRING COMMENT '使用状态',
    order_id BIGINT COMMENT '使用订单ID',
    used_time TIMESTAMP COMMENT '使用时间',
    start_time TIMESTAMP COMMENT '生效开始时间',
    end_time TIMESTAMP COMMENT '生效结束时间',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
) 
COMMENT '用户优惠券表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET; 