-- 优惠券表
CREATE TABLE IF NOT EXISTS ods.ods_coupon (
    coupon_id BIGINT COMMENT '优惠券ID',
    coupon_name STRING COMMENT '优惠券名称',
    coupon_type STRING COMMENT '优惠券类型',
    coupon_code STRING COMMENT '优惠券码',
    discount_type STRING COMMENT '优惠类型(满减、折扣等)',
    discount_amount DECIMAL(16,2) COMMENT '优惠金额',
    discount_percent DECIMAL(5,2) COMMENT '折扣比例',
    min_order_amount DECIMAL(16,2) COMMENT '最低订单金额',
    max_discount_amount DECIMAL(16,2) COMMENT '最大优惠金额',
    start_time TIMESTAMP COMMENT '生效开始时间',
    end_time TIMESTAMP COMMENT '生效结束时间',
    total_count INT COMMENT '发放总量',
    used_count INT COMMENT '已使用数量',
    per_limit INT COMMENT '每人限领数量',
    status STRING COMMENT '状态',
    description STRING COMMENT '使用说明',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
) 
COMMENT '优惠券表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET; 