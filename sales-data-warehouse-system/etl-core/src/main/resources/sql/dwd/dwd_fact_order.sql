-- 订单事实表
CREATE TABLE IF NOT EXISTS dwd.dwd_fact_order (
    order_id BIGINT COMMENT '订单ID',
    user_id BIGINT COMMENT '用户ID',
    order_status STRING COMMENT '订单状态',
    payment_status STRING COMMENT '支付状态',
    shipping_status STRING COMMENT '配送状态',
    address_id BIGINT COMMENT '收货地址ID',
    province STRING COMMENT '省份',
    city STRING COMMENT '城市',
    district STRING COMMENT '区县',
    coupon_id BIGINT COMMENT '优惠券ID',
    coupon_type STRING COMMENT '优惠券类型',
    coupon_amount DECIMAL(16,2) COMMENT '优惠券金额',
    order_amount DECIMAL(16,2) COMMENT '订单金额',
    discount_amount DECIMAL(16,2) COMMENT '优惠金额',
    shipping_amount DECIMAL(16,2) COMMENT '运费',
    payment_amount DECIMAL(16,2) COMMENT '支付金额',
    shipping_company STRING COMMENT '快递公司',
    shipping_sn STRING COMMENT '快递单号',
    payment_time TIMESTAMP COMMENT '支付时间',
    shipping_time TIMESTAMP COMMENT '发货时间',
    receive_time TIMESTAMP COMMENT '收货时间',
    order_comment STRING COMMENT '订单备注',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
) 
COMMENT '订单事实表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET; 