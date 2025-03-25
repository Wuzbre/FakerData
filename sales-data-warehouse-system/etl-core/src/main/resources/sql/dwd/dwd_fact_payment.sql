-- 支付事实表
CREATE TABLE IF NOT EXISTS dwd.dwd_fact_payment (
    payment_id BIGINT COMMENT '支付ID',
    order_id BIGINT COMMENT '订单ID',
    user_id BIGINT COMMENT '用户ID',
    payment_method STRING COMMENT '支付方式',
    transaction_no STRING COMMENT '交易流水号',
    payment_amount DECIMAL(16,2) COMMENT '支付金额',
    currency STRING COMMENT '货币类型',
    exchange_rate DECIMAL(16,6) COMMENT '汇率',
    status STRING COMMENT '支付状态',
    payment_time TIMESTAMP COMMENT '支付时间',
    callback_time TIMESTAMP COMMENT '回调时间',
    callback_content STRING COMMENT '回调内容',
    is_refunded BOOLEAN COMMENT '是否已退款',
    refund_amount DECIMAL(16,2) COMMENT '退款金额',
    refund_time TIMESTAMP COMMENT '退款时间',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
) 
COMMENT '支付事实表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET; 