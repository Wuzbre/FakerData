-- 退款事实表
CREATE TABLE IF NOT EXISTS dwd.dwd_fact_refund (
    refund_id BIGINT COMMENT '退款ID',
    order_id BIGINT COMMENT '订单ID',
    user_id BIGINT COMMENT '用户ID',
    payment_id BIGINT COMMENT '支付ID',
    refund_amount DECIMAL(16,2) COMMENT '退款金额',
    refund_reason STRING COMMENT '退款原因',
    refund_type STRING COMMENT '退款类型',
    status STRING COMMENT '退款状态',
    refund_time TIMESTAMP COMMENT '退款时间',
    transaction_no STRING COMMENT '退款流水号',
    currency STRING COMMENT '货币类型',
    exchange_rate DECIMAL(16,6) COMMENT '汇率',
    operator_id BIGINT COMMENT '操作人ID',
    operator_name STRING COMMENT '操作人姓名',
    operator_notes STRING COMMENT '操作备注',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
) 
COMMENT '退款事实表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET; 