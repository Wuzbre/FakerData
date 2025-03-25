-- 交易统计汇总表
CREATE TABLE IF NOT EXISTS dws.dws_trade_stats_df (
    date_type STRING COMMENT '统计日期类型(day/week/month)',
    stat_date STRING COMMENT '统计日期',
    order_count BIGINT COMMENT '订单总数',
    order_amount DECIMAL(16,2) COMMENT '订单总金额',
    actual_amount DECIMAL(16,2) COMMENT '实付总金额',
    discount_amount DECIMAL(16,2) COMMENT '优惠总金额',
    refund_count BIGINT COMMENT '退款订单数',
    refund_amount DECIMAL(16,2) COMMENT '退款总金额',
    user_count BIGINT COMMENT '下单用户数',
    new_user_count BIGINT COMMENT '新用户下单数',
    product_count BIGINT COMMENT '下单商品数',
    category_count BIGINT COMMENT '下单类目数',
    avg_order_amount DECIMAL(16,2) COMMENT '平均订单金额',
    max_order_amount DECIMAL(16,2) COMMENT '最大订单金额',
    user_avg_amount DECIMAL(16,2) COMMENT '用户平均消费',
    coupon_use_count BIGINT COMMENT '优惠券使用数',
    coupon_use_amount DECIMAL(16,2) COMMENT '优惠券优惠金额',
    points_use_count BIGINT COMMENT '积分使用次数',
    points_use_amount DECIMAL(16,2) COMMENT '积分抵扣金额',
    shipping_amount DECIMAL(16,2) COMMENT '运费总额',
    create_time TIMESTAMP COMMENT '创建时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
) 
COMMENT '交易统计汇总表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET; 