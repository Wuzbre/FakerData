-- 用户统计汇总表
CREATE TABLE IF NOT EXISTS dws.dws_user_stats_df (
    user_id BIGINT COMMENT '用户ID',
    date_type STRING COMMENT '统计日期类型(day/week/month)',
    stat_date STRING COMMENT '统计日期',
    order_count BIGINT COMMENT '下单次数',
    order_amount DECIMAL(16,2) COMMENT '下单金额',
    actual_amount DECIMAL(16,2) COMMENT '实付金额',
    discount_amount DECIMAL(16,2) COMMENT '优惠金额',
    refund_count BIGINT COMMENT '退款次数',
    refund_amount DECIMAL(16,2) COMMENT '退款金额',
    category_count BIGINT COMMENT '购买类目数',
    product_count BIGINT COMMENT '购买商品数',
    avg_order_amount DECIMAL(16,2) COMMENT '平均订单金额',
    max_order_amount DECIMAL(16,2) COMMENT '最大订单金额',
    last_order_time TIMESTAMP COMMENT '最后下单时间',
    visit_count BIGINT COMMENT '访问次数',
    total_stay_time BIGINT COMMENT '总停留时间(秒)',
    avg_stay_time DECIMAL(16,2) COMMENT '平均停留时间(秒)',
    cart_count BIGINT COMMENT '加购次数',
    favorite_count BIGINT COMMENT '收藏次数',
    coupon_get_count BIGINT COMMENT '领券次数',
    coupon_use_count BIGINT COMMENT '用券次数',
    points_get_count BIGINT COMMENT '获得积分次数',
    points_use_count BIGINT COMMENT '使用积分次数',
    create_time TIMESTAMP COMMENT '创建时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
) 
COMMENT '用户统计汇总表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET; 