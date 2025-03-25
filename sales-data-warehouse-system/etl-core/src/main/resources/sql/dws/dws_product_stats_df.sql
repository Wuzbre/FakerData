-- 商品统计汇总表
CREATE TABLE IF NOT EXISTS dws.dws_product_stats_df (
    product_id BIGINT COMMENT '商品ID',
    category_id BIGINT COMMENT '类目ID',
    brand_id BIGINT COMMENT '品牌ID',
    date_type STRING COMMENT '统计日期类型(day/week/month)',
    stat_date STRING COMMENT '统计日期',
    order_count BIGINT COMMENT '被下单次数',
    order_amount DECIMAL(16,2) COMMENT '被下单金额',
    actual_amount DECIMAL(16,2) COMMENT '实付金额',
    discount_amount DECIMAL(16,2) COMMENT '优惠金额',
    refund_count BIGINT COMMENT '退款次数',
    refund_amount DECIMAL(16,2) COMMENT '退款金额',
    buyer_count BIGINT COMMENT '购买人数',
    avg_price DECIMAL(16,2) COMMENT '平均售价',
    visit_count BIGINT COMMENT '访问次数',
    visitor_count BIGINT COMMENT '访客数',
    total_stay_time BIGINT COMMENT '总停留时间(秒)',
    avg_stay_time DECIMAL(16,2) COMMENT '平均停留时间(秒)',
    cart_count BIGINT COMMENT '加购次数',
    cart_user_count BIGINT COMMENT '加购人数',
    favorite_count BIGINT COMMENT '收藏次数',
    favorite_user_count BIGINT COMMENT '收藏人数',
    review_count BIGINT COMMENT '评价次数',
    good_review_count BIGINT COMMENT '好评次数',
    conversion_rate DECIMAL(16,4) COMMENT '转化率',
    create_time TIMESTAMP COMMENT '创建时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
) 
COMMENT '商品统计汇总表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET; 