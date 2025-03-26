-- 创建ADS数据库
CREATE DATABASE IF NOT EXISTS ads;

-- 1. 用户分析主题
-- 1.1 用户价值分布统计表（天粒度）
CREATE TABLE IF NOT EXISTS ads.ads_user_value_stats_d (
    dt STRING COMMENT '统计日期',
    user_level STRING COMMENT '用户等级',
    user_count BIGINT COMMENT '用户数',
    avg_customer_value DECIMAL(16,2) COMMENT '平均客户价值',
    avg_total_amount DECIMAL(16,2) COMMENT '平均消费金额',
    avg_order_count DECIMAL(16,2) COMMENT '平均订单数',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT '用户价值分布统计日表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 1.2 用户促销敏感度分析表（天粒度）
CREATE TABLE IF NOT EXISTS ads.ads_user_promo_sensitivity_d (
    dt STRING COMMENT '统计日期',
    user_level STRING COMMENT '用户等级',
    avg_promo_sensitivity DECIMAL(16,4) COMMENT '平均促销敏感度',
    avg_promo_orders DECIMAL(16,2) COMMENT '平均促销订单数',
    avg_discount_amount DECIMAL(16,2) COMMENT '平均优惠金额',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT '用户促销敏感度分析日表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 1.3 用户消费趋势分析表（月粒度）
CREATE TABLE IF NOT EXISTS ads.ads_user_consumption_trend_m (
    dt STRING COMMENT '统计月份',
    user_id STRING COMMENT '用户ID',
    user_level STRING COMMENT '用户等级',
    total_amount DECIMAL(16,2) COMMENT '消费总额',
    mom_rate DECIMAL(16,4) COMMENT '环比增长率',
    yoy_rate DECIMAL(16,4) COMMENT '同比增长率',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT '用户消费趋势月表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 2. 商品分析主题
-- 2.1 热销商品排行表（天粒度）
CREATE TABLE IF NOT EXISTS ads.ads_hot_product_rank_d (
    dt STRING COMMENT '统计日期',
    product_id STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_name STRING COMMENT '类目名称',
    total_quantity BIGINT COMMENT '销售数量',
    total_amount DECIMAL(16,2) COMMENT '销售金额',
    avg_rating DECIMAL(16,2) COMMENT '平均评分',
    popularity_score DECIMAL(16,2) COMMENT '热度得分',
    rank_num INT COMMENT '排名',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT '热销商品排行日表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 2.2 商品类目销售分析表（天粒度）
CREATE TABLE IF NOT EXISTS ads.ads_category_sales_stats_d (
    dt STRING COMMENT '统计日期',
    category_name STRING COMMENT '类目名称',
    total_amount DECIMAL(16,2) COMMENT '销售总额',
    total_quantity BIGINT COMMENT '销售数量',
    avg_rating DECIMAL(16,2) COMMENT '平均评分',
    product_count INT COMMENT '商品数量',
    sales_ratio DECIMAL(16,4) COMMENT '销售占比',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT '商品类目销售统计日表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 3. 供应商分析主题
-- 3.1 供应商综合评分表（天粒度）
CREATE TABLE IF NOT EXISTS ads.ads_supplier_score_d (
    dt STRING COMMENT '统计日期',
    supplier_id STRING COMMENT '供应商ID',
    supplier_name STRING COMMENT '供应商名称',
    total_sales_amount DECIMAL(16,2) COMMENT '销售总额',
    on_time_delivery_rate DECIMAL(16,4) COMMENT '准时交付率',
    quality_issue_rate DECIMAL(16,4) COMMENT '质量问题率',
    supplier_score DECIMAL(16,2) COMMENT '供应商得分',
    rank_num INT COMMENT '排名',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT '供应商评分日表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 3.2 供应商交付分析表（月粒度）
CREATE TABLE IF NOT EXISTS ads.ads_supplier_delivery_stats_m (
    dt STRING COMMENT '统计月份',
    supplier_id STRING COMMENT '供应商ID',
    supplier_name STRING COMMENT '供应商名称',
    avg_delivery_days DECIMAL(16,2) COMMENT '平均交付天数',
    on_time_delivery_rate DECIMAL(16,4) COMMENT '准时交付率',
    quality_issue_rate DECIMAL(16,4) COMMENT '质量问题率',
    total_deliveries BIGINT COMMENT '总交付订单数',
    mom_rate DECIMAL(16,4) COMMENT '环比增长率',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT '供应商交付分析月表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 4. 促销分析主题
-- 4.1 促销活动效果排行表（天粒度）
CREATE TABLE IF NOT EXISTS ads.ads_promotion_effect_rank_d (
    dt STRING COMMENT '统计日期',
    promotion_id STRING COMMENT '促销ID',
    promotion_name STRING COMMENT '促销名称',
    promotion_type STRING COMMENT '促销类型',
    total_sales_amount DECIMAL(16,2) COMMENT '销售总额',
    total_discount_amount DECIMAL(16,2) COMMENT '优惠总额',
    customer_count BIGINT COMMENT '参与客户数',
    promotion_roi DECIMAL(16,4) COMMENT '投资回报率',
    effectiveness_score DECIMAL(16,2) COMMENT '效果得分',
    rank_num INT COMMENT '排名',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT '促销活动效果排行日表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 4.2 促销活动客户分析表（天粒度）
CREATE TABLE IF NOT EXISTS ads.ads_promotion_customer_stats_d (
    dt STRING COMMENT '统计日期',
    promotion_id STRING COMMENT '促销ID',
    promotion_name STRING COMMENT '促销名称',
    customer_count BIGINT COMMENT '参与客户数',
    avg_user_orders DECIMAL(16,2) COMMENT '平均用户订单数',
    avg_user_amount DECIMAL(16,2) COMMENT '平均用户消费金额',
    retention_rate DECIMAL(16,4) COMMENT '客户留存率',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT '促销活动客户分析日表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 5. 库存分析主题
-- 5.1 库存健康状况表（天粒度）
CREATE TABLE IF NOT EXISTS ads.ads_inventory_health_d (
    dt STRING COMMENT '统计日期',
    product_id STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    current_stock INT COMMENT '当前库存',
    avg_daily_sales DECIMAL(16,2) COMMENT '日均销量',
    turnover_days DECIMAL(16,2) COMMENT '周转天数',
    stock_status STRING COMMENT '库存状态',
    stock_health_score DECIMAL(16,2) COMMENT '库存健康得分',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT '库存健康状况日表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 5.2 库存周转分析表（月粒度）
CREATE TABLE IF NOT EXISTS ads.ads_inventory_turnover_stats_m (
    dt STRING COMMENT '统计月份',
    category_name STRING COMMENT '类目名称',
    avg_turnover_days DECIMAL(16,2) COMMENT '平均周转天数',
    avg_turnover_rate DECIMAL(16,4) COMMENT '平均周转率',
    avg_efficiency_score DECIMAL(16,2) COMMENT '平均效率得分',
    stockout_count INT COMMENT '缺货商品数',
    overstock_count INT COMMENT '积压商品数',
    mom_rate DECIMAL(16,4) COMMENT '环比增长率',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT '库存周转分析月表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 6. 运营综合主题
-- 6.1 整体运营指标表（天粒度）
CREATE TABLE IF NOT EXISTS ads.ads_operation_overall_stats_d (
    dt STRING COMMENT '统计日期',
    total_users BIGINT COMMENT '总用户数',
    total_sales DECIMAL(16,2) COMMENT '总销售额',
    avg_customer_value DECIMAL(16,2) COMMENT '平均客户价值',
    promotion_order_ratio DECIMAL(16,4) COMMENT '促销订单占比',
    normal_stock_ratio DECIMAL(16,4) COMMENT '正常库存商品占比',
    avg_inventory_score DECIMAL(16,2) COMMENT '平均库存得分',
    avg_supplier_score DECIMAL(16,2) COMMENT '平均供应商得分',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT '整体运营指标日表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 6.2 运营趋势分析表（月粒度）
CREATE TABLE IF NOT EXISTS ads.ads_operation_trend_m (
    dt STRING COMMENT '统计月份',
    total_sales DECIMAL(16,2) COMMENT '总销售额',
    total_users BIGINT COMMENT '总用户数',
    total_orders BIGINT COMMENT '总订单数',
    avg_order_amount DECIMAL(16,2) COMMENT '平均订单金额',
    sales_mom_rate DECIMAL(16,4) COMMENT '销售额环比增长率',
    sales_yoy_rate DECIMAL(16,4) COMMENT '销售额同比增长率',
    user_mom_rate DECIMAL(16,4) COMMENT '用户数环比增长率',
    user_yoy_rate DECIMAL(16,4) COMMENT '用户数同比增长率',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT '运营趋势分析月表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET; 