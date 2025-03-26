-- 创建数据库
CREATE DATABASE IF NOT EXISTS retail_analysis;
USE retail_analysis;

-- DWS层表
-- 1. 用户行为汇总表
CREATE TABLE IF NOT EXISTS `dws_user_behavior` (
  `user_id` BIGINT COMMENT '用户ID',
  `total_amount` DECIMAL(16,2) COMMENT '总消费金额',
  `order_count` INT COMMENT '订单数',
  `avg_order_amount` DECIMAL(16,2) COMMENT '平均订单金额',
  `last_order_time` DATETIME COMMENT '最近下单时间',
  `dt` DATE COMMENT '统计日期'
) ENGINE=OLAP
DUPLICATE KEY(`user_id`, `dt`)
COMMENT '用户行为汇总表'
DISTRIBUTED BY HASH(`user_id`) BUCKETS 10
PROPERTIES (
  "replication_num" = "3"
);

-- 2. 订单汇总表
CREATE TABLE IF NOT EXISTS `dws_order` (
  `order_id` BIGINT COMMENT '订单ID',
  `user_id` BIGINT COMMENT '用户ID',
  `order_amount` DECIMAL(16,2) COMMENT '订单金额',
  `is_promotion` TINYINT COMMENT '是否促销订单',
  `order_status` STRING COMMENT '订单状态',
  `create_time` DATETIME COMMENT '创建时间',
  `dt` DATE COMMENT '统计日期'
) ENGINE=OLAP
DUPLICATE KEY(`order_id`, `dt`)
COMMENT '订单汇总表'
DISTRIBUTED BY HASH(`order_id`) BUCKETS 10
PROPERTIES (
  "replication_num" = "3"
);

-- 3. 库存汇总表
CREATE TABLE IF NOT EXISTS `dws_inventory` (
  `product_id` BIGINT COMMENT '商品ID',
  `current_stock` INT COMMENT '当前库存',
  `safety_stock` INT COMMENT '安全库存',
  `max_stock` INT COMMENT '最大库存',
  `stock_status` STRING COMMENT '库存状态',
  `stock_health_score` DECIMAL(5,2) COMMENT '库存健康评分',
  `dt` DATE COMMENT '统计日期'
) ENGINE=OLAP
DUPLICATE KEY(`product_id`, `dt`)
COMMENT '库存汇总表'
DISTRIBUTED BY HASH(`product_id`) BUCKETS 10
PROPERTIES (
  "replication_num" = "3"
);

-- 4. 供应商表现汇总表
CREATE TABLE IF NOT EXISTS `dws_supplier_performance` (
  `supplier_id` BIGINT COMMENT '供应商ID',
  `delivery_score` DECIMAL(5,2) COMMENT '交付评分',
  `quality_score` DECIMAL(5,2) COMMENT '质量评分',
  `service_score` DECIMAL(5,2) COMMENT '服务评分',
  `supplier_score` DECIMAL(5,2) COMMENT '综合评分',
  `dt` DATE COMMENT '统计日期'
) ENGINE=OLAP
DUPLICATE KEY(`supplier_id`, `dt`)
COMMENT '供应商表现汇总表'
DISTRIBUTED BY HASH(`supplier_id`) BUCKETS 10
PROPERTIES (
  "replication_num" = "3"
);

-- ADS层表
-- 1. 用户价值分析表
CREATE TABLE IF NOT EXISTS `ads_user_value` (
  `user_id` BIGINT COMMENT '用户ID',
  `user_value_score` DECIMAL(5,2) COMMENT '用户价值评分',
  `value_level` STRING COMMENT '价值等级',
  `total_amount` DECIMAL(16,2) COMMENT '累计消费金额',
  `avg_order_amount` DECIMAL(16,2) COMMENT '平均订单金额',
  `dt` DATE COMMENT '统计日期'
) ENGINE=OLAP
DUPLICATE KEY(`user_id`, `dt`)
COMMENT '用户价值分析表'
DISTRIBUTED BY HASH(`user_id`) BUCKETS 10
PROPERTIES (
  "replication_num" = "3"
);

-- 2. 用户促销敏感度分析表
CREATE TABLE IF NOT EXISTS `ads_user_promo_sensitivity` (
  `user_id` BIGINT COMMENT '用户ID',
  `promo_order_ratio` DECIMAL(5,4) COMMENT '促销订单占比',
  `avg_promo_discount` DECIMAL(5,2) COMMENT '平均促销折扣',
  `sensitivity_score` DECIMAL(5,2) COMMENT '促销敏感度评分',
  `dt` DATE COMMENT '统计日期'
) ENGINE=OLAP
DUPLICATE KEY(`user_id`, `dt`)
COMMENT '用户促销敏感度分析表'
DISTRIBUTED BY HASH(`user_id`) BUCKETS 10
PROPERTIES (
  "replication_num" = "3"
);

-- 3. 用户消费趋势分析表
CREATE TABLE IF NOT EXISTS `ads_user_consumption_trend` (
  `dt` DATE COMMENT '统计月份',
  `total_users` BIGINT COMMENT '总用户数',
  `active_users` BIGINT COMMENT '活跃用户数',
  `new_users` BIGINT COMMENT '新增用户数',
  `total_amount` DECIMAL(16,2) COMMENT '总消费金额',
  `avg_user_amount` DECIMAL(16,2) COMMENT '人均消费金额',
  `mom_user_rate` DECIMAL(5,4) COMMENT '用户环比增长率',
  `mom_amount_rate` DECIMAL(5,4) COMMENT '金额环比增长率'
) ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '用户消费趋势分析表'
DISTRIBUTED BY HASH(`dt`) BUCKETS 10
PROPERTIES (
  "replication_num" = "3"
);

-- 4. 热销商品排行表
CREATE TABLE IF NOT EXISTS `ads_hot_product_rank` (
  `product_id` BIGINT COMMENT '商品ID',
  `product_name` STRING COMMENT '商品名称',
  `sales_amount` DECIMAL(16,2) COMMENT '销售金额',
  `sales_count` BIGINT COMMENT '销售数量',
  `rank` INT COMMENT '排名',
  `dt` DATE COMMENT '统计日期'
) ENGINE=OLAP
DUPLICATE KEY(`product_id`, `dt`)
COMMENT '热销商品排行表'
DISTRIBUTED BY HASH(`product_id`) BUCKETS 10
PROPERTIES (
  "replication_num" = "3"
);

-- 5. 品类销售分析表
CREATE TABLE IF NOT EXISTS `ads_category_sales` (
  `category_id` BIGINT COMMENT '品类ID',
  `category_name` STRING COMMENT '品类名称',
  `sales_amount` DECIMAL(16,2) COMMENT '销售金额',
  `sales_count` BIGINT COMMENT '销售数量',
  `mom_amount_rate` DECIMAL(5,4) COMMENT '销售额环比增长率',
  `dt` DATE COMMENT '统计日期'
) ENGINE=OLAP
DUPLICATE KEY(`category_id`, `dt`)
COMMENT '品类销售分析表'
DISTRIBUTED BY HASH(`category_id`) BUCKETS 10
PROPERTIES (
  "replication_num" = "3"
);

-- 6. 供应商评分表
CREATE TABLE IF NOT EXISTS `ads_supplier_score` (
  `supplier_id` BIGINT COMMENT '供应商ID',
  `supplier_name` STRING COMMENT '供应商名称',
  `total_score` DECIMAL(5,2) COMMENT '总评分',
  `delivery_score` DECIMAL(5,2) COMMENT '交付评分',
  `quality_score` DECIMAL(5,2) COMMENT '质量评分',
  `service_score` DECIMAL(5,2) COMMENT '服务评分',
  `rank` INT COMMENT '排名',
  `dt` DATE COMMENT '统计日期'
) ENGINE=OLAP
DUPLICATE KEY(`supplier_id`, `dt`)
COMMENT '供应商评分表'
DISTRIBUTED BY HASH(`supplier_id`) BUCKETS 10
PROPERTIES (
  "replication_num" = "3"
);

-- 7. 供应商交付分析表
CREATE TABLE IF NOT EXISTS `ads_supplier_delivery` (
  `supplier_id` BIGINT COMMENT '供应商ID',
  `supplier_name` STRING COMMENT '供应商名称',
  `on_time_rate` DECIMAL(5,4) COMMENT '准时交付率',
  `full_delivery_rate` DECIMAL(5,4) COMMENT '完整交付率',
  `avg_delivery_days` DECIMAL(5,2) COMMENT '平均交付天数',
  `dt` DATE COMMENT '统计日期'
) ENGINE=OLAP
DUPLICATE KEY(`supplier_id`, `dt`)
COMMENT '供应商交付分析表'
DISTRIBUTED BY HASH(`supplier_id`) BUCKETS 10
PROPERTIES (
  "replication_num" = "3"
);

-- 8. 促销效果分析表
CREATE TABLE IF NOT EXISTS `ads_promotion_effect` (
  `promotion_id` BIGINT COMMENT '促销活动ID',
  `promotion_name` STRING COMMENT '促销活动名称',
  `sales_amount` DECIMAL(16,2) COMMENT '销售金额',
  `sales_count` BIGINT COMMENT '销售数量',
  `customer_count` BIGINT COMMENT '参与客户数',
  `roi` DECIMAL(8,4) COMMENT '投资回报率',
  `effect_score` DECIMAL(5,2) COMMENT '效果评分',
  `dt` DATE COMMENT '统计日期'
) ENGINE=OLAP
DUPLICATE KEY(`promotion_id`, `dt`)
COMMENT '促销效果分析表'
DISTRIBUTED BY HASH(`promotion_id`) BUCKETS 10
PROPERTIES (
  "replication_num" = "3"
);

-- 9. 促销客户分析表
CREATE TABLE IF NOT EXISTS `ads_promotion_customer` (
  `promotion_id` BIGINT COMMENT '促销活动ID',
  `customer_segment` STRING COMMENT '客户群体',
  `customer_count` BIGINT COMMENT '客户数量',
  `avg_order_amount` DECIMAL(16,2) COMMENT '平均订单金额',
  `repurchase_rate` DECIMAL(5,4) COMMENT '复购率',
  `dt` DATE COMMENT '统计日期'
) ENGINE=OLAP
DUPLICATE KEY(`promotion_id`, `customer_segment`, `dt`)
COMMENT '促销客户分析表'
DISTRIBUTED BY HASH(`promotion_id`) BUCKETS 10
PROPERTIES (
  "replication_num" = "3"
);

-- 10. 库存健康分析表
CREATE TABLE IF NOT EXISTS `ads_inventory_health` (
  `product_id` BIGINT COMMENT '商品ID',
  `product_name` STRING COMMENT '商品名称',
  `current_stock` INT COMMENT '当前库存',
  `turnover_days` DECIMAL(8,2) COMMENT '周转天数',
  `stock_status` STRING COMMENT '库存状态',
  `health_score` DECIMAL(5,2) COMMENT '健康评分',
  `dt` DATE COMMENT '统计日期'
) ENGINE=OLAP
DUPLICATE KEY(`product_id`, `dt`)
COMMENT '库存健康分析表'
DISTRIBUTED BY HASH(`product_id`) BUCKETS 10
PROPERTIES (
  "replication_num" = "3"
);

-- 11. 库存周转统计表
CREATE TABLE IF NOT EXISTS `ads_inventory_turnover_stats` (
  `category_id` BIGINT COMMENT '品类ID',
  `category_name` STRING COMMENT '品类名称',
  `avg_turnover_days` DECIMAL(8,2) COMMENT '平均周转天数',
  `turnover_rate` DECIMAL(5,4) COMMENT '周转率',
  `efficiency_score` DECIMAL(5,2) COMMENT '效率评分',
  `stockout_count` INT COMMENT '缺货商品数',
  `overstock_count` INT COMMENT '积压商品数',
  `dt` DATE COMMENT '统计月份'
) ENGINE=OLAP
DUPLICATE KEY(`category_id`, `dt`)
COMMENT '库存周转统计表'
DISTRIBUTED BY HASH(`category_id`) BUCKETS 10
PROPERTIES (
  "replication_num" = "3"
);

-- 12. 运营整体指标统计表
CREATE TABLE IF NOT EXISTS `ads_operation_overall_stats` (
  `total_users` BIGINT COMMENT '总用户数',
  `total_sales` DECIMAL(16,2) COMMENT '总销售额',
  `avg_customer_value` DECIMAL(16,2) COMMENT '平均客户价值',
  `promotion_order_ratio` DECIMAL(5,4) COMMENT '促销订单占比',
  `normal_stock_ratio` DECIMAL(5,4) COMMENT '正常库存商品占比',
  `avg_inventory_score` DECIMAL(5,2) COMMENT '平均库存评分',
  `avg_supplier_score` DECIMAL(5,2) COMMENT '平均供应商评分',
  `dt` DATE COMMENT '统计日期'
) ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '运营整体指标统计表'
DISTRIBUTED BY HASH(`dt`) BUCKETS 10
PROPERTIES (
  "replication_num" = "3"
);

-- 13. 运营趋势分析表
CREATE TABLE IF NOT EXISTS `ads_operation_trend` (
  `dt` DATE COMMENT '统计月份',
  `total_sales` DECIMAL(16,2) COMMENT '总销售额',
  `total_users` BIGINT COMMENT '总用户数',
  `total_orders` BIGINT COMMENT '总订单数',
  `avg_order_amount` DECIMAL(16,2) COMMENT '平均订单金额',
  `sales_mom_rate` DECIMAL(5,4) COMMENT '销售额环比增长率',
  `sales_yoy_rate` DECIMAL(5,4) COMMENT '销售额同比增长率',
  `user_mom_rate` DECIMAL(5,4) COMMENT '用户数环比增长率',
  `user_yoy_rate` DECIMAL(5,4) COMMENT '用户数同比增长率'
) ENGINE=OLAP
DUPLICATE KEY(`dt`)
COMMENT '运营趋势分析表'
DISTRIBUTED BY HASH(`dt`) BUCKETS 10
PROPERTIES (
  "replication_num" = "3"
); 