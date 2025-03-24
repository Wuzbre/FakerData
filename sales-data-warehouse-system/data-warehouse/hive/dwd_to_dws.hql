-- DWD层到DWS层的汇总脚本
-- 负责数据的聚合与统计

-- 设置动态分区
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.dynamic.partitions.pernode=10000;

-- 使用DWD数据库
USE purchase_sales_dwd;

-- 定义处理日期变量（从命令行传入）
-- 使用示例: hive -hivevar dt=20240101 -f dwd_to_dws.hql
-- ${hivevar:dt} 为传入的日期分区，格式为yyyyMMdd

-- 创建DWS库
CREATE DATABASE IF NOT EXISTS purchase_sales_dws;
USE purchase_sales_dws;

-- 创建DWS层销售日汇总表
CREATE TABLE IF NOT EXISTS dws_sales_day_agg (
    sales_date STRING COMMENT '销售日期',
    year INT COMMENT '年份',
    month INT COMMENT '月份',
    day INT COMMENT '日',
    total_orders INT COMMENT '总订单数',
    successful_orders INT COMMENT '成功订单数',
    cancelled_orders INT COMMENT '取消订单数',
    total_sales DECIMAL(20,2) COMMENT '总销售额',
    total_discount DECIMAL(20,2) COMMENT '总折扣额',
    net_sales DECIMAL(20,2) COMMENT '净销售额',
    total_profit DECIMAL(20,2) COMMENT '总利润',
    profit_margin DECIMAL(10,4) COMMENT '利润率',
    total_customers INT COMMENT '总客户数',
    new_customers INT COMMENT '新客户数',
    returning_customers INT COMMENT '回头客数',
    total_products INT COMMENT '售出产品总数',
    avg_order_value DECIMAL(12,2) COMMENT '平均订单金额',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'DWS层销售日汇总表'
PARTITIONED BY (dt STRING COMMENT '数据日期分区，格式：yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 销售日汇总统计
INSERT OVERWRITE TABLE purchase_sales_dws.dws_sales_day_agg PARTITION (dt='${hivevar:dt}')
SELECT
    so.order_date AS sales_date,
    so.order_year AS year,
    so.order_month AS month,
    so.order_day AS day,
    COUNT(DISTINCT so.sales_order_id) AS total_orders,
    COUNT(DISTINCT CASE WHEN so.status = 'completed' THEN so.sales_order_id ELSE NULL END) AS successful_orders,
    COUNT(DISTINCT CASE WHEN so.status = 'cancelled' THEN so.sales_order_id ELSE NULL END) AS cancelled_orders,
    SUM(so.total_amount) AS total_sales,
    SUM(so.discount) AS total_discount,
    SUM(so.final_amount) AS net_sales,
    SUM(soi.profit) AS total_profit,
    CASE 
        WHEN SUM(so.final_amount) > 0 
        THEN SUM(soi.profit) / SUM(so.final_amount)
        ELSE 0
    END AS profit_margin,
    COUNT(DISTINCT so.user_id) AS total_customers,
    COUNT(DISTINCT CASE WHEN u.created_date = so.order_date THEN so.user_id ELSE NULL END) AS new_customers,
    COUNT(DISTINCT CASE WHEN u.created_date < so.order_date THEN so.user_id ELSE NULL END) AS returning_customers,
    SUM(soi.quantity) AS total_products,
    SUM(so.final_amount) / COUNT(DISTINCT so.sales_order_id) AS avg_order_value,
    CURRENT_TIMESTAMP() AS etl_time
FROM 
    purchase_sales_dwd.dwd_fact_sales_order so
LEFT JOIN 
    purchase_sales_dwd.dwd_fact_sales_order_item soi ON so.sales_order_id = soi.sales_order_id AND soi.dt = '${hivevar:dt}'
LEFT JOIN 
    purchase_sales_dwd.dwd_dim_user u ON so.user_id = u.user_id AND u.dt = '${hivevar:dt}'
WHERE 
    so.dt = '${hivevar:dt}'
GROUP BY 
    so.order_date, so.order_year, so.order_month, so.order_day;

-- 创建DWS层销售月汇总表
CREATE TABLE IF NOT EXISTS dws_sales_month_agg (
    year_month STRING COMMENT '年月，格式：yyyy-MM',
    year INT COMMENT '年份',
    month INT COMMENT '月份',
    total_orders INT COMMENT '总订单数',
    successful_orders INT COMMENT '成功订单数',
    cancelled_orders INT COMMENT '取消订单数',
    total_sales DECIMAL(20,2) COMMENT '总销售额',
    total_discount DECIMAL(20,2) COMMENT '总折扣额',
    net_sales DECIMAL(20,2) COMMENT '净销售额',
    total_profit DECIMAL(20,2) COMMENT '总利润',
    profit_margin DECIMAL(10,4) COMMENT '利润率',
    total_customers INT COMMENT '总客户数',
    new_customers INT COMMENT '新客户数',
    returning_customers INT COMMENT '回头客数',
    total_products INT COMMENT '售出产品总数',
    avg_order_value DECIMAL(12,2) COMMENT '平均订单金额',
    month_over_month_growth DECIMAL(10,4) COMMENT '环比增长率',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'DWS层销售月汇总表'
PARTITIONED BY (dt STRING COMMENT '数据日期分区，格式：yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 销售月汇总统计
INSERT OVERWRITE TABLE purchase_sales_dws.dws_sales_month_agg PARTITION (dt='${hivevar:dt}')
SELECT
    CONCAT(so.order_year, '-', LPAD(so.order_month, 2, '0')) AS year_month,
    so.order_year AS year,
    so.order_month AS month,
    COUNT(DISTINCT so.sales_order_id) AS total_orders,
    COUNT(DISTINCT CASE WHEN so.status = 'completed' THEN so.sales_order_id ELSE NULL END) AS successful_orders,
    COUNT(DISTINCT CASE WHEN so.status = 'cancelled' THEN so.sales_order_id ELSE NULL END) AS cancelled_orders,
    SUM(so.total_amount) AS total_sales,
    SUM(so.discount) AS total_discount,
    SUM(so.final_amount) AS net_sales,
    SUM(soi.profit) AS total_profit,
    CASE 
        WHEN SUM(so.final_amount) > 0 
        THEN SUM(soi.profit) / SUM(so.final_amount)
        ELSE 0
    END AS profit_margin,
    COUNT(DISTINCT so.user_id) AS total_customers,
    COUNT(DISTINCT CASE WHEN CONCAT(YEAR(u.created_date), '-', LPAD(MONTH(u.created_date), 2, '0')) = CONCAT(so.order_year, '-', LPAD(so.order_month, 2, '0')) THEN so.user_id ELSE NULL END) AS new_customers,
    COUNT(DISTINCT CASE WHEN CONCAT(YEAR(u.created_date), '-', LPAD(MONTH(u.created_date), 2, '0')) < CONCAT(so.order_year, '-', LPAD(so.order_month, 2, '0')) THEN so.user_id ELSE NULL END) AS returning_customers,
    SUM(soi.quantity) AS total_products,
    SUM(so.final_amount) / COUNT(DISTINCT so.sales_order_id) AS avg_order_value,
    NULL AS month_over_month_growth, -- 此处暂时为NULL，后续可通过ETL过程计算
    CURRENT_TIMESTAMP() AS etl_time
FROM 
    purchase_sales_dwd.dwd_fact_sales_order so
LEFT JOIN 
    purchase_sales_dwd.dwd_fact_sales_order_item soi ON so.sales_order_id = soi.sales_order_id AND soi.dt = '${hivevar:dt}'
LEFT JOIN 
    purchase_sales_dwd.dwd_dim_user u ON so.user_id = u.user_id AND u.dt = '${hivevar:dt}'
WHERE 
    so.dt = '${hivevar:dt}'
GROUP BY 
    so.order_year, so.order_month;

-- 创建DWS层产品销售汇总表
CREATE TABLE IF NOT EXISTS dws_product_sales_agg (
    product_id INT COMMENT '产品ID',
    product_name STRING COMMENT '产品名称',
    brand STRING COMMENT '品牌',
    category_id INT COMMENT '分类ID',
    total_quantity INT COMMENT '总销售数量',
    total_sales DECIMAL(20,2) COMMENT '总销售额',
    total_profit DECIMAL(20,2) COMMENT '总利润',
    profit_margin DECIMAL(10,4) COMMENT '利润率',
    avg_unit_price DECIMAL(10,2) COMMENT '平均单价',
    max_unit_price DECIMAL(10,2) COMMENT '最高单价',
    min_unit_price DECIMAL(10,2) COMMENT '最低单价',
    total_discount DECIMAL(10,2) COMMENT '总折扣额',
    discount_rate DECIMAL(10,4) COMMENT '平均折扣率',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'DWS层产品销售汇总表'
PARTITIONED BY (dt STRING COMMENT '数据日期分区，格式：yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 产品销售汇总统计
INSERT OVERWRITE TABLE purchase_sales_dws.dws_product_sales_agg PARTITION (dt='${hivevar:dt}')
SELECT
    p.product_id,
    p.product_name,
    p.brand,
    p.category_id,
    SUM(soi.quantity) AS total_quantity,
    SUM(soi.total_price) AS total_sales,
    SUM(soi.profit) AS total_profit,
    CASE 
        WHEN SUM(soi.total_price) > 0 
        THEN SUM(soi.profit) / SUM(soi.total_price)
        ELSE 0
    END AS profit_margin,
    AVG(soi.unit_price) AS avg_unit_price,
    MAX(soi.unit_price) AS max_unit_price,
    MIN(soi.unit_price) AS min_unit_price,
    SUM(soi.discount) AS total_discount,
    CASE 
        WHEN SUM(soi.total_price + soi.discount) > 0 
        THEN SUM(soi.discount) / SUM(soi.total_price + soi.discount)
        ELSE 0
    END AS discount_rate,
    CURRENT_TIMESTAMP() AS etl_time
FROM 
    purchase_sales_dwd.dwd_fact_sales_order_item soi
LEFT JOIN 
    purchase_sales_dwd.dwd_dim_product p ON soi.product_id = p.product_id AND p.dt = '${hivevar:dt}'
WHERE 
    soi.dt = '${hivevar:dt}'
GROUP BY 
    p.product_id, p.product_name, p.brand, p.category_id;

-- 创建DWS层客户购买行为汇总表
CREATE TABLE IF NOT EXISTS dws_customer_behavior_agg (
    user_id INT COMMENT '用户ID',
    username STRING COMMENT '用户名',
    total_orders INT COMMENT '总订单数',
    first_order_date STRING COMMENT '首次购买日期',
    last_order_date STRING COMMENT '最近购买日期',
    total_spend DECIMAL(20,2) COMMENT '总消费金额',
    avg_order_value DECIMAL(12,2) COMMENT '平均订单金额',
    purchase_frequency DECIMAL(10,4) COMMENT '购买频率(订单数/活跃天数)',
    most_purchased_product_id INT COMMENT '最常购买产品ID',
    most_purchased_product_name STRING COMMENT '最常购买产品名称',
    most_purchased_category_id INT COMMENT '最常购买分类ID',
    avg_days_between_orders DECIMAL(10,2) COMMENT '平均订单间隔天数',
    customer_lifetime_value DECIMAL(20,2) COMMENT '客户生命周期价值',
    is_active BOOLEAN COMMENT '是否活跃客户(90天内有购买)',
    membership_level STRING COMMENT '会员等级',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'DWS层客户购买行为汇总表'
PARTITIONED BY (dt STRING COMMENT '数据日期分区，格式：yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 客户购买行为汇总统计
INSERT OVERWRITE TABLE purchase_sales_dws.dws_customer_behavior_agg PARTITION (dt='${hivevar:dt}')
WITH customer_orders AS (
    -- 按客户统计订单数据
    SELECT
        so.user_id,
        COUNT(DISTINCT so.sales_order_id) AS total_orders,
        MIN(so.order_date) AS first_order_date,
        MAX(so.order_date) AS last_order_date,
        SUM(so.final_amount) AS total_spend,
        SUM(so.final_amount) / COUNT(DISTINCT so.sales_order_id) AS avg_order_value,
        COUNT(DISTINCT so.order_date) AS active_days,
        DATEDIFF(MAX(so.order_date), MIN(so.order_date)) AS customer_lifespan_days
    FROM
        purchase_sales_dwd.dwd_fact_sales_order so
    WHERE
        so.dt = '${hivevar:dt}'
    GROUP BY
        so.user_id
),
customer_product_preference AS (
    -- 计算客户产品偏好
    SELECT
        so.user_id,
        soi.product_id,
        p.product_name,
        p.category_id,
        COUNT(*) AS purchase_count,
        ROW_NUMBER() OVER(PARTITION BY so.user_id ORDER BY COUNT(*) DESC) AS rank
    FROM
        purchase_sales_dwd.dwd_fact_sales_order so
    JOIN
        purchase_sales_dwd.dwd_fact_sales_order_item soi ON so.sales_order_id = soi.sales_order_id AND soi.dt = '${hivevar:dt}'
    JOIN
        purchase_sales_dwd.dwd_dim_product p ON soi.product_id = p.product_id AND p.dt = '${hivevar:dt}'
    WHERE
        so.dt = '${hivevar:dt}'
    GROUP BY
        so.user_id, soi.product_id, p.product_name, p.category_id
)
SELECT
    u.user_id,
    u.username,
    COALESCE(co.total_orders, 0) AS total_orders,
    co.first_order_date,
    co.last_order_date,
    COALESCE(co.total_spend, 0) AS total_spend,
    COALESCE(co.avg_order_value, 0) AS avg_order_value,
    CASE 
        WHEN COALESCE(co.active_days, 0) > 0 
        THEN COALESCE(co.total_orders, 0) / COALESCE(co.active_days, 1)
        ELSE 0
    END AS purchase_frequency,
    cpp.product_id AS most_purchased_product_id,
    cpp.product_name AS most_purchased_product_name,
    cpp.category_id AS most_purchased_category_id,
    CASE 
        WHEN COALESCE(co.total_orders, 0) > 1 AND COALESCE(co.customer_lifespan_days, 0) > 0
        THEN COALESCE(co.customer_lifespan_days, 0) / (COALESCE(co.total_orders, 1) - 1)
        ELSE NULL
    END AS avg_days_between_orders,
    -- 简化的客户生命周期价值计算(年化消费 * 预期客户关系年限)
    CASE 
        WHEN co.first_order_date IS NOT NULL AND co.customer_lifespan_days > 0
        THEN (co.total_spend / co.customer_lifespan_days) * 365 * 3  -- 假设平均客户关系持续3年
        ELSE COALESCE(co.total_spend, 0)
    END AS customer_lifetime_value,
    CASE 
        WHEN DATEDIFF(CURRENT_DATE, TO_DATE(COALESCE(co.last_order_date, '1970-01-01'))) <= 90
        THEN TRUE
        ELSE FALSE
    END AS is_active,
    u.membership_level,
    CURRENT_TIMESTAMP() AS etl_time
FROM 
    purchase_sales_dwd.dwd_dim_user u
LEFT JOIN 
    customer_orders co ON u.user_id = co.user_id
LEFT JOIN 
    customer_product_preference cpp ON u.user_id = cpp.user_id AND cpp.rank = 1
WHERE 
    u.dt = '${hivevar:dt}';

-- 创建DWS层供应商采购汇总表
CREATE TABLE IF NOT EXISTS dws_supplier_purchase_agg (
    supplier_id INT COMMENT '供应商ID',
    supplier_name STRING COMMENT '供应商名称',
    total_orders INT COMMENT '总订单数',
    total_purchase_amount DECIMAL(20,2) COMMENT '总采购金额',
    total_products INT COMMENT '采购产品总数',
    avg_order_value DECIMAL(12,2) COMMENT '平均订单金额',
    avg_lead_time DECIMAL(10,2) COMMENT '平均交货周期(天)',
    on_time_delivery_rate DECIMAL(10,4) COMMENT '准时交货率',
    quality_rating DECIMAL(5,2) COMMENT '质量评分',
    payment_terms STRING COMMENT '付款条件',
    last_order_date STRING COMMENT '最近采购日期',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'DWS层供应商采购汇总表'
PARTITIONED BY (dt STRING COMMENT '数据日期分区，格式：yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 供应商采购汇总统计
INSERT OVERWRITE TABLE purchase_sales_dws.dws_supplier_purchase_agg PARTITION (dt='${hivevar:dt}')
SELECT
    po.supplier_id,
    s.supplier_name, -- 假设存在供应商维度表
    COUNT(DISTINCT po.purchase_order_id) AS total_orders,
    SUM(po.total_amount) AS total_purchase_amount,
    SUM(poi.quantity) AS total_products,
    SUM(po.total_amount) / COUNT(DISTINCT po.purchase_order_id) AS avg_order_value,
    AVG(po.delivery_days) AS avg_lead_time,
    SUM(CASE WHEN po.is_delayed = FALSE THEN 1 ELSE 0 END) / COUNT(*) AS on_time_delivery_rate,
    AVG(s.quality_rating) AS quality_rating, -- 假设供应商维度表有质量评分
    s.payment_terms, -- 假设供应商维度表有付款条件
    MAX(po.order_date) AS last_order_date,
    CURRENT_TIMESTAMP() AS etl_time
FROM 
    purchase_sales_dwd.dwd_fact_purchase_order po
LEFT JOIN 
    purchase_sales_dwd.dwd_fact_purchase_order_item poi ON po.purchase_order_id = poi.purchase_order_id AND poi.dt = '${hivevar:dt}'
LEFT JOIN 
    purchase_sales_ods.ods_suppliers s ON po.supplier_id = s.supplier_id AND s.dt = '${hivevar:dt}' -- 假设ODS层有供应商表
WHERE 
    po.dt = '${hivevar:dt}'
GROUP BY 
    po.supplier_id, s.supplier_name, s.payment_terms; 