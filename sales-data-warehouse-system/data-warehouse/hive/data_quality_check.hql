-- 数据质量检查脚本
-- 对各层的数据进行检验，确保数据质量符合要求

-- 定义处理日期变量（从命令行传入）
-- 使用示例: hive -hivevar dt=20240101 -f data_quality_check.hql
-- ${hivevar:dt} 为传入的日期分区，格式为yyyyMMdd

-- 创建数据质量检查结果表（如果不存在）
CREATE DATABASE IF NOT EXISTS data_quality;
USE data_quality;

CREATE TABLE IF NOT EXISTS dq_check_results (
    check_id STRING COMMENT '检查ID',
    check_date STRING COMMENT '检查日期',
    check_time TIMESTAMP COMMENT '检查时间',
    check_layer STRING COMMENT '检查数据层(ODS/DWD/DWS)',
    check_table STRING COMMENT '检查表名',
    check_type STRING COMMENT '检查类型(完整性/一致性/准确性等)',
    check_description STRING COMMENT '检查描述',
    check_result BOOLEAN COMMENT '检查结果(TRUE/FALSE)',
    check_details STRING COMMENT '检查详情',
    threshold DOUBLE COMMENT '阈值(如适用)',
    actual_value DOUBLE COMMENT '实际值(如适用)',
    severity STRING COMMENT '严重程度(HIGH/MEDIUM/LOW)',
    suggested_action STRING COMMENT '建议操作'
)
PARTITIONED BY (dt STRING COMMENT '数据日期分区，格式：yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ODS层数据质量检查：空值检查
INSERT INTO data_quality.dq_check_results PARTITION (dt='${hivevar:dt}')
WITH ods_users_nulls AS (
    SELECT
        'DQ-ODS-001' AS check_id,
        CURRENT_DATE() AS check_date,
        CURRENT_TIMESTAMP() AS check_time,
        'ODS' AS check_layer,
        'ods_users' AS check_table,
        'NULL检查' AS check_type,
        'ods_users表中空值比例检查' AS check_description,
        COUNT(*) AS total_rows,
        SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS null_user_id,
        SUM(CASE WHEN username IS NULL THEN 1 ELSE 0 END) AS null_username,
        SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS null_email,
        SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) AS null_status,
        SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END) AS null_created_at
    FROM
        purchase_sales_ods.ods_users
    WHERE
        dt = '${hivevar:dt}'
)
SELECT
    check_id,
    check_date,
    check_time,
    check_layer,
    check_table,
    check_type,
    check_description,
    CASE 
        WHEN (null_user_id / NULLIF(total_rows, 0)) <= 0.01 AND
             (null_username / NULLIF(total_rows, 0)) <= 0.05 AND
             (null_created_at / NULLIF(total_rows, 0)) <= 0.05
        THEN TRUE
        ELSE FALSE
    END AS check_result,
    CONCAT(
        'user_id空值率: ', ROUND(null_user_id / NULLIF(total_rows, 0) * 100, 2), '%, ',
        'username空值率: ', ROUND(null_username / NULLIF(total_rows, 0) * 100, 2), '%, ',
        'email空值率: ', ROUND(null_email / NULLIF(total_rows, 0) * 100, 2), '%, ',
        'status空值率: ', ROUND(null_status / NULLIF(total_rows, 0) * 100, 2), '%, ',
        'created_at空值率: ', ROUND(null_created_at / NULLIF(total_rows, 0) * 100, 2), '%'
    ) AS check_details,
    0.05 AS threshold,
    null_user_id / NULLIF(total_rows, 0) AS actual_value,
    CASE 
        WHEN (null_user_id / NULLIF(total_rows, 0)) > 0.01 THEN 'HIGH'
        WHEN (null_username / NULLIF(total_rows, 0)) > 0.05 OR
             (null_created_at / NULLIF(total_rows, 0)) > 0.05 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity,
    CASE 
        WHEN (null_user_id / NULLIF(total_rows, 0)) > 0.01 THEN '检查数据源中user_id为空的记录'
        WHEN (null_username / NULLIF(total_rows, 0)) > 0.05 THEN '检查数据源中username为空的记录'
        WHEN (null_created_at / NULLIF(total_rows, 0)) > 0.05 THEN '检查数据源中created_at为空的记录'
        ELSE '无需操作'
    END AS suggested_action
FROM
    ods_users_nulls;

-- ODS层数据质量检查：数据量检查
INSERT INTO data_quality.dq_check_results PARTITION (dt='${hivevar:dt}')
WITH ods_records_count AS (
    SELECT
        'DQ-ODS-002' AS check_id,
        CURRENT_DATE() AS check_date,
        CURRENT_TIMESTAMP() AS check_time,
        'ODS' AS check_layer,
        '所有ODS表' AS check_table,
        '数据量检查' AS check_type,
        'ODS层各表数据量检查' AS check_description,
        (SELECT COUNT(*) FROM purchase_sales_ods.ods_users WHERE dt = '${hivevar:dt}') AS users_count,
        (SELECT COUNT(*) FROM purchase_sales_ods.ods_products WHERE dt = '${hivevar:dt}') AS products_count,
        (SELECT COUNT(*) FROM purchase_sales_ods.ods_sales_orders WHERE dt = '${hivevar:dt}') AS sales_orders_count,
        (SELECT COUNT(*) FROM purchase_sales_ods.ods_sales_order_items WHERE dt = '${hivevar:dt}') AS sales_order_items_count,
        (SELECT COUNT(*) FROM purchase_sales_ods.ods_purchase_orders WHERE dt = '${hivevar:dt}') AS purchase_orders_count,
        (SELECT COUNT(*) FROM purchase_sales_ods.ods_purchase_order_items WHERE dt = '${hivevar:dt}') AS purchase_order_items_count
)
SELECT
    check_id,
    check_date,
    check_time,
    check_layer,
    check_table,
    check_type,
    check_description,
    CASE 
        WHEN users_count > 0 AND 
             products_count > 0 AND 
             sales_orders_count > 0 AND 
             sales_order_items_count > 0 AND
             purchase_orders_count > 0 AND
             purchase_order_items_count > 0
        THEN TRUE
        ELSE FALSE
    END AS check_result,
    CONCAT(
        'users数量: ', users_count, ', ',
        'products数量: ', products_count, ', ',
        'sales_orders数量: ', sales_orders_count, ', ',
        'sales_order_items数量: ', sales_order_items_count, ', ',
        'purchase_orders数量: ', purchase_orders_count, ', ',
        'purchase_order_items数量: ', purchase_order_items_count
    ) AS check_details,
    0 AS threshold,
    users_count AS actual_value,
    CASE 
        WHEN (users_count = 0 OR products_count = 0) THEN 'HIGH'
        WHEN (sales_orders_count = 0 OR sales_order_items_count = 0) THEN 'HIGH'
        WHEN (purchase_orders_count = 0 OR purchase_order_items_count = 0) THEN 'HIGH'
        ELSE 'LOW'
    END AS severity,
    CASE 
        WHEN users_count = 0 THEN '检查users表数据加载是否正常'
        WHEN products_count = 0 THEN '检查products表数据加载是否正常'
        WHEN sales_orders_count = 0 THEN '检查sales_orders表数据加载是否正常'
        WHEN sales_order_items_count = 0 THEN '检查sales_order_items表数据加载是否正常'
        WHEN purchase_orders_count = 0 THEN '检查purchase_orders表数据加载是否正常'
        WHEN purchase_order_items_count = 0 THEN '检查purchase_order_items表数据加载是否正常'
        ELSE '无需操作'
    END AS suggested_action
FROM
    ods_records_count;

-- ODS层数据质量检查：主键重复检查
INSERT INTO data_quality.dq_check_results PARTITION (dt='${hivevar:dt}')
WITH ods_duplicate_keys AS (
    SELECT
        'DQ-ODS-003' AS check_id,
        CURRENT_DATE() AS check_date,
        CURRENT_TIMESTAMP() AS check_time,
        'ODS' AS check_layer,
        'ods_users' AS check_table,
        '主键重复检查' AS check_type,
        'ods_users表中user_id重复检查' AS check_description,
        COUNT(*) AS total_rows,
        COUNT(user_id) - COUNT(DISTINCT user_id) AS duplicate_user_ids
    FROM
        purchase_sales_ods.ods_users
    WHERE
        dt = '${hivevar:dt}'
)
SELECT
    check_id,
    check_date,
    check_time,
    check_layer,
    check_table,
    check_type,
    check_description,
    CASE 
        WHEN duplicate_user_ids = 0 THEN TRUE
        ELSE FALSE
    END AS check_result,
    CONCAT('重复user_id数量: ', duplicate_user_ids) AS check_details,
    0 AS threshold,
    duplicate_user_ids AS actual_value,
    CASE 
        WHEN duplicate_user_ids > 0 THEN 'HIGH'
        ELSE 'LOW'
    END AS severity,
    CASE 
        WHEN duplicate_user_ids > 0 THEN '检查数据源中重复的user_id记录'
        ELSE '无需操作'
    END AS suggested_action
FROM
    ods_duplicate_keys;

-- DWD层数据完整性检查：ODS到DWD的数据转换完整性
INSERT INTO data_quality.dq_check_results PARTITION (dt='${hivevar:dt}')
WITH dwd_completeness AS (
    SELECT
        'DQ-DWD-001' AS check_id,
        CURRENT_DATE() AS check_date,
        CURRENT_TIMESTAMP() AS check_time,
        'DWD' AS check_layer,
        '所有DWD表' AS check_table,
        '数据转换完整性' AS check_type,
        'ODS到DWD数据量转换检查' AS check_description,
        (SELECT COUNT(*) FROM purchase_sales_ods.ods_users WHERE dt = '${hivevar:dt}') AS ods_users_count,
        (SELECT COUNT(*) FROM purchase_sales_dwd.dwd_dim_user WHERE dt = '${hivevar:dt}') AS dwd_users_count,
        (SELECT COUNT(*) FROM purchase_sales_ods.ods_products WHERE dt = '${hivevar:dt}') AS ods_products_count,
        (SELECT COUNT(*) FROM purchase_sales_dwd.dwd_dim_product WHERE dt = '${hivevar:dt}') AS dwd_products_count,
        (SELECT COUNT(*) FROM purchase_sales_ods.ods_sales_orders WHERE dt = '${hivevar:dt}') AS ods_sales_orders_count,
        (SELECT COUNT(*) FROM purchase_sales_dwd.dwd_fact_sales_order WHERE dt = '${hivevar:dt}') AS dwd_sales_orders_count
)
SELECT
    check_id,
    check_date,
    check_time,
    check_layer,
    check_table,
    check_type,
    check_description,
    CASE 
        WHEN dwd_users_count >= ods_users_count * 0.99 AND 
             dwd_products_count >= ods_products_count * 0.99 AND 
             dwd_sales_orders_count >= ods_sales_orders_count * 0.99
        THEN TRUE
        ELSE FALSE
    END AS check_result,
    CONCAT(
        'ODS到DWD用户表转换率: ', ROUND(dwd_users_count / NULLIF(ods_users_count, 0) * 100, 2), '%, ',
        'ODS到DWD产品表转换率: ', ROUND(dwd_products_count / NULLIF(ods_products_count, 0) * 100, 2), '%, ',
        'ODS到DWD销售订单表转换率: ', ROUND(dwd_sales_orders_count / NULLIF(ods_sales_orders_count, 0) * 100, 2), '%'
    ) AS check_details,
    0.99 AS threshold,
    dwd_users_count / NULLIF(ods_users_count, 0) AS actual_value,
    CASE 
        WHEN dwd_users_count < ods_users_count * 0.9 OR 
             dwd_products_count < ods_products_count * 0.9 OR 
             dwd_sales_orders_count < ods_sales_orders_count * 0.9 THEN 'HIGH'
        WHEN dwd_users_count < ods_users_count * 0.99 OR 
             dwd_products_count < ods_products_count * 0.99 OR 
             dwd_sales_orders_count < ods_sales_orders_count * 0.99 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity,
    CASE 
        WHEN dwd_users_count < ods_users_count * 0.99 THEN '检查用户数据转换过程'
        WHEN dwd_products_count < ods_products_count * 0.99 THEN '检查产品数据转换过程'
        WHEN dwd_sales_orders_count < ods_sales_orders_count * 0.99 THEN '检查销售订单数据转换过程'
        ELSE '无需操作'
    END AS suggested_action
FROM
    dwd_completeness;

-- DWD层数据一致性检查：销售订单与订单明细关联一致性
INSERT INTO data_quality.dq_check_results PARTITION (dt='${hivevar:dt}')
WITH dwd_consistency AS (
    SELECT
        'DQ-DWD-002' AS check_id,
        CURRENT_DATE() AS check_date,
        CURRENT_TIMESTAMP() AS check_time,
        'DWD' AS check_layer,
        'dwd_fact_sales_order/dwd_fact_sales_order_item' AS check_table,
        '数据一致性' AS check_type,
        '销售订单与订单明细一致性检查' AS check_description,
        COUNT(DISTINCT so.sales_order_id) AS total_orders,
        COUNT(DISTINCT CASE WHEN soi.sales_order_id IS NULL THEN so.sales_order_id ELSE NULL END) AS orphan_orders,
        -- 每个订单至少有一个明细项
        COUNT(DISTINCT so.sales_order_id) - COUNT(DISTINCT CASE 
            WHEN soi.sales_order_id IS NOT NULL THEN so.sales_order_id 
            ELSE NULL 
        END) AS orders_without_items
    FROM
        purchase_sales_dwd.dwd_fact_sales_order so
    LEFT JOIN
        purchase_sales_dwd.dwd_fact_sales_order_item soi ON so.sales_order_id = soi.sales_order_id AND soi.dt = '${hivevar:dt}'
    WHERE
        so.dt = '${hivevar:dt}'
)
SELECT
    check_id,
    check_date,
    check_time,
    check_layer,
    check_table,
    check_type,
    check_description,
    CASE 
        WHEN orphan_orders = 0 THEN TRUE
        ELSE FALSE
    END AS check_result,
    CONCAT(
        '总订单数: ', total_orders, ', ',
        '无明细订单数: ', orphan_orders, ', ',
        '无明细订单比例: ', ROUND(orphan_orders / NULLIF(total_orders, 0) * 100, 2), '%'
    ) AS check_details,
    0 AS threshold,
    orphan_orders / NULLIF(total_orders, 0) AS actual_value,
    CASE 
        WHEN orphan_orders / NULLIF(total_orders, 0) > 0.05 THEN 'HIGH'
        WHEN orphan_orders / NULLIF(total_orders, 0) > 0 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity,
    CASE 
        WHEN orphan_orders > 0 THEN '检查存在订单但无订单明细的记录'
        ELSE '无需操作'
    END AS suggested_action
FROM
    dwd_consistency;

-- DWS层数据聚合准确性检查
INSERT INTO data_quality.dq_check_results PARTITION (dt='${hivevar:dt}')
WITH dws_accuracy AS (
    SELECT
        'DQ-DWS-001' AS check_id,
        CURRENT_DATE() AS check_date,
        CURRENT_TIMESTAMP() AS check_time,
        'DWS' AS check_layer,
        'dws_sales_day_agg' AS check_table,
        '聚合准确性' AS check_type,
        '销售日汇总数据准确性检查' AS check_description,
        -- 计算DWS层汇总的总销售额
        (SELECT SUM(total_sales) FROM purchase_sales_dws.dws_sales_day_agg WHERE dt = '${hivevar:dt}') AS dws_total_sales,
        -- 计算DWD层订单明细表的总销售额
        (SELECT SUM(total_price) FROM purchase_sales_dwd.dwd_fact_sales_order_item WHERE dt = '${hivevar:dt}') AS dwd_total_sales,
        -- 计算差异百分比
        ABS((SELECT SUM(total_sales) FROM purchase_sales_dws.dws_sales_day_agg WHERE dt = '${hivevar:dt}') - 
            (SELECT SUM(total_price) FROM purchase_sales_dwd.dwd_fact_sales_order_item WHERE dt = '${hivevar:dt}')) / 
            NULLIF((SELECT SUM(total_price) FROM purchase_sales_dwd.dwd_fact_sales_order_item WHERE dt = '${hivevar:dt}'), 0) AS discrepancy_rate
)
SELECT
    check_id,
    check_date,
    check_time,
    check_layer,
    check_table,
    check_type,
    check_description,
    CASE 
        WHEN discrepancy_rate <= 0.001 THEN TRUE
        ELSE FALSE
    END AS check_result,
    CONCAT(
        'DWS层总销售额: ', dws_total_sales, ', ',
        'DWD层总销售额: ', dwd_total_sales, ', ',
        '差异百分比: ', ROUND(discrepancy_rate * 100, 4), '%'
    ) AS check_details,
    0.001 AS threshold,
    discrepancy_rate AS actual_value,
    CASE 
        WHEN discrepancy_rate > 0.01 THEN 'HIGH'
        WHEN discrepancy_rate > 0.001 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity,
    CASE 
        WHEN discrepancy_rate > 0.001 THEN '检查DWS层销售额汇总逻辑'
        ELSE '无需操作'
    END AS suggested_action
FROM
    dws_accuracy;

-- 查询结果摘要
SELECT 
    check_id, 
    check_layer, 
    check_table, 
    check_type, 
    check_result, 
    severity,
    suggested_action
FROM 
    data_quality.dq_check_results
WHERE 
    dt = '${hivevar:dt}'
ORDER BY 
    CASE severity 
        WHEN 'HIGH' THEN 1 
        WHEN 'MEDIUM' THEN 2 
        WHEN 'LOW' THEN 3 
    END,
    check_layer, 
    check_table; 