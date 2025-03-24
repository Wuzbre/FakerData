-- ODS层到DWD层的数据转换脚本
-- 负责数据清洗、转换和标准化

-- 设置动态分区
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.dynamic.partitions.pernode=10000;

-- 使用ODS数据库
USE purchase_sales_ods;

-- 定义处理日期变量（从命令行传入）
-- 使用示例: hive -hivevar dt=20240101 -f ods_to_dwd.hql
-- ${hivevar:dt} 为传入的日期分区，格式为yyyyMMdd

-- 创建DWD库
CREATE DATABASE IF NOT EXISTS purchase_sales_dwd;
USE purchase_sales_dwd;

-- 创建DWD层用户表
CREATE TABLE IF NOT EXISTS dwd_dim_user (
    user_id INT COMMENT '用户ID',
    username STRING COMMENT '用户名',
    email STRING COMMENT '用户邮箱',
    phone STRING COMMENT '用户手机号',
    nickname STRING COMMENT '用户昵称',
    status STRING COMMENT '账户状态',
    is_verified BOOLEAN COMMENT '邮箱/手机号是否验证',
    role STRING COMMENT '用户角色',
    created_date STRING COMMENT '账户创建日期',
    updated_date STRING COMMENT '最后更新日期',
    last_login_date STRING COMMENT '最后登录日期',
    membership_level STRING COMMENT '会员等级',
    preferred_language STRING COMMENT '用户语言偏好',
    preferred_currency STRING COMMENT '用户货币偏好',
    shipping_address STRING COMMENT '收货地址',
    billing_address STRING COMMENT '账单地址',
    newsletter_subscribed BOOLEAN COMMENT '是否订阅邮件',
    referral_code STRING COMMENT '推荐码',
    active_flag BOOLEAN COMMENT '是否活跃',
    start_date STRING COMMENT '记录开始日期',
    end_date STRING COMMENT '记录结束日期',
    current_flag BOOLEAN COMMENT '是否当前记录',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'DWD层用户维度表'
PARTITIONED BY (dt STRING COMMENT '数据日期分区，格式：yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 数据清洗与加载: 用户维度表
INSERT OVERWRITE TABLE purchase_sales_dwd.dwd_dim_user PARTITION (dt='${hivevar:dt}')
SELECT
    user_id,
    username,
    REGEXP_REPLACE(LOWER(email), '\\s+', '') AS email,  -- 清洗邮箱：转小写并去除空格
    REGEXP_REPLACE(phone, '[^0-9+]', '') AS phone,      -- 清洗电话：仅保留数字和+号
    COALESCE(nickname, username) AS nickname,           -- 昵称为空时使用用户名
    CASE 
        WHEN status IS NULL OR TRIM(status) = '' THEN 'unknown'
        ELSE LOWER(status) 
    END AS status,
    is_verified,
    COALESCE(role, 'customer') AS role,
    TO_DATE(created_at) AS created_date,
    TO_DATE(updated_at) AS updated_date,
    TO_DATE(last_login) AS last_login_date,
    COALESCE(membership_level, 'standard') AS membership_level,
    preferred_language,
    COALESCE(preferred_currency, 'CNY') AS preferred_currency,
    shipping_address,
    billing_address,
    COALESCE(newsletter_subscribed, FALSE) AS newsletter_subscribed,
    referral_code,
    CASE 
        WHEN status = 'active' AND (last_login IS NULL OR DATEDIFF(CURRENT_DATE, TO_DATE(last_login)) <= 90)
        THEN TRUE
        ELSE FALSE
    END AS active_flag,
    TO_DATE(created_at) AS start_date,
    CASE 
        WHEN status = 'deleted' THEN TO_DATE(updated_at)
        ELSE '9999-12-31'
    END AS end_date,
    CASE 
        WHEN status != 'deleted' THEN TRUE
        ELSE FALSE
    END AS current_flag,
    CURRENT_TIMESTAMP() AS etl_time
FROM
    purchase_sales_ods.ods_users
WHERE
    dt = '${hivevar:dt}';

-- 创建DWD层员工表
CREATE TABLE IF NOT EXISTS dwd_dim_employee (
    employee_id INT COMMENT '员工ID',
    first_name STRING COMMENT '员工名字',
    last_name STRING COMMENT '员工姓氏',
    full_name STRING COMMENT '员工全名',
    gender STRING COMMENT '性别',
    email STRING COMMENT '邮箱地址',
    phone STRING COMMENT '手机号',
    department STRING COMMENT '部门',
    position STRING COMMENT '职位',
    employment_status STRING COMMENT '雇佣状态',
    work_status STRING COMMENT '工作状态',
    hire_date STRING COMMENT '入职日期',
    termination_date STRING COMMENT '离职日期',
    start_date STRING COMMENT '记录开始日期',
    end_date STRING COMMENT '记录结束日期',
    current_flag BOOLEAN COMMENT '是否当前记录',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'DWD层员工维度表'
PARTITIONED BY (dt STRING COMMENT '数据日期分区，格式：yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 数据清洗与加载: 员工维度表
INSERT OVERWRITE TABLE purchase_sales_dwd.dwd_dim_employee PARTITION (dt='${hivevar:dt}')
SELECT
    employee_id,
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name) AS full_name,
    CASE 
        WHEN gender IS NULL OR TRIM(gender) = '' THEN 'unknown'
        ELSE LOWER(gender) 
    END AS gender,
    REGEXP_REPLACE(LOWER(email), '\\s+', '') AS email,
    REGEXP_REPLACE(phone, '[^0-9+]', '') AS phone,
    COALESCE(department, 'unknown') AS department,
    COALESCE(position, 'unknown') AS position,
    COALESCE(employment_status, 'unknown') AS employment_status,
    COALESCE(work_status, 'unknown') AS work_status,
    hire_date,
    termination_date,
    COALESCE(TO_DATE(created_at), hire_date) AS start_date,
    CASE 
        WHEN employment_status = 'resigned' OR employment_status = 'terminated' THEN 
            COALESCE(termination_date, TO_DATE(updated_at))
        ELSE '9999-12-31'
    END AS end_date,
    CASE 
        WHEN employment_status != 'resigned' AND employment_status != 'terminated' THEN TRUE
        ELSE FALSE
    END AS current_flag,
    CURRENT_TIMESTAMP() AS etl_time
FROM
    purchase_sales_ods.ods_employees
WHERE
    dt = '${hivevar:dt}';

-- 创建DWD层产品表
CREATE TABLE IF NOT EXISTS dwd_dim_product (
    product_id INT COMMENT '产品ID',
    product_name STRING COMMENT '产品名称',
    product_description STRING COMMENT '产品描述',
    sku STRING COMMENT '产品SKU',
    category_id INT COMMENT '产品分类ID',
    brand STRING COMMENT '品牌名称',
    model STRING COMMENT '产品型号或系列',
    color STRING COMMENT '产品颜色',
    price DECIMAL(10,2) COMMENT '销售价格',
    cost_price DECIMAL(10,2) COMMENT '成本价格',
    discount_price DECIMAL(10,2) COMMENT '折扣价格',
    profit_margin DECIMAL(10,4) COMMENT '利润率',
    currency STRING COMMENT '货币类型',
    status STRING COMMENT '产品状态',
    launch_date STRING COMMENT '上架日期',
    discontinued_date STRING COMMENT '停产日期',
    supplier_id INT COMMENT '供应商ID',
    manufacturer STRING COMMENT '制造商名称',
    country_of_origin STRING COMMENT '生产国家',
    start_date STRING COMMENT '记录开始日期',
    end_date STRING COMMENT '记录结束日期',
    current_flag BOOLEAN COMMENT '是否当前记录',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'DWD层产品维度表'
PARTITIONED BY (dt STRING COMMENT '数据日期分区，格式：yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 数据清洗与加载: 产品维度表
INSERT OVERWRITE TABLE purchase_sales_dwd.dwd_dim_product PARTITION (dt='${hivevar:dt}')
SELECT
    product_id,
    product_name,
    product_description,
    COALESCE(sku, CONCAT('SKU-', CAST(product_id AS STRING))) AS sku,
    category_id,
    COALESCE(brand, '未知品牌') AS brand,
    model,
    color,
    CASE 
        WHEN price <= 0 OR price IS NULL THEN NULL
        ELSE price
    END AS price,
    CASE 
        WHEN cost_price <= 0 OR cost_price IS NULL THEN NULL
        ELSE cost_price
    END AS cost_price,
    CASE 
        WHEN discount_price <= 0 OR discount_price IS NULL THEN price
        ELSE discount_price
    END AS discount_price,
    CASE 
        WHEN price > 0 AND cost_price > 0 THEN (price - cost_price) / price
        ELSE NULL
    END AS profit_margin,
    COALESCE(currency, 'CNY') AS currency,
    COALESCE(status, 'unknown') AS status,
    launch_date,
    discontinued_date,
    supplier_id,
    manufacturer,
    country_of_origin,
    COALESCE(TO_DATE(created_at), launch_date) AS start_date,
    CASE 
        WHEN status = 'discontinued' THEN COALESCE(discontinued_date, TO_DATE(updated_at))
        ELSE '9999-12-31'
    END AS end_date,
    CASE 
        WHEN status != 'discontinued' THEN TRUE
        ELSE FALSE
    END AS current_flag,
    CURRENT_TIMESTAMP() AS etl_time
FROM
    purchase_sales_ods.ods_products
WHERE
    dt = '${hivevar:dt}';

-- 创建DWD层销售订单事实表
CREATE TABLE IF NOT EXISTS dwd_fact_sales_order (
    sales_order_id INT COMMENT '销售订单ID',
    user_id INT COMMENT '客户ID',
    created_by INT COMMENT '创建者ID',
    order_date STRING COMMENT '下单日期',
    order_year INT COMMENT '订单年份',
    order_month INT COMMENT '订单月份',
    order_day INT COMMENT '订单日',
    order_hour INT COMMENT '订单小时',
    expected_delivery_date STRING COMMENT '预计交货日期',
    actual_delivery_date STRING COMMENT '实际交货日期',
    shipping_method STRING COMMENT '配送方式',
    shipping_cost DECIMAL(10,2) COMMENT '配送费用',
    status STRING COMMENT '订单状态',
    total_amount DECIMAL(12,2) COMMENT '订单总金额',
    currency STRING COMMENT '货币类型',
    discount DECIMAL(10,2) COMMENT '订单总折扣',
    final_amount DECIMAL(12,2) COMMENT '最终应支付金额',
    payment_status STRING COMMENT '支付状态',
    payment_method STRING COMMENT '支付方式',
    payment_date STRING COMMENT '支付日期',
    created_date STRING COMMENT '创建日期',
    delivery_days INT COMMENT '交付天数',
    is_delayed BOOLEAN COMMENT '是否延迟交付',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'DWD层销售订单事实表'
PARTITIONED BY (dt STRING COMMENT '数据日期分区，格式：yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 数据清洗与加载: 销售订单事实表
INSERT OVERWRITE TABLE purchase_sales_dwd.dwd_fact_sales_order PARTITION (dt='${hivevar:dt}')
SELECT
    sales_order_id,
    user_id,
    created_by,
    TO_DATE(order_date) AS order_date,
    YEAR(order_date) AS order_year,
    MONTH(order_date) AS order_month,
    DAY(order_date) AS order_day,
    HOUR(order_date) AS order_hour,
    TO_DATE(expected_delivery_date) AS expected_delivery_date,
    TO_DATE(actual_delivery_date) AS actual_delivery_date,
    shipping_method,
    COALESCE(shipping_cost, 0) AS shipping_cost,
    COALESCE(status, 'unknown') AS status,
    COALESCE(total_amount, 0) AS total_amount,
    COALESCE(currency, 'CNY') AS currency,
    COALESCE(discount, 0) AS discount,
    COALESCE(final_amount, total_amount) AS final_amount,
    COALESCE(payment_status, 'unknown') AS payment_status,
    payment_method,
    TO_DATE(payment_date) AS payment_date,
    TO_DATE(created_at) AS created_date,
    CASE
        WHEN actual_delivery_date IS NOT NULL AND order_date IS NOT NULL 
        THEN DATEDIFF(actual_delivery_date, order_date)
        ELSE NULL
    END AS delivery_days,
    CASE
        WHEN actual_delivery_date IS NOT NULL AND expected_delivery_date IS NOT NULL 
        THEN actual_delivery_date > expected_delivery_date
        ELSE NULL
    END AS is_delayed,
    CURRENT_TIMESTAMP() AS etl_time
FROM
    purchase_sales_ods.ods_sales_orders
WHERE
    dt = '${hivevar:dt}';

-- 创建DWD层销售订单明细事实表
CREATE TABLE IF NOT EXISTS dwd_fact_sales_order_item (
    sales_order_item_id INT COMMENT '销售订单项ID',
    sales_order_id INT COMMENT '销售订单ID',
    product_id INT COMMENT '产品ID',
    quantity INT COMMENT '数量',
    unit_price DECIMAL(10,2) COMMENT '单价',
    total_price DECIMAL(12,2) COMMENT '总价',
    discount DECIMAL(10,2) COMMENT '折扣金额',
    final_price DECIMAL(12,2) COMMENT '最终价格',
    order_date STRING COMMENT '订单日期',
    expected_delivery_date STRING COMMENT '预计交货日期',
    actual_delivery_date STRING COMMENT '实际交货日期',
    status STRING COMMENT '项目状态',
    warehouse_location STRING COMMENT '发货仓库位置',
    profit DECIMAL(12,2) COMMENT '利润',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'DWD层销售订单明细事实表'
PARTITIONED BY (dt STRING COMMENT '数据日期分区，格式：yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 数据清洗与加载: 销售订单明细事实表
INSERT OVERWRITE TABLE purchase_sales_dwd.dwd_fact_sales_order_item PARTITION (dt='${hivevar:dt}')
SELECT
    i.sales_order_item_id,
    i.sales_order_id,
    i.product_id,
    COALESCE(i.quantity, 0) AS quantity,
    COALESCE(i.unit_price, 0) AS unit_price,
    COALESCE(i.total_price, 0) AS total_price,
    COALESCE(i.discount, 0) AS discount,
    COALESCE(i.final_price, i.total_price) AS final_price,
    TO_DATE(o.order_date) AS order_date,
    TO_DATE(i.expected_delivery_date) AS expected_delivery_date,
    TO_DATE(i.actual_delivery_date) AS actual_delivery_date,
    COALESCE(i.status, 'unknown') AS status,
    i.warehouse_location,
    -- 计算利润 = 最终价格 - (数量 * 成本价)
    COALESCE(i.final_price, 0) - (COALESCE(i.quantity, 0) * COALESCE(p.cost_price, 0)) AS profit,
    CURRENT_TIMESTAMP() AS etl_time
FROM
    purchase_sales_ods.ods_sales_order_items i
LEFT JOIN
    purchase_sales_ods.ods_sales_orders o ON i.sales_order_id = o.sales_order_id AND o.dt = '${hivevar:dt}'
LEFT JOIN
    purchase_sales_ods.ods_products p ON i.product_id = p.product_id AND p.dt = '${hivevar:dt}'
WHERE
    i.dt = '${hivevar:dt}';

-- 创建DWD层采购订单事实表
CREATE TABLE IF NOT EXISTS dwd_fact_purchase_order (
    purchase_order_id INT COMMENT '采购订单ID',
    supplier_id INT COMMENT '供应商ID',
    created_by INT COMMENT '创建者ID',
    approved_by INT COMMENT '审批人ID',
    order_date STRING COMMENT '下单日期',
    order_year INT COMMENT '订单年份',
    order_month INT COMMENT '订单月份',
    order_day INT COMMENT '订单日',
    expected_delivery_date STRING COMMENT '预计交货日期',
    actual_delivery_date STRING COMMENT '实际交货日期',
    status STRING COMMENT '订单状态',
    total_amount DECIMAL(12,2) COMMENT '订单总金额',
    currency STRING COMMENT '货币类型',
    payment_status STRING COMMENT '支付状态',
    payment_method STRING COMMENT '支付方式',
    payment_date STRING COMMENT '支付日期',
    shipping_cost DECIMAL(10,2) COMMENT '配送费用',
    warehouse_location STRING COMMENT '存放仓库位置',
    created_date STRING COMMENT '创建日期',
    delivery_days INT COMMENT '交付天数',
    is_delayed BOOLEAN COMMENT '是否延迟交付',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'DWD层采购订单事实表'
PARTITIONED BY (dt STRING COMMENT '数据日期分区，格式：yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 数据清洗与加载: 采购订单事实表
INSERT OVERWRITE TABLE purchase_sales_dwd.dwd_fact_purchase_order PARTITION (dt='${hivevar:dt}')
SELECT
    purchase_order_id,
    supplier_id,
    created_by,
    approved_by,
    TO_DATE(order_date) AS order_date,
    YEAR(order_date) AS order_year,
    MONTH(order_date) AS order_month,
    DAY(order_date) AS order_day,
    TO_DATE(expected_delivery_date) AS expected_delivery_date,
    TO_DATE(actual_delivery_date) AS actual_delivery_date,
    COALESCE(status, 'unknown') AS status,
    COALESCE(total_amount, 0) AS total_amount,
    COALESCE(currency, 'CNY') AS currency,
    COALESCE(payment_status, 'unknown') AS payment_status,
    payment_method,
    TO_DATE(payment_date) AS payment_date,
    COALESCE(shipping_cost, 0) AS shipping_cost,
    warehouse_location,
    TO_DATE(created_at) AS created_date,
    CASE
        WHEN actual_delivery_date IS NOT NULL AND order_date IS NOT NULL 
        THEN DATEDIFF(actual_delivery_date, order_date)
        ELSE NULL
    END AS delivery_days,
    CASE
        WHEN actual_delivery_date IS NOT NULL AND expected_delivery_date IS NOT NULL 
        THEN actual_delivery_date > expected_delivery_date
        ELSE NULL
    END AS is_delayed,
    CURRENT_TIMESTAMP() AS etl_time
FROM
    purchase_sales_ods.ods_purchase_orders
WHERE
    dt = '${hivevar:dt}';

-- 创建DWD层采购订单明细事实表
CREATE TABLE IF NOT EXISTS dwd_fact_purchase_order_item (
    purchase_order_item_id INT COMMENT '采购订单项ID',
    purchase_order_id INT COMMENT '采购订单ID',
    product_id INT COMMENT '产品ID',
    quantity INT COMMENT '数量',
    unit_price DECIMAL(10,2) COMMENT '单价',
    total_price DECIMAL(12,2) COMMENT '总价',
    received_quantity INT COMMENT '已接收数量',
    completion_rate DECIMAL(5,2) COMMENT '完成率',
    status STRING COMMENT '项目状态',
    order_date STRING COMMENT '订单日期',
    expected_delivery_date STRING COMMENT '预计交货日期',
    actual_delivery_date STRING COMMENT '实际交货日期',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'DWD层采购订单明细事实表'
PARTITIONED BY (dt STRING COMMENT '数据日期分区，格式：yyyyMMdd')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 数据清洗与加载: 采购订单明细事实表
INSERT OVERWRITE TABLE purchase_sales_dwd.dwd_fact_purchase_order_item PARTITION (dt='${hivevar:dt}')
SELECT
    i.purchase_order_item_id,
    i.purchase_order_id,
    i.product_id,
    COALESCE(i.quantity, 0) AS quantity,
    COALESCE(i.unit_price, 0) AS unit_price,
    COALESCE(i.total_price, 0) AS total_price,
    COALESCE(i.received_quantity, 0) AS received_quantity,
    CASE 
        WHEN COALESCE(i.quantity, 0) > 0 
        THEN COALESCE(i.received_quantity, 0) / COALESCE(i.quantity, 0)
        ELSE 0
    END AS completion_rate,
    COALESCE(i.status, 'unknown') AS status,
    TO_DATE(o.order_date) AS order_date,
    TO_DATE(i.expected_delivery_date) AS expected_delivery_date,
    TO_DATE(i.actual_delivery_date) AS actual_delivery_date,
    CURRENT_TIMESTAMP() AS etl_time
FROM
    purchase_sales_ods.ods_purchase_order_items i
LEFT JOIN
    purchase_sales_ods.ods_purchase_orders o ON i.purchase_order_id = o.purchase_order_id AND o.dt = '${hivevar:dt}'
WHERE
    i.dt = '${hivevar:dt}'; 