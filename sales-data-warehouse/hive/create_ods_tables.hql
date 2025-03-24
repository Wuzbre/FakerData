-- ODS层表结构定义脚本
-- 原始数据存储层，与源系统保持一致的结构

CREATE DATABASE IF NOT EXISTS purchase_sales_ods;
USE purchase_sales_ods;

-- 创建ODS层用户表
CREATE TABLE IF NOT EXISTS ods_users (
    user_id INT COMMENT '用户ID',
    username STRING COMMENT '用户名',
    email STRING COMMENT '用户邮箱',
    phone STRING COMMENT '用户手机号',
    password STRING COMMENT '密码',
    nickname STRING COMMENT '用户昵称',
    avatar_url STRING COMMENT '用户头像URL',
    status STRING COMMENT '账户状态',
    is_verified BOOLEAN COMMENT '邮箱/手机号是否验证',
    role STRING COMMENT '用户角色',
    created_at TIMESTAMP COMMENT '账户创建时间',
    updated_at TIMESTAMP COMMENT '最后更新时间',
    last_login TIMESTAMP COMMENT '最后登录时间',
    account_balance DECIMAL(12,2) COMMENT '账户余额',
    points_balance INT COMMENT '积分余额',
    membership_level STRING COMMENT '会员等级',
    failed_attempts INT COMMENT '登录失败次数',
    lock_until TIMESTAMP COMMENT '锁定时间',
    two_factor_enabled BOOLEAN COMMENT '是否启用双因素认证',
    preferred_language STRING COMMENT '用户语言偏好',
    preferred_currency STRING COMMENT '用户货币偏好',
    shipping_address STRING COMMENT '收货地址',
    billing_address STRING COMMENT '账单地址',
    newsletter_subscribed BOOLEAN COMMENT '是否订阅邮件',
    referral_code STRING COMMENT '推荐码',
    referred_by_user_id INT COMMENT '推荐用户ID',
    cart_id INT COMMENT '购物车ID',
    order_count INT COMMENT '订单数量',
    order_total DECIMAL(14,2) COMMENT '累计消费总额',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'ODS层用户表'
PARTITIONED BY (dt STRING COMMENT '数据日期分区，格式：yyyyMMdd')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE;

-- 创建ODS层员工表
CREATE TABLE IF NOT EXISTS ods_employees (
    employee_id INT COMMENT '员工ID',
    first_name STRING COMMENT '员工名字',
    last_name STRING COMMENT '员工姓氏',
    gender STRING COMMENT '性别',
    birth_date DATE COMMENT '出生日期',
    email STRING COMMENT '邮箱地址',
    phone STRING COMMENT '手机号',
    address STRING COMMENT '员工住址',
    emergency_contact STRING COMMENT '紧急联系人',
    hire_date DATE COMMENT '入职日期',
    position STRING COMMENT '职位',
    department STRING COMMENT '部门',
    employment_status STRING COMMENT '雇佣状态',
    work_status STRING COMMENT '工作状态',
    probation_period_end DATE COMMENT '试用期结束日期',
    termination_date DATE COMMENT '离职日期',
    resignation_reason STRING COMMENT '离职原因',
    created_at TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP COMMENT '最后更新时间',
    last_login TIMESTAMP COMMENT '最后登录时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'ODS层员工表'
PARTITIONED BY (dt STRING COMMENT '数据日期分区，格式：yyyyMMdd')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE;

-- 创建ODS层产品表
CREATE TABLE IF NOT EXISTS ods_products (
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
    currency STRING COMMENT '货币类型',
    status STRING COMMENT '产品状态',
    launch_date DATE COMMENT '上架日期',
    discontinued_date DATE COMMENT '停产日期',
    supplier_id INT COMMENT '供应商ID',
    manufacturer STRING COMMENT '制造商名称',
    country_of_origin STRING COMMENT '生产国家',
    image_url STRING COMMENT '主图URL',
    additional_images STRING COMMENT '其他图片URL',
    weight DECIMAL(10,2) COMMENT '产品重量',
    dimensions STRING COMMENT '产品尺寸',
    warranty STRING COMMENT '保修期',
    created_at TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP COMMENT '最后更新时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'ODS层产品表'
PARTITIONED BY (dt STRING COMMENT '数据日期分区，格式：yyyyMMdd')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE;

-- 创建ODS层采购订单表
CREATE TABLE IF NOT EXISTS ods_purchase_orders (
    purchase_order_id INT COMMENT '采购订单ID',
    supplier_id INT COMMENT '供应商ID',
    order_date TIMESTAMP COMMENT '下单日期',
    expected_delivery_date TIMESTAMP COMMENT '预计交货日期',
    actual_delivery_date TIMESTAMP COMMENT '实际交货日期',
    status STRING COMMENT '订单状态',
    total_amount DECIMAL(12,2) COMMENT '订单总金额',
    currency STRING COMMENT '货币类型',
    payment_status STRING COMMENT '支付状态',
    payment_method STRING COMMENT '支付方式',
    payment_date TIMESTAMP COMMENT '支付日期',
    shipping_cost DECIMAL(10,2) COMMENT '配送费用',
    warehouse_location STRING COMMENT '存放仓库位置',
    created_by INT COMMENT '创建者ID',
    approved_by INT COMMENT '审批人ID',
    note STRING COMMENT '备注',
    created_at TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP COMMENT '最后更新时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'ODS层采购订单表'
PARTITIONED BY (dt STRING COMMENT '数据日期分区，格式：yyyyMMdd')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE;

-- 创建ODS层采购订单明细表
CREATE TABLE IF NOT EXISTS ods_purchase_order_items (
    purchase_order_item_id INT COMMENT '采购订单项ID',
    purchase_order_id INT COMMENT '采购订单ID',
    product_id INT COMMENT '产品ID',
    quantity INT COMMENT '数量',
    unit_price DECIMAL(10,2) COMMENT '单价',
    total_price DECIMAL(12,2) COMMENT '总价',
    received_quantity INT COMMENT '已接收数量',
    status STRING COMMENT '项目状态',
    expected_delivery_date DATE COMMENT '预计交货日期',
    actual_delivery_date DATE COMMENT '实际交货日期',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'ODS层采购订单明细表'
PARTITIONED BY (dt STRING COMMENT '数据日期分区，格式：yyyyMMdd')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE;

-- 创建ODS层销售订单表
CREATE TABLE IF NOT EXISTS ods_sales_orders (
    sales_order_id INT COMMENT '销售订单ID',
    user_id INT COMMENT '客户ID',
    order_date TIMESTAMP COMMENT '下单日期',
    expected_delivery_date TIMESTAMP COMMENT '预计交货日期',
    actual_delivery_date TIMESTAMP COMMENT '实际交货日期',
    shipping_method STRING COMMENT '配送方式',
    shipping_cost DECIMAL(10,2) COMMENT '配送费用',
    shipping_address STRING COMMENT '收货地址',
    billing_address STRING COMMENT '账单地址',
    status STRING COMMENT '订单状态',
    total_amount DECIMAL(12,2) COMMENT '订单总金额',
    currency STRING COMMENT '货币类型',
    discount DECIMAL(10,2) COMMENT '订单总折扣',
    final_amount DECIMAL(12,2) COMMENT '最终应支付金额',
    payment_status STRING COMMENT '支付状态',
    payment_method STRING COMMENT '支付方式',
    payment_date TIMESTAMP COMMENT '支付日期',
    tracking_number STRING COMMENT '快递单号',
    created_by INT COMMENT '创建者ID',
    note STRING COMMENT '备注',
    referral_code STRING COMMENT '推荐码',
    created_at TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP COMMENT '最后更新时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'ODS层销售订单表'
PARTITIONED BY (dt STRING COMMENT '数据日期分区，格式：yyyyMMdd')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE;

-- 创建ODS层销售订单明细表
CREATE TABLE IF NOT EXISTS ods_sales_order_items (
    sales_order_item_id INT COMMENT '销售订单项ID',
    sales_order_id INT COMMENT '销售订单ID',
    product_id INT COMMENT '产品ID',
    quantity INT COMMENT '数量',
    unit_price DECIMAL(10,2) COMMENT '单价',
    total_price DECIMAL(12,2) COMMENT '总价',
    discount DECIMAL(10,2) COMMENT '折扣金额',
    final_price DECIMAL(12,2) COMMENT '最终价格',
    expected_delivery_date TIMESTAMP COMMENT '预计交货日期',
    actual_delivery_date TIMESTAMP COMMENT '实际交货日期',
    status STRING COMMENT '项目状态',
    warehouse_location STRING COMMENT '发货仓库位置',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'ODS层销售订单明细表'
PARTITIONED BY (dt STRING COMMENT '数据日期分区，格式：yyyyMMdd')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE; 