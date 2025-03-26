-- 创建数据库
CREATE DATABASE IF NOT EXISTS dim;
CREATE DATABASE IF NOT EXISTS dwd;
CREATE DATABASE IF NOT EXISTS dws;

-- 维度层（DIM）表
-- 用户维度表
CREATE TABLE IF NOT EXISTS dim.dim_user (
    user_id STRING COMMENT '用户ID',
    username STRING COMMENT '用户名',
    email STRING COMMENT '邮箱',
    phone STRING COMMENT '电话',
    address STRING COMMENT '地址',
    region STRING COMMENT '地区',
    city STRING COMMENT '城市',
    user_level STRING COMMENT '用户等级',
    register_time TIMESTAMP COMMENT '注册时间',
    last_login_time TIMESTAMP COMMENT '最后登录时间',
    total_orders INT COMMENT '总订单数',
    total_amount DECIMAL(10,2) COMMENT '总消费金额',
    promotion_sensitivity DOUBLE COMMENT '促销敏感度',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
) 
COMMENT '用户维度表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 商品维度表
CREATE TABLE IF NOT EXISTS dim.dim_product (
    product_id STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_id STRING COMMENT '类目ID',
    category_name STRING COMMENT '类目名称',
    supplier_id STRING COMMENT '供应商ID',
    supplier_name STRING COMMENT '供应商名称',
    price DECIMAL(10,2) COMMENT '价格',
    unit STRING COMMENT '单位',
    description STRING COMMENT '描述',
    popularity_level STRING COMMENT '热度等级',
    is_seasonal BOOLEAN COMMENT '是否季节性商品',
    season_type STRING COMMENT '季节类型',
    min_stock INT COMMENT '最小库存',
    max_stock INT COMMENT '最大库存',
    reorder_point INT COMMENT '再订货点',
    safety_stock INT COMMENT '安全库存',
    turnover_days INT COMMENT '周转天数',
    turnover_rate DOUBLE COMMENT '周转率',
    sales_volume INT COMMENT '销量',
    sales_amount DECIMAL(10,2) COMMENT '销售额',
    avg_rating DOUBLE COMMENT '平均评分',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
)
COMMENT '商品维度表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 供应商维度表
CREATE TABLE IF NOT EXISTS dim.dim_supplier (
    supplier_id STRING COMMENT '供应商ID',
    supplier_name STRING COMMENT '供应商名称',
    contact_name STRING COMMENT '联系人姓名',
    contact_phone STRING COMMENT '联系电话',
    email STRING COMMENT '邮箱',
    address STRING COMMENT '地址',
    region STRING COMMENT '地区',
    city STRING COMMENT '城市',
    supply_capacity INT COMMENT '供应能力',
    cooperation_start_date DATE COMMENT '合作开始日期',
    credit_score INT COMMENT '信用评分',
    supply_stability DOUBLE COMMENT '供应稳定性',
    avg_delivery_days INT COMMENT '平均供货天数',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
)
COMMENT '供应商维度表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 类目维度表
CREATE TABLE IF NOT EXISTS dim.dim_category (
    category_id STRING COMMENT '类目ID',
    category_name STRING COMMENT '类目名称',
    parent_id STRING COMMENT '父类目ID',
    category_level INT COMMENT '类目层级',
    sort_order INT COMMENT '排序序号',
    is_seasonal BOOLEAN COMMENT '是否季节性类目',
    season_type STRING COMMENT '季节类型',
    sales_volume INT COMMENT '销量',
    sales_amount DECIMAL(10,2) COMMENT '销售额',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
)
COMMENT '类目维度表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 明细层（DWD）表
-- 订单事实表
CREATE TABLE IF NOT EXISTS dwd.dwd_fact_order (
    order_id STRING COMMENT '订单ID',
    user_id STRING COMMENT '用户ID',
    order_status INT COMMENT '订单状态',
    total_amount DECIMAL(10,2) COMMENT '订单总金额',
    actual_amount DECIMAL(10,2) COMMENT '实际支付金额',
    payment_method INT COMMENT '支付方式',
    shipping_address STRING COMMENT '收货地址',
    shipping_phone STRING COMMENT '收货电话',
    shipping_name STRING COMMENT '收货人姓名',
    region STRING COMMENT '地区',
    city STRING COMMENT '城市',
    order_source STRING COMMENT '订单来源',
    promotion_id STRING COMMENT '促销ID',
    promotion_discount DECIMAL(10,2) COMMENT '促销折扣金额',
    weather STRING COMMENT '下单时天气',
    delivery_days INT COMMENT '配送天数',
    is_first_order BOOLEAN COMMENT '是否首单',
    created_at TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP COMMENT '更新时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
)
COMMENT '订单事实表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 订单明细事实表
CREATE TABLE IF NOT EXISTS dwd.dwd_fact_order_detail (
    detail_id STRING COMMENT '明细ID',
    order_id STRING COMMENT '订单ID',
    product_id STRING COMMENT '商品ID',
    quantity INT COMMENT '数量',
    unit_price DECIMAL(10,2) COMMENT '单价',
    original_price DECIMAL(10,2) COMMENT '原价',
    total_price DECIMAL(10,2) COMMENT '总价',
    discount_amount DECIMAL(10,2) COMMENT '折扣金额',
    is_seasonal_price BOOLEAN COMMENT '是否季节性价格',
    is_promotion_price BOOLEAN COMMENT '是否促销价格',
    rating DOUBLE COMMENT '评分',
    review STRING COMMENT '评价内容',
    created_at TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP COMMENT '更新时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
)
COMMENT '订单明细事实表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 库存变动事实表
CREATE TABLE IF NOT EXISTS dwd.dwd_fact_inventory_change (
    log_id STRING COMMENT '日志ID',
    product_id STRING COMMENT '商品ID',
    change_type INT COMMENT '变动类型',
    change_quantity INT COMMENT '变动数量',
    before_quantity INT COMMENT '变动前数量',
    after_quantity INT COMMENT '变动后数量',
    related_order_id STRING COMMENT '关联订单ID',
    operator STRING COMMENT '操作人',
    remark STRING COMMENT '备注',
    created_at TIMESTAMP COMMENT '创建时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
)
COMMENT '库存变动事实表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 价格变动事实表
CREATE TABLE IF NOT EXISTS dwd.dwd_fact_price_change (
    history_id STRING COMMENT '历史记录ID',
    product_id STRING COMMENT '商品ID',
    old_price DECIMAL(10,2) COMMENT '原价',
    new_price DECIMAL(10,2) COMMENT '新价',
    change_reason STRING COMMENT '变动原因',
    operator STRING COMMENT '操作人',
    created_at TIMESTAMP COMMENT '创建时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
)
COMMENT '价格变动事实表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 促销事实表
CREATE TABLE IF NOT EXISTS dwd.dwd_fact_promotion (
    promotion_id STRING COMMENT '促销ID',
    promotion_name STRING COMMENT '促销名称',
    promotion_type STRING COMMENT '促销类型',
    start_time TIMESTAMP COMMENT '开始时间',
    end_time TIMESTAMP COMMENT '结束时间',
    discount_rate DOUBLE COMMENT '折扣率',
    status INT COMMENT '状态',
    participant_count INT COMMENT '参与人数',
    order_count INT COMMENT '订单数',
    total_discount_amount DECIMAL(10,2) COMMENT '总折扣金额',
    created_at TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP COMMENT '更新时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
)
COMMENT '促销事实表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 汇总层（DWS）表
-- 用户行为汇总表
CREATE TABLE IF NOT EXISTS dws.dws_user_behavior (
    user_id STRING COMMENT '用户ID',
    user_level STRING COMMENT '用户等级',
    order_count INT COMMENT '订单数',
    total_amount DECIMAL(10,2) COMMENT '总金额',
    avg_order_amount DECIMAL(10,2) COMMENT '平均订单金额',
    promotion_order_count INT COMMENT '促销订单数',
    total_promotion_discount DECIMAL(10,2) COMMENT '总促销折扣',
    purchased_product_count INT COMMENT '购买商品种类数',
    dt STRING COMMENT '分区字段'
)
COMMENT '用户行为汇总表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 商品销售汇总表
CREATE TABLE IF NOT EXISTS dws.dws_product_sales (
    product_id STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_id STRING COMMENT '类目ID',
    category_name STRING COMMENT '类目名称',
    order_count INT COMMENT '订单数',
    total_quantity INT COMMENT '总数量',
    total_amount DECIMAL(10,2) COMMENT '总金额',
    avg_price DECIMAL(10,2) COMMENT '平均价格',
    promotion_quantity INT COMMENT '促销数量',
    promotion_amount DECIMAL(10,2) COMMENT '促销金额',
    avg_rating DOUBLE COMMENT '平均评分',
    dt STRING COMMENT '分区字段'
)
COMMENT '商品销售汇总表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 供应商表现汇总表
CREATE TABLE IF NOT EXISTS dws.dws_supplier_performance (
    supplier_id STRING COMMENT '供应商ID',
    supplier_name STRING COMMENT '供应商名称',
    product_count INT COMMENT '商品数量',
    total_supply_quantity INT COMMENT '总供应量',
    total_sales_amount DECIMAL(10,2) COMMENT '总销售额',
    avg_delivery_days DOUBLE COMMENT '平均供货天数',
    on_time_delivery_rate DOUBLE COMMENT '准时交付率',
    quality_issue_rate DOUBLE COMMENT '质量问题率',
    dt STRING COMMENT '分区字段'
)
COMMENT '供应商表现汇总表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 促销效果汇总表
CREATE TABLE IF NOT EXISTS dws.dws_promotion_effect (
    promotion_id STRING COMMENT '促销ID',
    promotion_name STRING COMMENT '促销名称',
    promotion_type STRING COMMENT '促销类型',
    affected_product_count INT COMMENT '影响商品数',
    order_count INT COMMENT '订单数',
    customer_count INT COMMENT '参与客户数',
    total_discount_amount DECIMAL(10,2) COMMENT '总折扣金额',
    total_sales_amount DECIMAL(10,2) COMMENT '总销售额',
    promotion_roi DOUBLE COMMENT '促销ROI',
    dt STRING COMMENT '分区字段'
)
COMMENT '促销效果汇总表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 库存周转汇总表
CREATE TABLE IF NOT EXISTS dws.dws_inventory_turnover (
    product_id STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_id STRING COMMENT '类目ID',
    category_name STRING COMMENT '类目名称',
    avg_daily_sales INT COMMENT '日均销量',
    current_stock INT COMMENT '当前库存',
    turnover_days DOUBLE COMMENT '周转天数',
    turnover_rate DOUBLE COMMENT '周转率',
    stock_status STRING COMMENT '库存状态',
    reorder_times INT COMMENT '补货次数',
    dt STRING COMMENT '分区字段'
)
COMMENT '库存周转汇总表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET; 