-- 商品维度表
CREATE TABLE IF NOT EXISTS dim.dim_product (
    product_id BIGINT COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_id BIGINT COMMENT '类目ID',
    category_name STRING COMMENT '类目名称',
    category_level INT COMMENT '类目层级',
    brand_id BIGINT COMMENT '品牌ID',
    brand_name STRING COMMENT '品牌名称',
    price DECIMAL(16,2) COMMENT '当前售价',
    market_price DECIMAL(16,2) COMMENT '市场价',
    status STRING COMMENT '商品状态',
    description STRING COMMENT '商品描述',
    main_image STRING COMMENT '主图URL',
    attribute_list STRING COMMENT '属性列表',
    is_featured BOOLEAN COMMENT '是否推荐',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    start_date DATE COMMENT '生效开始日期',
    end_date DATE COMMENT '生效结束日期',
    is_current BOOLEAN COMMENT '是否当前版本',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
) 
COMMENT '商品维度表'
STORED AS PARQUET; 