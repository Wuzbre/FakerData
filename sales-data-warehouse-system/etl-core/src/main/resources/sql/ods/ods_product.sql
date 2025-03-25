-- 商品表
CREATE TABLE IF NOT EXISTS ods.ods_product (
    product_id BIGINT COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_id BIGINT COMMENT '类目ID',
    brand_id BIGINT COMMENT '品牌ID',
    shop_id BIGINT COMMENT '店铺ID',
    price DECIMAL(16,2) COMMENT '售价',
    market_price DECIMAL(16,2) COMMENT '市场价',
    cost_price DECIMAL(16,2) COMMENT '成本价',
    stock INT COMMENT '库存',
    sales INT COMMENT '销量',
    status STRING COMMENT '商品状态',
    description STRING COMMENT '商品描述',
    main_image STRING COMMENT '主图URL',
    attribute_list STRING COMMENT '属性列表',
    is_featured BOOLEAN COMMENT '是否推荐',
    is_new BOOLEAN COMMENT '是否新品',
    is_hot BOOLEAN COMMENT '是否热销',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
) 
COMMENT '商品表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET; 