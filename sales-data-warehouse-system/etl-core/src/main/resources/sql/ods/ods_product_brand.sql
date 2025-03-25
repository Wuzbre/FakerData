-- 商品品牌表
CREATE TABLE IF NOT EXISTS ods.ods_product_brand (
    brand_id BIGINT COMMENT '品牌ID',
    brand_name STRING COMMENT '品牌名称',
    logo_url STRING COMMENT '品牌LOGO URL',
    description STRING COMMENT '品牌描述',
    website STRING COMMENT '品牌官网',
    status STRING COMMENT '状态',
    sort_order INT COMMENT '排序序号',
    is_featured BOOLEAN COMMENT '是否推荐',
    keywords STRING COMMENT '搜索关键词',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
) 
COMMENT '商品品牌表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET; 