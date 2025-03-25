-- 品牌维度表
CREATE TABLE IF NOT EXISTS dim.dim_brand (
    brand_id BIGINT COMMENT '品牌ID',
    brand_name STRING COMMENT '品牌名称',
    logo_url STRING COMMENT '品牌LOGO URL',
    description STRING COMMENT '品牌描述',
    website STRING COMMENT '品牌官网',
    status STRING COMMENT '状态',
    sort_order INT COMMENT '排序序号',
    is_featured BOOLEAN COMMENT '是否推荐',
    total_products INT COMMENT '商品总数',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    start_date DATE COMMENT '生效开始日期',
    end_date DATE COMMENT '生效结束日期',
    is_current BOOLEAN COMMENT '是否当前版本',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
) 
COMMENT '品牌维度表'
STORED AS PARQUET; 