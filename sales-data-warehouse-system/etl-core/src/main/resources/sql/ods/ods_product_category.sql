-- 商品类目表
CREATE TABLE IF NOT EXISTS ods.ods_product_category (
    category_id BIGINT COMMENT '类目ID',
    category_name STRING COMMENT '类目名称',
    parent_id BIGINT COMMENT '父类目ID',
    level INT COMMENT '层级',
    path STRING COMMENT '类目路径',
    sort_order INT COMMENT '排序序号',
    is_visible BOOLEAN COMMENT '是否可见',
    icon_url STRING COMMENT '图标URL',
    description STRING COMMENT '类目描述',
    keywords STRING COMMENT '搜索关键词',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
) 
COMMENT '商品类目表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET; 