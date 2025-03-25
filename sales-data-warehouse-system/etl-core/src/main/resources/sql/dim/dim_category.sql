-- 类目维度表
CREATE TABLE IF NOT EXISTS dim.dim_category (
    category_id BIGINT COMMENT '类目ID',
    category_name STRING COMMENT '类目名称',
    parent_id BIGINT COMMENT '父类目ID',
    level INT COMMENT '层级',
    path STRING COMMENT '类目路径',
    sort_order INT COMMENT '排序序号',
    is_visible BOOLEAN COMMENT '是否可见',
    icon_url STRING COMMENT '图标URL',
    description STRING COMMENT '类目描述',
    total_products INT COMMENT '商品总数',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    start_date DATE COMMENT '生效开始日期',
    end_date DATE COMMENT '生效结束日期',
    is_current BOOLEAN COMMENT '是否当前版本',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
) 
COMMENT '类目维度表'
STORED AS PARQUET; 