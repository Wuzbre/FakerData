-- 用户行为事实表
CREATE TABLE IF NOT EXISTS dwd.dwd_fact_user_behavior (
    behavior_id BIGINT COMMENT '行为ID',
    user_id BIGINT COMMENT '用户ID',
    session_id STRING COMMENT '会话ID',
    product_id BIGINT COMMENT '商品ID',
    category_id BIGINT COMMENT '类目ID',
    brand_id BIGINT COMMENT '品牌ID',
    behavior_type STRING COMMENT '行为类型(浏览、收藏、加购物车、购买等)',
    behavior_time TIMESTAMP COMMENT '行为时间',
    stay_time INT COMMENT '停留时间(秒)',
    source_type STRING COMMENT '来源类型',
    source_page STRING COMMENT '来源页面',
    device_type STRING COMMENT '设备类型',
    os_type STRING COMMENT '操作系统',
    browser_type STRING COMMENT '浏览器类型',
    ip_address STRING COMMENT 'IP地址',
    province STRING COMMENT '省份',
    city STRING COMMENT '城市',
    is_new_user BOOLEAN COMMENT '是否新用户',
    create_time TIMESTAMP COMMENT '创建时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
) 
COMMENT '用户行为事实表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET; 