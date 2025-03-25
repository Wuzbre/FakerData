-- 积分记录表
CREATE TABLE IF NOT EXISTS ods.ods_points_record (
    record_id BIGINT COMMENT '记录ID',
    user_id BIGINT COMMENT '用户ID',
    order_id BIGINT COMMENT '订单ID',
    points_type STRING COMMENT '积分类型',
    points_value INT COMMENT '积分值',
    before_points INT COMMENT '变更前积分',
    after_points INT COMMENT '变更后积分',
    expire_time TIMESTAMP COMMENT '过期时间',
    status STRING COMMENT '状态',
    description STRING COMMENT '描述',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
) 
COMMENT '积分记录表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET; 