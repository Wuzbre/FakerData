-- 用户表
CREATE TABLE IF NOT EXISTS ods.ods_user (
    user_id BIGINT COMMENT '用户ID',
    user_name STRING COMMENT '用户名',
    password STRING COMMENT '密码',
    phone STRING COMMENT '手机号',
    email STRING COMMENT '邮箱',
    gender STRING COMMENT '性别',
    birth_date DATE COMMENT '出生日期',
    registration_time TIMESTAMP COMMENT '注册时间',
    member_level STRING COMMENT '会员等级',
    is_active BOOLEAN COMMENT '是否活跃',
    status STRING COMMENT '用户状态',
    avatar_url STRING COMMENT '头像URL',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
) 
COMMENT '用户表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET; 