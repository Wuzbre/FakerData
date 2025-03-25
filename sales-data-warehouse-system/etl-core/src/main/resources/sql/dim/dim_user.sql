-- 用户维度表
CREATE TABLE IF NOT EXISTS dim.dim_user (
    user_id BIGINT COMMENT '用户ID',
    user_name STRING COMMENT '用户名',
    phone STRING COMMENT '手机号',
    email STRING COMMENT '邮箱',
    gender STRING COMMENT '性别',
    birth_date DATE COMMENT '出生日期',
    registration_time TIMESTAMP COMMENT '注册时间',
    member_level STRING COMMENT '会员等级',
    is_active BOOLEAN COMMENT '是否活跃',
    total_points BIGINT COMMENT '总积分',
    available_points BIGINT COMMENT '可用积分',
    last_login_time TIMESTAMP COMMENT '最后登录时间',
    last_order_time TIMESTAMP COMMENT '最后下单时间',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    start_date DATE COMMENT '生效开始日期',
    end_date DATE COMMENT '生效结束日期',
    is_current BOOLEAN COMMENT '是否当前版本',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
) 
COMMENT '用户维度表'
STORED AS PARQUET; 