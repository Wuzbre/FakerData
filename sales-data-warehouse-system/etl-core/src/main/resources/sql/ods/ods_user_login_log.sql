-- 用户登录日志表
CREATE TABLE IF NOT EXISTS ods.ods_user_login_log (
    log_id BIGINT COMMENT '日志ID',
    user_id BIGINT COMMENT '用户ID',
    login_time TIMESTAMP COMMENT '登录时间',
    logout_time TIMESTAMP COMMENT '登出时间',
    ip_address STRING COMMENT 'IP地址',
    device_type STRING COMMENT '设备类型',
    os_type STRING COMMENT '操作系统',
    browser_type STRING COMMENT '浏览器类型',
    session_id STRING COMMENT '会话ID',
    login_status STRING COMMENT '登录状态',
    login_source STRING COMMENT '登录来源',
    province STRING COMMENT '省份',
    city STRING COMMENT '城市',
    create_time TIMESTAMP COMMENT '创建时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
) 
COMMENT '用户登录日志表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET; 