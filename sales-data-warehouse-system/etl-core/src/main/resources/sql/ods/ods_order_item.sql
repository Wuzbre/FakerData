-- 订单明细表
CREATE TABLE IF NOT EXISTS ods.ods_order_item (
    item_id BIGINT COMMENT '订单项ID',
    order_id BIGINT COMMENT '订单ID',
    product_id BIGINT COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    product_image STRING COMMENT '商品图片',
    product_price DECIMAL(16,2) COMMENT '商品单价',
    purchase_quantity INT COMMENT '购买数量',
    total_amount DECIMAL(16,2) COMMENT '总金额',
    discount_amount DECIMAL(16,2) COMMENT '优惠金额',
    coupon_amount DECIMAL(16,2) COMMENT '优惠券金额',
    points_amount DECIMAL(16,2) COMMENT '积分抵扣金额',
    actual_amount DECIMAL(16,2) COMMENT '实付金额',
    is_reviewed BOOLEAN COMMENT '是否已评价',
    review_time TIMESTAMP COMMENT '评价时间',
    review_content STRING COMMENT '评价内容',
    review_rating INT COMMENT '评价星级',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间',
    dt STRING COMMENT '分区字段'
) 
COMMENT '订单明细表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET; 