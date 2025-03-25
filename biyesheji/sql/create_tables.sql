-- 创建数据库
CREATE DATABASE IF NOT EXISTS sales_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE sales_db;

-- 用户信息表
CREATE TABLE IF NOT EXISTS user (
    user_id VARCHAR(32) PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    password VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    phone VARCHAR(20),
    address TEXT,
    region VARCHAR(50) COMMENT '所属区域',
    city VARCHAR(50) COMMENT '所在城市',
    user_level VARCHAR(20) COMMENT '用户等级：frequent-高频，regular-普通，occasional-低频',
    register_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_login_time DATETIME COMMENT '最后登录时间',
    total_orders INT DEFAULT 0 COMMENT '总订单数',
    total_amount DECIMAL(12,2) DEFAULT 0 COMMENT '总消费金额',
    promotion_sensitivity DECIMAL(3,2) COMMENT '促销敏感度',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户信息表';

-- 商品类别表
CREATE TABLE IF NOT EXISTS category (
    category_id VARCHAR(32) PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL,
    parent_id VARCHAR(32),
    category_level INT NOT NULL COMMENT '分类层级',
    sort_order INT DEFAULT 0 COMMENT '排序号',
    is_seasonal BOOLEAN DEFAULT FALSE COMMENT '是否季节性类别',
    season_type VARCHAR(20) COMMENT '适用季节：spring-春季，summer-夏季，autumn-秋季，winter-冬季',
    sales_volume INT DEFAULT 0 COMMENT '销售量',
    sales_amount DECIMAL(12,2) DEFAULT 0 COMMENT '销售金额',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品类别表';

-- 供应商信息表
CREATE TABLE IF NOT EXISTS supplier (
    supplier_id VARCHAR(32) PRIMARY KEY,
    supplier_name VARCHAR(100) NOT NULL,
    contact_name VARCHAR(50),
    contact_phone VARCHAR(20),
    email VARCHAR(100),
    address TEXT,
    region VARCHAR(50) COMMENT '所属区域',
    city VARCHAR(50) COMMENT '所在城市',
    supply_capacity INT COMMENT '供应能力',
    cooperation_start_date DATE COMMENT '合作开始日期',
    credit_score INT DEFAULT 100 COMMENT '信用评分',
    supply_stability DECIMAL(3,2) DEFAULT 1.00 COMMENT '供应稳定性',
    avg_delivery_days INT COMMENT '平均供货天数',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='供应商信息表';

-- 商品信息表
CREATE TABLE IF NOT EXISTS product (
    product_id VARCHAR(32) PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category_id VARCHAR(32) NOT NULL,
    supplier_id VARCHAR(32) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    stock INT NOT NULL DEFAULT 0,
    unit VARCHAR(20) COMMENT '单位',
    description TEXT,
    popularity_level VARCHAR(20) COMMENT '热度等级：hot-热门，normal-普通，cold-冷门',
    is_seasonal BOOLEAN DEFAULT FALSE COMMENT '是否季节性商品',
    season_type VARCHAR(20) COMMENT '适用季节',
    min_stock INT COMMENT '最小库存',
    max_stock INT COMMENT '最大库存',
    reorder_point INT COMMENT '补货点',
    safety_stock INT COMMENT '安全库存',
    turnover_days INT COMMENT '周转天数',
    turnover_rate DECIMAL(5,2) COMMENT '周转率',
    sales_volume INT DEFAULT 0 COMMENT '销量',
    sales_amount DECIMAL(12,2) DEFAULT 0 COMMENT '销售额',
    avg_rating DECIMAL(2,1) DEFAULT 5.0 COMMENT '平均评分',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES category(category_id),
    FOREIGN KEY (supplier_id) REFERENCES supplier(supplier_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品信息表';

-- 订单主表
CREATE TABLE IF NOT EXISTS `order` (
    order_id VARCHAR(32) PRIMARY KEY,
    user_id VARCHAR(32) NOT NULL,
    order_status TINYINT NOT NULL COMMENT '订单状态：0-待付款，1-已付款，2-已发货，3-已完成，4-已取消',
    total_amount DECIMAL(10,2) NOT NULL,
    actual_amount DECIMAL(10,2) NOT NULL COMMENT '实际支付金额',
    payment_method TINYINT COMMENT '支付方式：1-支付宝，2-微信，3-银行卡',
    shipping_address TEXT,
    shipping_phone VARCHAR(20),
    shipping_name VARCHAR(50),
    region VARCHAR(50) COMMENT '收货区域',
    city VARCHAR(50) COMMENT '收货城市',
    order_source VARCHAR(20) COMMENT '订单来源：normal-普通购买，holiday-节假日购买，promotion-活动购买',
    promotion_id VARCHAR(50) COMMENT '关联活动ID',
    promotion_discount DECIMAL(10,2) COMMENT '活动优惠金额',
    weather VARCHAR(20) COMMENT '下单时天气',
    delivery_days INT COMMENT '配送天数',
    is_first_order BOOLEAN DEFAULT FALSE COMMENT '是否首单',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES user(user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单主表';

-- 订单详情表
CREATE TABLE IF NOT EXISTS order_detail (
    detail_id VARCHAR(32) PRIMARY KEY,
    order_id VARCHAR(32) NOT NULL,
    product_id VARCHAR(32) NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    original_price DECIMAL(10,2) NOT NULL COMMENT '原始单价',
    total_price DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2) DEFAULT 0 COMMENT '优惠金额',
    is_seasonal_price BOOLEAN DEFAULT FALSE COMMENT '是否季节性定价',
    is_promotion_price BOOLEAN DEFAULT FALSE COMMENT '是否促销定价',
    rating DECIMAL(2,1) COMMENT '商品评分',
    review TEXT COMMENT '商品评价',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES `order`(order_id),
    FOREIGN KEY (product_id) REFERENCES product(product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单详情表';

-- 活动表
CREATE TABLE IF NOT EXISTS promotion (
    promotion_id VARCHAR(32) PRIMARY KEY,
    promotion_name VARCHAR(100) NOT NULL,
    promotion_type VARCHAR(50) COMMENT '活动类型：holiday-节假日，season-季节性，festival-购物节',
    start_time DATETIME NOT NULL,
    end_time DATETIME NOT NULL,
    discount_rate DECIMAL(3,2) COMMENT '折扣率',
    status TINYINT DEFAULT 1 COMMENT '活动状态：0-未开始，1-进行中，2-已结束',
    participant_count INT DEFAULT 0 COMMENT '参与人数',
    order_count INT DEFAULT 0 COMMENT '订单数',
    total_discount_amount DECIMAL(12,2) DEFAULT 0 COMMENT '总优惠金额',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='活动表';

-- 库存变动记录表
CREATE TABLE IF NOT EXISTS inventory_log (
    log_id VARCHAR(32) PRIMARY KEY,
    product_id VARCHAR(32) NOT NULL,
    change_type TINYINT COMMENT '变动类型：1-入库，2-出库，3-退货入库，4-报损',
    change_quantity INT NOT NULL COMMENT '变动数量',
    before_quantity INT NOT NULL COMMENT '变动前数量',
    after_quantity INT NOT NULL COMMENT '变动后数量',
    related_order_id VARCHAR(32) COMMENT '关联订单ID',
    operator VARCHAR(50) COMMENT '操作人',
    remark TEXT COMMENT '备注',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES product(product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='库存变动记录表';

-- 商品价格历史表
CREATE TABLE IF NOT EXISTS price_history (
    history_id VARCHAR(32) PRIMARY KEY,
    product_id VARCHAR(32) NOT NULL,
    old_price DECIMAL(10,2) NOT NULL,
    new_price DECIMAL(10,2) NOT NULL,
    change_reason VARCHAR(50) COMMENT '变更原因：seasonal-季节性调整，promotion-促销调整，cost-成本调整',
    operator VARCHAR(50) COMMENT '操作人',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES product(product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品价格历史表'; 