"""数据配置文件"""

BRANDS = [
    "华为", "小米", "苹果", "三星", "荣耀", "OPPO", "vivo",  # 电子品牌
    "耐克", "阿迪达斯", "彪马", "安踏", "李宁", "New Balance",  # 运动品牌
    # ... 其他品牌
]

DESCRIPTORS = [
    "Pro", "X", "Max", "Air", "Ultra", "Lite",  # 高端技术词汇
    "时尚", "经典", "奢华", "舒适", "流行", "运动", "高性能",  # 服饰、鞋类
    # ... 其他描述词
]

CATEGORIES = [
    "智能手机", "笔记本电脑", "无线耳机", "智能手表", "智能音响", "4K电视",  # 电子产品
    "运动鞋", "跑步鞋", "篮球鞋", "休闲鞋", "帆布鞋", "拖鞋",  # 鞋类
    # ... 其他类别
]

# 数据生成配置
DATA_GENERATION = {
    'base_rates': {
        'user': 500,
        'employee': 1,
        'product': 40,
        'purchase_order': 300,
        'sales_order': 600
    },
    'holiday_rate': 0.5  # 节假日数据生成比率
}

# 数据验证规则
VALIDATION_RULES = {
    'user': {
        'email': r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$',
        'phone': r'^1[3-9]\d{9}$',
        'password_min_length': 8
    },
    'product': {
        'price_min': 0.01,
        'price_max': 1000000
    }
}

# 数据库连接配置
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'password',
    'database': 'sales_db',
    'port': 3306
}

# 默认生成数据量
DEFAULT_USERS = 1000
DEFAULT_PRODUCTS = 200
DEFAULT_ORDERS = 10000
DEFAULT_DAYS = 365  # 默认生成一年的数据

# 产品类别及其对应的权重（影响产品分布）
PRODUCT_CATEGORIES = {
    '电子产品': 0.30,
    '服装': 0.25,
    '家居': 0.20,
    '食品': 0.15,
    '图书': 0.10
}

# 电子产品子类别
ELECTRONICS_SUBCATEGORIES = {
    '手机': 0.30,
    '电脑': 0.25,
    '平板': 0.15,
    '相机': 0.10,
    '耳机': 0.10,
    '智能手表': 0.05,
    '音响': 0.05
}

# 服装子类别
CLOTHING_SUBCATEGORIES = {
    '上衣': 0.30,
    '裤子': 0.25,
    '裙子': 0.15,
    '鞋子': 0.20,
    '配饰': 0.10
}

# 家居子类别
HOME_SUBCATEGORIES = {
    '家具': 0.30,
    '厨具': 0.25,
    '床上用品': 0.20,
    '装饰品': 0.15,
    '灯具': 0.10
}

# 食品子类别
FOOD_SUBCATEGORIES = {
    '零食': 0.30,
    '饮料': 0.25,
    '乳制品': 0.15,
    '粮油': 0.15,
    '调味品': 0.15
}

# 图书子类别
BOOK_SUBCATEGORIES = {
    '小说': 0.30,
    '教育': 0.25,
    '科技': 0.20,
    '艺术': 0.15,
    '杂志': 0.10
}

# 子类别到主类别的映射
SUBCATEGORY_TO_CATEGORY = {}
for cat, subcat_dict in [
    ('电子产品', ELECTRONICS_SUBCATEGORIES),
    ('服装', CLOTHING_SUBCATEGORIES),
    ('家居', HOME_SUBCATEGORIES),
    ('食品', FOOD_SUBCATEGORIES),
    ('图书', BOOK_SUBCATEGORIES)
]:
    for subcat in subcat_dict:
        SUBCATEGORY_TO_CATEGORY[subcat] = cat

# 区域销售分布（影响用户地址和订单分布）
REGIONS = {
    '华东': ['上海', '江苏', '浙江', '安徽', '福建', '江西', '山东'],
    '华北': ['北京', '天津', '河北', '山西', '内蒙古'],
    '华南': ['广东', '广西', '海南'],
    '华中': ['河南', '湖北', '湖南'],
    '西南': ['重庆', '四川', '贵州', '云南', '西藏'],
    '西北': ['陕西', '甘肃', '青海', '宁夏', '新疆'],
    '东北': ['辽宁', '吉林', '黑龙江']
}

# 区域销售比重
REGION_DISTRIBUTION = {
    '华东': 0.35,
    '华北': 0.25,
    '华南': 0.20,
    '华中': 0.08,
    '西南': 0.05,
    '西北': 0.03,
    '东北': 0.04
}

# 订单状态转换概率
ORDER_STATUS = {
    '已下单': 1.0,      # 所有订单初始状态都是"已下单"
    '已支付': 0.95,     # 95%的订单会变成"已支付"
    '已发货': 0.90,     # 90%的已支付订单会变成"已发货"
    '已完成': 0.85,     # 85%的已发货订单会变成"已完成"
    '已取消': 0.05,     # 5%的订单会被取消
    '已退货': 0.03      # 3%的已完成订单会被退货
}

# 季节性销售因子（月份索引，从0开始代表1月）
SEASONAL_FACTORS = {
    0: 0.8,   # 1月 - 春节前购物高峰
    1: 1.2,   # 2月 - 春节期间
    2: 0.7,   # 3月
    3: 0.8,   # 4月
    4: 1.0,   # 5月 - 五一促销
    5: 0.9,   # 6月 - 毕业季
    6: 0.7,   # 7月
    7: 0.8,   # 8月
    8: 1.1,   # 9月 - 开学季
    9: 0.9,   # 10月
    10: 1.5,  # 11月 - 双十一购物节
    11: 1.2   # 12月 - 年终促销
}

# 支付方式及占比
PAYMENT_METHODS = {
    '支付宝': 0.40,
    '微信支付': 0.35,
    '银行卡': 0.15,
    '货到付款': 0.10
}

# 产品价格范围（元）
PRICE_RANGES = {
    '电子产品': {
        '手机': (800, 8000),
        '电脑': (3000, 15000),
        '平板': (1000, 8000),
        '相机': (1000, 10000),
        '耳机': (100, 2000),
        '智能手表': (300, 3000),
        '音响': (200, 5000)
    },
    '服装': {
        '上衣': (50, 500),
        '裤子': (80, 600),
        '裙子': (100, 800),
        '鞋子': (100, 1000),
        '配饰': (20, 300)
    },
    '家居': {
        '家具': (500, 8000),
        '厨具': (50, 1000),
        '床上用品': (100, 2000),
        '装饰品': (20, 500),
        '灯具': (50, 2000)
    },
    '食品': {
        '零食': (5, 100),
        '饮料': (3, 50),
        '乳制品': (5, 80),
        '粮油': (10, 150),
        '调味品': (5, 100)
    },
    '图书': {
        '小说': (20, 80),
        '教育': (30, 200),
        '科技': (40, 150),
        '艺术': (50, 300),
        '杂志': (15, 50)
    }
}

# 库存周转率参考值（越高表示周转越快）
INVENTORY_TURNOVER_REFERENCE = {
    '电子产品': 4.0,
    '服装': 5.0,
    '家居': 3.0,
    '食品': 8.0,
    '图书': 2.5
}

# 订单数量范围
ORDER_QUANTITY_RANGE = {
    '食品': (1, 10),    # 食品可能一次买多个
    '图书': (1, 3),     # 图书通常不会一次买太多
    '服装': (1, 5),     # 服装适量购买
    '电子产品': (1, 2),  # 电子产品通常单件购买
    '家居': (1, 3)      # 家居适量购买
}

# 用户购买频率分布（每个月购买的订单数）
USER_PURCHASE_FREQUENCY = {
    '高频用户': (3, 8),    # 高频用户每月购买3-8次
    '中频用户': (1, 3),    # 中频用户每月购买1-3次
    '低频用户': (0, 1)     # 低频用户不一定每月都购买
}

# 用户类型分布
USER_TYPE_DISTRIBUTION = {
    '高频用户': 0.15,
    '中频用户': 0.45,
    '低频用户': 0.40
}

# 用户年龄分布
USER_AGE_DISTRIBUTION = {
    '18-25': 0.25,
    '26-35': 0.35,
    '36-45': 0.20,
    '46-60': 0.15,
    '60+': 0.05
}

# SQL创建表语句
CREATE_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(50) NOT NULL,
        password VARCHAR(50) NOT NULL,
        email VARCHAR(100) NOT NULL,
        phone VARCHAR(20) NOT NULL,
        address VARCHAR(200) NOT NULL,
        city VARCHAR(50) NOT NULL,
        province VARCHAR(50) NOT NULL,
        postal_code VARCHAR(20) NOT NULL,
        region VARCHAR(20) NOT NULL,
        age_group VARCHAR(20) NOT NULL,
        user_type VARCHAR(20) NOT NULL,
        registration_date DATETIME NOT NULL,
        last_login DATETIME NOT NULL,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS products (
        product_id INT AUTO_INCREMENT PRIMARY KEY,
        product_name VARCHAR(100) NOT NULL,
        category VARCHAR(50) NOT NULL,
        subcategory VARCHAR(50) NOT NULL,
        description TEXT NOT NULL,
        price DECIMAL(10,2) NOT NULL,
        cost DECIMAL(10,2) NOT NULL,
        supplier VARCHAR(100) NOT NULL,
        sku VARCHAR(50) NOT NULL,
        weight DECIMAL(10,2) NOT NULL,
        dimensions VARCHAR(50) NOT NULL,
        is_active BOOLEAN DEFAULT TRUE,
        create_time DATETIME NOT NULL,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS orders (
        order_id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        order_date DATETIME NOT NULL,
        status VARCHAR(20) NOT NULL,
        payment_method VARCHAR(50) NOT NULL,
        shipping_fee DECIMAL(10,2) NOT NULL,
        total_amount DECIMAL(10,2) NOT NULL,
        discount DECIMAL(10,2) DEFAULT 0.00,
        shipping_address VARCHAR(200) NOT NULL,
        shipping_city VARCHAR(50) NOT NULL,
        shipping_province VARCHAR(50) NOT NULL,
        shipping_postal_code VARCHAR(20) NOT NULL,
        shipping_region VARCHAR(20) NOT NULL,
        delivery_date DATETIME,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(user_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS order_items (
        order_item_id INT AUTO_INCREMENT PRIMARY KEY,
        order_id INT NOT NULL,
        product_id INT NOT NULL,
        quantity INT NOT NULL,
        price DECIMAL(10,2) NOT NULL,
        discount DECIMAL(10,2) DEFAULT 0.00,
        total_price DECIMAL(10,2) NOT NULL,
        is_gift BOOLEAN DEFAULT FALSE,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (order_id) REFERENCES orders(order_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS inventory (
        inventory_id INT AUTO_INCREMENT PRIMARY KEY,
        product_id INT NOT NULL,
        quantity INT NOT NULL,
        low_stock_threshold INT NOT NULL,
        reorder_quantity INT NOT NULL,
        last_restock_date DATETIME NOT NULL,
        warehouse VARCHAR(50) NOT NULL,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS inventory_transactions (
        transaction_id INT AUTO_INCREMENT PRIMARY KEY,
        product_id INT NOT NULL,
        transaction_type ENUM('入库', '出库', '调整') NOT NULL,
        quantity INT NOT NULL,
        transaction_date DATETIME NOT NULL,
        order_id INT,
        notes VARCHAR(200),
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES products(product_id),
        FOREIGN KEY (order_id) REFERENCES orders(order_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS suppliers (
        supplier_id INT AUTO_INCREMENT PRIMARY KEY,
        supplier_name VARCHAR(100) NOT NULL,
        contact_name VARCHAR(50) NOT NULL,
        email VARCHAR(100) NOT NULL,
        phone VARCHAR(20) NOT NULL,
        address VARCHAR(200) NOT NULL,
        city VARCHAR(50) NOT NULL,
        province VARCHAR(50) NOT NULL,
        postal_code VARCHAR(20) NOT NULL,
        region VARCHAR(20) NOT NULL,
        create_time DATETIME NOT NULL,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS promotions (
        promotion_id INT AUTO_INCREMENT PRIMARY KEY,
        promotion_name VARCHAR(100) NOT NULL,
        description TEXT NOT NULL,
        discount_type ENUM('百分比', '固定金额') NOT NULL,
        discount_value DECIMAL(10,2) NOT NULL,
        start_date DATETIME NOT NULL,
        end_date DATETIME NOT NULL,
        is_active BOOLEAN DEFAULT TRUE,
        category VARCHAR(50),
        subcategory VARCHAR(50),
        min_purchase DECIMAL(10,2) DEFAULT 0.00,
        create_time DATETIME NOT NULL,
        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """
] 