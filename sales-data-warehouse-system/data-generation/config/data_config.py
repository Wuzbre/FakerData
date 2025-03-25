"""数据配置文件"""

# 品牌配置
BRANDS = {
    "电子产品": [
        "华为", "小米", "苹果", "三星", "荣耀", "OPPO", "vivo", "一加", "魅族", "联想",
        "戴尔", "华硕", "宏碁", "惠普", "索尼", "LG", "松下", "飞利浦", "TCL", "海信"
    ],
    "运动品牌": [
        "耐克", "阿迪达斯", "彪马", "安踏", "李宁", "New Balance", "Under Armour",
        "匡威", "亚瑟士", "美津浓", "斯凯奇", "斐乐", "特步", "361度", "匹克"
    ],
    "奢侈品牌": [
        "古驰", "路易威登", "香奈儿", "爱马仕", "迪奥", "普拉达", "巴宝莉", "卡地亚",
        "宝格丽", "蒂芙尼", "范思哲", "圣罗兰", "纪梵希", "芬迪", "巴黎世家"
    ],
    "服装品牌": [
        "ZARA", "H&M", "优衣库", "Gap", "Forever 21", "Levi's", "Calvin Klein",
        "Tommy Hilfiger", "Ralph Lauren", "Michael Kors", "Coach", "Mango",
        "无印良品", "太平鸟", "森马", "美特斯邦威", "以纯", "海澜之家"
    ],
    "零售品牌": [
        "宜家", "苏宁", "京东", "天猫", "沃尔玛", "Target", "家乐福", "大润发",
        "永辉", "物美", "华润万家", "盒马鲜生", "全家", "7-11", "罗森"
    ],
    "汽车品牌": [
        "大众", "丰田", "宝马", "奔驰", "特斯拉", "奥迪", "保时捷", "法拉利",
        "兰博基尼", "宾利", "劳斯莱斯", "玛莎拉蒂", "捷豹", "路虎", "沃尔沃"
    ],
    "美妆品牌": [
        "兰蔻", "雅诗兰黛", "香奈儿", "迪奥", "SK-II", "资生堂", "欧莱雅",
        "美宝莲", "MAC", "YSL", "纪梵希", "植村秀", "悦木之源", "科颜氏"
    ],
    "家居品牌": [
        "宜家", "红星美凯龙", "居然之家", "索菲亚", "欧派", "尚品宅配",
        "顾家家居", "全友家居", "曲美家居", "志邦家居", "好莱客", "皮阿诺"
    ]
}

# 描述词配置
DESCRIPTORS = {
    "电子产品": [
        "Pro", "X", "Max", "Air", "Ultra", "Lite", "Plus", "Mini", "Edge",
        "智能", "迷你", "轻盈", "超薄", "高效", "坚固", "全新", "旗舰",
        "专业", "精英", "至尊", "尊享", "尊贵", "尊荣", "尊享版", "尊享版"
    ],
    "服装鞋类": [
        "时尚", "经典", "奢华", "舒适", "流行", "运动", "高性能", "休闲",
        "商务", "正装", "休闲", "运动", "潮流", "复古", "高街", "街头",
        "前卫", "优雅", "简约", "精致", "奢华", "高端", "轻奢", "轻奢风"
    ],
    "家居用品": [
        "环保", "健康", "高端", "艺术", "豪华", "限量版", "复古", "现代",
        "简约", "北欧", "日式", "美式", "欧式", "中式", "轻奢", "工业风",
        "田园", "地中海", "新中式", "极简", "轻奢", "轻奢风", "轻奢主义"
    ],
    "美妆护肤": [
        "滋润", "保湿", "美白", "抗衰", "修护", "焕颜", "紧致", "提亮",
        "控油", "清爽", "温和", "敏感", "舒缓", "修护", "焕新", "焕活",
        "奢宠", "臻选", "臻享", "臻品", "臻选", "臻享", "臻品", "臻选"
    ]
}

# 商品类别配置
CATEGORIES = {
    "电子产品": {
        "智能手机": ["旗舰机", "中端机", "入门机", "折叠屏", "游戏手机"],
        "笔记本电脑": ["轻薄本", "游戏本", "商务本", "工作站", "二合一"],
        "平板电脑": ["娱乐平板", "商务平板", "教育平板", "专业平板"],
        "智能手表": ["运动手表", "商务手表", "健康手表", "儿童手表"],
        "智能家居": ["智能音箱", "智能门锁", "智能摄像头", "智能灯具"],
        "游戏设备": ["游戏主机", "游戏手柄", "VR设备", "游戏耳机"],
        "摄影器材": ["相机", "镜头", "无人机", "运动相机", "摄影配件"]
    },
    "服装鞋类": {
        "男装": ["T恤", "衬衫", "西装", "夹克", "风衣", "休闲裤", "牛仔裤"],
        "女装": ["连衣裙", "半身裙", "T恤", "衬衫", "外套", "裤子", "套装"],
        "童装": ["上衣", "裤子", "裙子", "外套", "套装", "鞋袜", "配饰"],
        "运动服": ["运动T恤", "运动裤", "运动外套", "运动套装", "瑜伽服"],
        "鞋类": ["运动鞋", "休闲鞋", "正装鞋", "凉鞋", "拖鞋", "靴子"],
        "配饰": ["帽子", "围巾", "手套", "腰带", "袜子", "包包", "首饰"]
    },
    "家居用品": {
        "家具": ["沙发", "床", "餐桌", "书桌", "衣柜", "电视柜", "茶几"],
        "家电": ["电视", "冰箱", "洗衣机", "空调", "热水器", "微波炉", "烤箱"],
        "厨具": ["锅具", "餐具", "刀具", "厨房电器", "厨房收纳", "厨房清洁"],
        "家纺": ["床品", "窗帘", "地毯", "抱枕", "毛巾", "浴巾", "地垫"],
        "装饰": ["挂画", "摆件", "花瓶", "绿植", "香薰", "时钟", "镜子"],
        "收纳": ["收纳盒", "收纳柜", "置物架", "鞋柜", "衣柜", "书柜"]
    },
    "美妆护肤": {
        "护肤": ["洁面", "爽肤水", "精华", "面霜", "面膜", "防晒", "眼霜"],
        "彩妆": ["粉底", "口红", "眼影", "睫毛膏", "腮红", "修容", "定妆"],
        "香水": ["女士香水", "男士香水", "中性香水"],
        "美妆工具": ["化妆刷", "美妆蛋", "化妆镜", "化妆包", "美妆收纳"],
        "护发": ["洗发水", "护发素", "发膜", "精油", "造型产品"],
        "美体": ["沐浴露", "身体乳", "护手霜", "足部护理", "脱毛产品"]
    }
}

# 价格范围配置
PRICE_RANGES = {
    "电子产品": {
        "智能手机": (1000, 15000),
        "笔记本电脑": (3000, 30000),
        "平板电脑": (1000, 10000),
        "智能手表": (500, 5000),
        "智能家居": (100, 5000),
        "游戏设备": (500, 10000),
        "摄影器材": (1000, 50000)
    },
    "服装鞋类": {
        "男装": (50, 5000),
        "女装": (50, 5000),
        "童装": (30, 1000),
        "运动服": (100, 2000),
        "鞋类": (100, 3000),
        "配饰": (20, 2000)
    },
    "家居用品": {
        "家具": (500, 50000),
        "家电": (1000, 50000),
        "厨具": (50, 5000),
        "家纺": (50, 3000),
        "装饰": (50, 5000),
        "收纳": (30, 3000)
    },
    "美妆护肤": {
        "护肤": (50, 5000),
        "彩妆": (30, 3000),
        "香水": (200, 5000),
        "美妆工具": (20, 1000),
        "护发": (30, 1000),
        "美体": (30, 1000)
    }
}

# 用户类型分布
USER_TYPE_DISTRIBUTION = {
    "普通用户": 0.7,
    "VIP用户": 0.2,
    "企业用户": 0.1
}

# 用户年龄分布
USER_AGE_DISTRIBUTION = {
    "18-24岁": 0.2,
    "25-34岁": 0.4,
    "35-44岁": 0.25,
    "45-54岁": 0.1,
    "55岁以上": 0.05
}

# 区域分布
REGION_DISTRIBUTION = {
    "华东": 0.3,
    "华北": 0.2,
    "华南": 0.2,
    "华中": 0.15,
    "西南": 0.1,
    "西北": 0.05
}

# 区域省份映射
REGIONS = {
    "华东": ["上海", "江苏", "浙江", "安徽", "福建", "江西", "山东"],
    "华北": ["北京", "天津", "河北", "山西", "内蒙古"],
    "华南": ["广东", "广西", "海南"],
    "华中": ["河南", "湖北", "湖南"],
    "西南": ["重庆", "四川", "贵州", "云南", "西藏"],
    "西北": ["陕西", "甘肃", "青海", "宁夏", "新疆"]
}

# 支付方式
PAYMENT_METHODS = {
    "支付宝": 0.4,
    "微信支付": 0.4,
    "银行卡": 0.15,
    "其他": 0.05
}

# 库存周转率参考
INVENTORY_TURNOVER_REFERENCE = {
    "电子产品": 6.0,
    "服装鞋类": 4.0,
    "家居用品": 3.0,
    "美妆护肤": 5.0,
    "食品": 8.0,
    "图书": 2.0
}

# 创建表的SQL语句
CREATE_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(50) NOT NULL UNIQUE,
        password VARCHAR(255) NOT NULL,
        email VARCHAR(100) NOT NULL UNIQUE,
        phone VARCHAR(20) NOT NULL,
        address TEXT,
        city VARCHAR(50),
        province VARCHAR(50),
        postal_code VARCHAR(20),
        region VARCHAR(20),
        age_group VARCHAR(20),
        user_type VARCHAR(20),
        registration_date DATETIME,
        last_login DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS suppliers (
        supplier_id INT AUTO_INCREMENT PRIMARY KEY,
        supplier_name VARCHAR(100) NOT NULL,
        contact_name VARCHAR(50),
        email VARCHAR(100),
        phone VARCHAR(20),
        address TEXT,
        city VARCHAR(50),
        province VARCHAR(50),
        postal_code VARCHAR(20),
        region VARCHAR(20),
        create_time DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS products (
        product_id INT AUTO_INCREMENT PRIMARY KEY,
        product_name VARCHAR(200) NOT NULL,
        category VARCHAR(50) NOT NULL,
        subcategory VARCHAR(50),
        description TEXT,
        price DECIMAL(10,2) NOT NULL,
        cost DECIMAL(10,2),
        supplier VARCHAR(50),
        sku VARCHAR(50) UNIQUE,
        weight DECIMAL(10,2),
        dimensions VARCHAR(50),
        is_active BOOLEAN DEFAULT TRUE,
        create_time DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS orders (
        order_id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        order_date DATETIME NOT NULL,
        status VARCHAR(20) NOT NULL,
        payment_method VARCHAR(20),
        shipping_fee DECIMAL(10,2),
        total_amount DECIMAL(10,2) NOT NULL,
        discount DECIMAL(10,2),
        shipping_address TEXT,
        shipping_city VARCHAR(50),
        shipping_province VARCHAR(50),
        shipping_postal_code VARCHAR(20),
        shipping_region VARCHAR(20),
        delivery_date DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
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
        discount DECIMAL(10,2),
        total_price DECIMAL(10,2) NOT NULL,
        is_gift BOOLEAN DEFAULT FALSE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (order_id) REFERENCES orders(order_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS inventory (
        inventory_id INT AUTO_INCREMENT PRIMARY KEY,
        product_id INT NOT NULL,
        quantity INT NOT NULL,
        low_stock_threshold INT,
        reorder_quantity INT,
        last_restock_date DATETIME,
        warehouse VARCHAR(50),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS inventory_transactions (
        transaction_id INT AUTO_INCREMENT PRIMARY KEY,
        product_id INT NOT NULL,
        transaction_type VARCHAR(20) NOT NULL,
        quantity INT NOT NULL,
        transaction_date DATETIME NOT NULL,
        order_id INT,
        notes TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES products(product_id),
        FOREIGN KEY (order_id) REFERENCES orders(order_id)
    )
    """
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

# 维表配置
DIMENSION_TABLES = {
    # 时间维表
    "dim_date": """
    CREATE TABLE IF NOT EXISTS dim_date (
        date_key INT PRIMARY KEY,
        date DATE NOT NULL,
        year INT NOT NULL,
        quarter INT NOT NULL,
        month INT NOT NULL,
        day INT NOT NULL,
        day_of_week INT NOT NULL,
        day_of_year INT NOT NULL,
        week_of_year INT NOT NULL,
        is_weekend BOOLEAN NOT NULL,
        is_holiday BOOLEAN NOT NULL,
        holiday_name VARCHAR(50),
        is_workday BOOLEAN NOT NULL,
        season VARCHAR(20) NOT NULL,
        fiscal_year INT NOT NULL,
        fiscal_quarter INT NOT NULL,
        fiscal_month INT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP
    )
    """,
    
    # 地区维表
    "dim_location": """
    CREATE TABLE IF NOT EXISTS dim_location (
        location_key INT PRIMARY KEY,
        country VARCHAR(50) NOT NULL,
        province VARCHAR(50) NOT NULL,
        city VARCHAR(50) NOT NULL,
        district VARCHAR(50),
        street VARCHAR(100),
        postal_code VARCHAR(20),
        region VARCHAR(20) NOT NULL,
        latitude DECIMAL(10,6),
        longitude DECIMAL(10,6),
        is_active BOOLEAN DEFAULT TRUE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP
    )
    """,
    
    # 支付方式维表
    "dim_payment_method": """
    CREATE TABLE IF NOT EXISTS dim_payment_method (
        payment_method_key INT PRIMARY KEY,
        payment_method_code VARCHAR(20) NOT NULL UNIQUE,
        payment_method_name VARCHAR(50) NOT NULL,
        payment_type VARCHAR(20) NOT NULL,
        description TEXT,
        is_active BOOLEAN DEFAULT TRUE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP
    )
    """,
    
    # 商品类别维表
    "dim_product_category": """
    CREATE TABLE IF NOT EXISTS dim_product_category (
        category_key INT PRIMARY KEY,
        category_code VARCHAR(20) NOT NULL UNIQUE,
        category_name VARCHAR(50) NOT NULL,
        parent_category_key INT,
        category_level INT NOT NULL,
        category_path VARCHAR(200) NOT NULL,
        description TEXT,
        is_active BOOLEAN DEFAULT TRUE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (parent_category_key) REFERENCES dim_product_category(category_key)
    )
    """,
    
    # 供应商维表
    "dim_supplier": """
    CREATE TABLE IF NOT EXISTS dim_supplier (
        supplier_key INT PRIMARY KEY,
        supplier_code VARCHAR(20) NOT NULL UNIQUE,
        supplier_name VARCHAR(100) NOT NULL,
        contact_name VARCHAR(50),
        contact_phone VARCHAR(20),
        contact_email VARCHAR(100),
        address TEXT,
        city VARCHAR(50),
        province VARCHAR(50),
        postal_code VARCHAR(20),
        region VARCHAR(20),
        business_license VARCHAR(50),
        tax_number VARCHAR(50),
        credit_rating VARCHAR(20),
        is_active BOOLEAN DEFAULT TRUE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP
    )
    """,
    
    # 仓库维表
    "dim_warehouse": """
    CREATE TABLE IF NOT EXISTS dim_warehouse (
        warehouse_key INT PRIMARY KEY,
        warehouse_code VARCHAR(20) NOT NULL UNIQUE,
        warehouse_name VARCHAR(100) NOT NULL,
        warehouse_type VARCHAR(20) NOT NULL,
        address TEXT,
        city VARCHAR(50),
        province VARCHAR(50),
        postal_code VARCHAR(20),
        region VARCHAR(20),
        manager VARCHAR(50),
        contact_phone VARCHAR(20),
        capacity DECIMAL(10,2),
        is_active BOOLEAN DEFAULT TRUE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP
    )
    """,
    
    # 员工维表
    "dim_employee": """
    CREATE TABLE IF NOT EXISTS dim_employee (
        employee_key INT PRIMARY KEY,
        employee_code VARCHAR(20) NOT NULL UNIQUE,
        employee_name VARCHAR(50) NOT NULL,
        department VARCHAR(50) NOT NULL,
        position VARCHAR(50) NOT NULL,
        hire_date DATE NOT NULL,
        email VARCHAR(100),
        phone VARCHAR(20),
        status VARCHAR(20) NOT NULL,
        is_active BOOLEAN DEFAULT TRUE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP
    )
    """,
    
    # 客户维表
    "dim_customer": """
    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_key INT PRIMARY KEY,
        customer_code VARCHAR(20) NOT NULL UNIQUE,
        customer_name VARCHAR(100) NOT NULL,
        customer_type VARCHAR(20) NOT NULL,
        gender VARCHAR(10),
        birth_date DATE,
        email VARCHAR(100),
        phone VARCHAR(20),
        address TEXT,
        city VARCHAR(50),
        province VARCHAR(50),
        postal_code VARCHAR(20),
        region VARCHAR(20),
        registration_date DATE NOT NULL,
        customer_level VARCHAR(20),
        points INT DEFAULT 0,
        is_active BOOLEAN DEFAULT TRUE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP
    )
    """
}

# 维表数据生成配置
DIMENSION_DATA_GENERATION = {
    # 时间维表配置
    "dim_date": {
        "start_date": "2023-01-01",
        "end_date": "2023-12-31",
        "holidays": {
            "2023-01-01": "元旦",
            "2023-01-22": "春节",
            "2023-02-05": "元宵节",
            "2023-04-05": "清明节",
            "2023-05-01": "劳动节",
            "2023-06-22": "端午节",
            "2023-09-29": "中秋节",
            "2023-10-01": "国庆节"
        }
    },
    
    # 地区维表配置
    "dim_location": {
        "regions": REGIONS,
        "distribution": REGION_DISTRIBUTION
    },
    
    # 支付方式维表配置
    "dim_payment_method": {
        "methods": [
            {"code": "ALIPAY", "name": "支付宝", "type": "电子支付"},
            {"code": "WECHAT", "name": "微信支付", "type": "电子支付"},
            {"code": "BANK", "name": "银行卡", "type": "银行卡"},
            {"code": "CASH", "name": "现金", "type": "现金"},
            {"code": "OTHER", "name": "其他", "type": "其他"}
        ]
    },
    
    # 商品类别维表配置
    "dim_product_category": {
        "categories": CATEGORIES,
        "levels": 3  # 类别层级数
    },
    
    # 供应商维表配置
    "dim_supplier": {
        "count": 50,  # 供应商数量
        "types": ["生产商", "代理商", "经销商", "批发商", "零售商"]
    },
    
    # 仓库维表配置
    "dim_warehouse": {
        "count": 10,  # 仓库数量
        "types": ["中心仓", "区域仓", "配送仓", "门店仓", "保税仓"]
    },
    
    # 员工维表配置
    "dim_employee": {
        "count": 100,  # 员工数量
        "departments": ["销售部", "采购部", "仓储部", "物流部", "客服部", "财务部", "IT部", "人力资源部"],
        "positions": ["经理", "主管", "专员", "助理", "实习生"]
    },
    
    # 客户维表配置
    "dim_customer": {
        "count": 1000,  # 客户数量
        "types": ["个人", "企业", "VIP"],
        "levels": ["普通", "银卡", "金卡", "钻石卡", "黑卡"]
    }
}

# 维表关联配置
DIMENSION_RELATIONSHIPS = {
    "orders": {
        "dim_date": "order_date",
        "dim_customer": "customer_key",
        "dim_employee": "employee_key",
        "dim_payment_method": "payment_method_key",
        "dim_location": "shipping_location_key"
    },
    "products": {
        "dim_product_category": "category_key",
        "dim_supplier": "supplier_key"
    },
    "inventory": {
        "dim_warehouse": "warehouse_key",
        "dim_product": "product_key"
    }
}

# 活动配置
ACTIVITY_CONFIG = {
    "activity_types": {
        "大促活动": {
            "weight": 0.1,  # 权重
            "duration_range": (3, 7),  # 持续时间范围（天）
            "discount_range": (0.3, 0.5),  # 折扣范围
            "sales_increase": (2.0, 3.0),  # 销量提升倍数
            "customer_increase": (1.5, 2.0),  # 客户数提升倍数
            "frequency": "yearly"  # 频率
        },
        "节日活动": {
            "weight": 0.2,
            "duration_range": (1, 3),
            "discount_range": (0.2, 0.3),
            "sales_increase": (1.5, 2.0),
            "customer_increase": (1.2, 1.5),
            "frequency": "monthly"
        },
        "周末活动": {
            "weight": 0.3,
            "duration_range": (1, 2),
            "discount_range": (0.1, 0.2),
            "sales_increase": (1.2, 1.5),
            "customer_increase": (1.1, 1.3),
            "frequency": "weekly"
        },
        "日常活动": {
            "weight": 0.4,
            "duration_range": (1, 1),
            "discount_range": (0.05, 0.15),
            "sales_increase": (1.1, 1.3),
            "customer_increase": (1.05, 1.2),
            "frequency": "daily"
        }
    },
    "activity_status": ["筹备中", "进行中", "已结束", "已取消"],
    "activity_channels": ["线上", "线下", "全渠道"],
    "activity_targets": ["新客户", "老客户", "所有客户"],
    "activity_metrics": {
        "views": (1000, 100000),
        "participants": (100, 10000),
        "conversion_rate": (0.01, 0.1),
        "avg_order_value": (100, 1000)
    }
}

# 活动时间配置
ACTIVITY_DATES = {
    "大促活动": {
        "618": {"start": "06-18", "end": "06-20"},
        "双11": {"start": "11-11", "end": "11-13"},
        "双12": {"start": "12-12", "end": "12-14"},
        "年货节": {"start": "01-15", "end": "01-25"}
    },
    "节日活动": {
        "春节": {"start": "02-10", "end": "02-17"},
        "元宵节": {"start": "02-24", "end": "02-24"},
        "劳动节": {"start": "05-01", "end": "05-05"},
        "端午节": {"start": "06-10", "end": "06-12"},
        "中秋节": {"start": "09-17", "end": "09-19"},
        "国庆节": {"start": "10-01", "end": "10-07"}
    }
} 