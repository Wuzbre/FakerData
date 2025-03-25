import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

# 加载环境变量
load_dotenv()

# 数据库配置
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': 'sales_db',
    'port': int(os.getenv('DB_PORT', 3306))
}

# 数据生成配置
DATA_CONFIG = {
    'user_count': 50000,  # 用户数量
    'category_count': 200,  # 类别数量
    'supplier_count': 500,  # 供应商数量
    'product_count': 10000,  # 商品数量
    'order_count': 100000,  # 订单数量
    'max_order_details': 8,  # 每个订单最多的商品数量
    'date_start': '2023-01-01',  # 订单数据开始日期
    'date_end': '2023-12-31',  # 订单数据结束日期
    'batch_size': 1000  # 批量插入大小
}

# 商品类别层级配置
CATEGORY_LEVELS = {
    1: 10,  # 一级类别数量
    2: 30,  # 二级类别数量
    3: 10   # 三级类别数量
}

# 区域销售特征配置
REGIONS = {
    '华东': {
        'cities': ['上海', '江苏', '浙江', '安徽', '江西', '山东', '福建'],
        'sales_factor': 1.2,  # 销售倍数
        'user_distribution': 0.3,  # 用户分布比例
        'price_factor': 1.1  # 价格倍数
    },
    '华北': {
        'cities': ['北京', '天津', '河北', '山西', '内蒙古'],
        'sales_factor': 1.1,
        'user_distribution': 0.2,
        'price_factor': 1.2
    },
    '华中': {
        'cities': ['河南', '湖北', '湖南'],
        'sales_factor': 1.0,
        'user_distribution': 0.15,
        'price_factor': 0.9
    },
    '华南': {
        'cities': ['广东', '广西', '海南'],
        'sales_factor': 1.15,
        'user_distribution': 0.15,
        'price_factor': 1.1
    },
    '西南': {
        'cities': ['重庆', '四川', '贵州', '云南', '西藏'],
        'sales_factor': 0.9,
        'user_distribution': 0.1,
        'price_factor': 0.85
    },
    '西北': {
        'cities': ['陕西', '甘肃', '青海', '宁夏', '新疆'],
        'sales_factor': 0.85,
        'user_distribution': 0.05,
        'price_factor': 0.8
    },
    '东北': {
        'cities': ['辽宁', '吉林', '黑龙江'],
        'sales_factor': 0.95,
        'user_distribution': 0.05,
        'price_factor': 0.9
    }
}

# 节假日配置
HOLIDAYS = {
    '元旦': {
        'date': '2023-01-01',
        'duration': 3,
        'sales_boost': 1.5,
        'categories_boost': ['食品', '酒水', '礼品'],
        'user_activity_boost': 1.3
    },
    '春节': {
        'date': '2023-01-22',
        'duration': 7,
        'sales_boost': 2.0,
        'categories_boost': ['食品', '酒水', '服装', '礼品'],
        'user_activity_boost': 1.8
    },
    '清明节': {
        'date': '2023-04-05',
        'duration': 3,
        'sales_boost': 1.3,
        'categories_boost': ['食品', '鲜花'],
        'user_activity_boost': 1.2
    },
    '劳动节': {
        'date': '2023-05-01',
        'duration': 5,
        'sales_boost': 1.6,
        'categories_boost': ['服装', '电子产品', '家居'],
        'user_activity_boost': 1.4
    },
    '端午节': {
        'date': '2023-06-22',
        'duration': 3,
        'sales_boost': 1.4,
        'categories_boost': ['食品', '礼品'],
        'user_activity_boost': 1.3
    },
    '中秋节': {
        'date': '2023-09-29',
        'duration': 3,
        'sales_boost': 1.8,
        'categories_boost': ['食品', '礼品', '酒水'],
        'user_activity_boost': 1.5
    },
    '国庆节': {
        'date': '2023-10-01',
        'duration': 7,
        'sales_boost': 2.0,
        'categories_boost': ['服装', '电子产品', '家居', '食品'],
        'user_activity_boost': 1.6
    }
}

# 季节性商品配置
SEASONAL_PRODUCTS = {
    'spring': {
        'boost': 1.3,
        'months': [3, 4, 5],
        'categories': ['春装', '运动户外', '家装'],
        'weather_factor': {
            'sunny': 1.2,
            'rainy': 0.8,
            'cloudy': 1.0
        }
    },
    'summer': {
        'boost': 1.5,
        'months': [6, 7, 8],
        'categories': ['夏装', '防晒', '冷饮'],
        'weather_factor': {
            'sunny': 1.5,
            'rainy': 0.7,
            'cloudy': 1.1
        }
    },
    'autumn': {
        'boost': 1.2,
        'months': [9, 10, 11],
        'categories': ['秋装', '户外运动', '保健品'],
        'weather_factor': {
            'sunny': 1.3,
            'rainy': 0.9,
            'cloudy': 1.1
        }
    },
    'winter': {
        'boost': 1.4,
        'months': [12, 1, 2],
        'categories': ['冬装', '取暖', '保健品'],
        'weather_factor': {
            'sunny': 1.1,
            'rainy': 0.8,
            'snowy': 1.4
        }
    }
}

# 商品热度配置
PRODUCT_POPULARITY = {
    'hot': {
        'ratio': 0.2,      # 20%是热门商品
        'price_factor': 1.2,  # 价格因子
        'stock_range': (500, 2000),  # 库存范围
        'reorder_point': 200,  # 补货点
        'order_probability': 0.4  # 被订购概率
    },
    'normal': {
        'ratio': 0.5,      # 50%是普通商品
        'price_factor': 1.0,
        'stock_range': (200, 1000),
        'reorder_point': 100,
        'order_probability': 0.3
    },
    'cold': {
        'ratio': 0.3,      # 30%是冷门商品
        'price_factor': 0.8,
        'stock_range': (50, 200),
        'reorder_point': 30,
        'order_probability': 0.1
    }
}

# 用户购买行为配置
USER_BEHAVIOR = {
    'frequent': {
        'ratio': 0.2,    # 20%是高频用户
        'order_frequency': (10, 20),  # 订单频率范围（每月）
        'avg_order_amount': (500, 2000),  # 平均订单金额范围
        'promotion_sensitivity': 0.8  # 促销敏感度
    },
    'regular': {
        'ratio': 0.5,    # 50%是普通用户
        'order_frequency': (5, 10),
        'avg_order_amount': (200, 800),
        'promotion_sensitivity': 0.6
    },
    'occasional': {
        'ratio': 0.3,    # 30%是低频用户
        'order_frequency': (1, 5),
        'avg_order_amount': (100, 500),
        'promotion_sensitivity': 0.4
    }
}

# 促销活动配置
PROMOTIONS = {
    'holiday': {
        'frequency': 1.0,  # 节假日必定触发促销
        'duration_range': (3, 7),  # 持续时间范围（天）
        'product_selection_rate': 0.3,  # 商品选择比例
        'max_products': 50  # 最大参与商品数
    },
    'discount': {
        'frequency': 0.3,  # 每天触发概率
        'duration_range': (3, 7),
        'product_selection_rate': 0.2,
        'max_products': 30
    },
    'flash_sale': {
        'frequency': 0.2,
        'duration_range': (1, 3),
        'product_selection_rate': 0.1,
        'max_products': 20
    },
    'bundle': {
        'frequency': 0.15,
        'duration_range': (5, 14),
        'product_selection_rate': 0.15,
        'max_products': 25
    },
    'clearance': {
        'frequency': 0.1,
        'duration_range': (7, 14),
        'product_selection_rate': 0.25,
        'max_products': 40
    }
}

# 库存周转配置
INVENTORY_TURNOVER = {
    'fast': {
        'turnover_days_range': (7, 15),    # 快速周转商品
        'safety_stock_factor': 0.3,        # 安全库存系数
        'categories': ['食品', '生鲜', '日用品']
    },
    'normal': {
        'turnover_days_range': (15, 30),   # 正常周转商品
        'safety_stock_factor': 0.4,        # 安全库存系数
        'categories': ['服装', '电子', '家居']
    },
    'slow': {
        'turnover_days_range': (30, 60),   # 慢速周转商品
        'safety_stock_factor': 0.5,        # 安全库存系数
        'categories': ['奢侈品', '家具', '收藏品']
    }
}

# 批量插入配置
BATCH_SIZE = 1000  # 每次批量插入的数据量

# 天气影响配置
WEATHER_IMPACT = {
    'spring': {
        'weather_types': ['sunny', 'rainy', 'cloudy'],
        'weather_weights': [0.6, 0.2, 0.2]
    },
    'summer': {
        'weather_types': ['sunny', 'rainy', 'cloudy'],
        'weather_weights': [0.7, 0.2, 0.1]
    },
    'autumn': {
        'weather_types': ['sunny', 'rainy', 'cloudy'],
        'weather_weights': [0.5, 0.3, 0.2]
    },
    'winter': {
        'weather_types': ['sunny', 'snowy', 'cloudy'],
        'weather_weights': [0.4, 0.3, 0.3]
    },
    'purchase_factors': {
        'sunny': 1.2,
        'rainy': 0.8,
        'cloudy': 1.0,
        'snowy': 0.7
    },
    'delivery_factors': {
        'sunny': 1.0,
        'rainy': 1.2,
        'cloudy': 1.1,
        'snowy': 1.5
    }
} 