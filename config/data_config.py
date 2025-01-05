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