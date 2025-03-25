import random
from datetime import datetime, timedelta
from config import SEASONAL_PRODUCTS, REGIONS

class DataGeneratorUtils:
    @staticmethod
    def get_random_weather(date):
        """根据日期生成合理的天气"""
        month = date.month
        if month in [12, 1, 2]:  # 冬季
            return random.choices(['sunny', 'cloudy', 'snowy', 'rainy'], 
                               weights=[0.3, 0.3, 0.2, 0.2])[0]
        elif month in [3, 4, 5]:  # 春季
            return random.choices(['sunny', 'cloudy', 'rainy'], 
                               weights=[0.4, 0.3, 0.3])[0]
        elif month in [6, 7, 8]:  # 夏季
            return random.choices(['sunny', 'cloudy', 'rainy'], 
                               weights=[0.5, 0.3, 0.2])[0]
        else:  # 秋季
            return random.choices(['sunny', 'cloudy', 'rainy'], 
                               weights=[0.4, 0.4, 0.2])[0]

    @staticmethod
    def get_season_by_date(date):
        """根据日期获取季节"""
        month = date.month
        if month in [3, 4, 5]:
            return 'spring'
        elif month in [6, 7, 8]:
            return 'summer'
        elif month in [9, 10, 11]:
            return 'autumn'
        else:
            return 'winter'

    @staticmethod
    def calculate_delivery_days(region, weather):
        """计算配送天数"""
        base_days = {
            '华东': 2,
            '华北': 2,
            '华中': 3,
            '华南': 3,
            '西南': 4,
            '西北': 5,
            '东北': 4
        }
        
        # 天气影响
        weather_factor = {
            'sunny': 1.0,
            'cloudy': 1.1,
            'rainy': 1.3,
            'snowy': 1.5
        }
        
        base_delivery = base_days.get(region, 3)
        weather_impact = weather_factor.get(weather, 1.0)
        
        # 添加随机波动
        random_factor = random.uniform(0.9, 1.1)
        
        delivery_days = int(base_delivery * weather_impact * random_factor)
        return max(1, delivery_days)  # 确保至少1天

    @staticmethod
    def calculate_price_with_factors(base_price, region, season, weather, popularity_level):
        """计算考虑各种因素后的价格"""
        # 区域价格因子
        region_factor = REGIONS[region]['price_factor']
        
        # 季节性因子
        season_info = SEASONAL_PRODUCTS[season]
        season_factor = season_info['boost']
        
        # 天气因子
        weather_factor = season_info['weather_factor'].get(weather, 1.0)
        
        # 商品热度因子
        popularity_factor = {
            'hot': 1.2,
            'normal': 1.0,
            'cold': 0.8
        }.get(popularity_level, 1.0)
        
        # 计算最终价格
        final_price = base_price * region_factor * season_factor * weather_factor * popularity_factor
        
        # 添加小幅随机波动
        final_price *= random.uniform(0.95, 1.05)
        
        return round(final_price, 2)

    @staticmethod
    def generate_review(rating):
        """根据评分生成评价"""
        positive_comments = [
            "商品质量非常好，很满意",
            "包装很好，物流很快",
            "性价比高，会继续购买",
            "商品和描述一致，很满意",
            "客服服务态度很好",
            "物超所值，推荐购买"
        ]
        
        neutral_comments = [
            "商品还行，符合预期",
            "一般般，可以接受",
            "质量还可以",
            "物流速度一般",
            "性价比一般"
        ]
        
        negative_comments = [
            "商品质量不太好",
            "发货太慢了",
            "包装简陋",
            "不太满意",
            "性价比较低"
        ]
        
        if rating >= 4.5:
            return random.choice(positive_comments)
        elif rating >= 3.5:
            return random.choice(neutral_comments)
        else:
            return random.choice(negative_comments)

    @staticmethod
    def calculate_promotion_sensitivity(user_behavior, user_order_count):
        """计算用户对促销的敏感度"""
        base_sensitivity = {
            'frequent': 0.8,
            'regular': 0.6,
            'occasional': 0.4
        }.get(user_behavior, 0.5)
        
        # 订单数量影响
        order_factor = min(user_order_count / 10, 1.0)  # 最多提升100%
        
        sensitivity = base_sensitivity * (1 + order_factor)
        return round(min(sensitivity, 1.0), 2)  # 确保不超过1.0

    @staticmethod
    def calculate_supplier_stability(cooperation_days, credit_score):
        """计算供应商稳定性"""
        # 合作时间影响（最大权重0.6）
        time_factor = min(cooperation_days / 365, 2) * 0.3  # 最多2年
        
        # 信用评分影响（最大权重0.4）
        credit_factor = (credit_score / 100) * 0.4
        
        stability = time_factor + credit_factor + 0.3  # 基础稳定性0.3
        return round(min(stability, 1.0), 2)  # 确保不超过1.0 