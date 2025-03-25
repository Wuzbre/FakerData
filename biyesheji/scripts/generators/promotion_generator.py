from datetime import datetime, timedelta
import random
from ..base_generator import BaseGenerator
from ..config import (
    PROMOTIONS, HOLIDAYS, SEASONAL_PRODUCTS,
    PRODUCT_POPULARITY
)

class PromotionGenerator(BaseGenerator):
    def __init__(self, product_ids, product_info, category_seasons):
        super().__init__()
        self.product_ids = product_ids
        self.product_info = product_info
        self.category_seasons = category_seasons
        self.promotion_ids = []
        self.promotion_info = {}  # 存储促销活动信息，用于后续订单生成

    def _generate_promotion_name(self, promotion_type, holiday=None):
        """生成促销活动名称"""
        prefixes = {
            'discount': ['超值', '限时', '特惠', '折扣'],
            'flash_sale': ['限时秒杀', '闪购', '抢购', '秒杀'],
            'bundle': ['优惠套装', '组合', '搭配', '套餐'],
            'clearance': ['清仓', '库存', '特卖', '促销'],
            'holiday': ['节日', '庆典', '喜庆', '欢乐']
        }
        
        if holiday:
            return f"{holiday['name']}{random.choice(prefixes['holiday'])}专场"
        return f"{random.choice(prefixes[promotion_type])}活动"

    def _calculate_discount(self, promotion_type, product_price, product_popularity):
        """计算折扣力度"""
        # 根据促销类型和商品热度确定折扣范围
        base_discounts = {
            'holiday': (0.7, 0.9),      # 节假日促销折扣范围
            'discount': (0.8, 0.95),    # 普通折扣促销范围
            'flash_sale': (0.5, 0.7),   # 限时特卖折扣范围
            'bundle': (0.85, 0.95),     # 套装促销折扣范围
            'clearance': (0.3, 0.5)     # 清仓促销折扣范围
        }
        
        # 商品热度影响因子
        popularity_factor = {
            'hot': 0.9,      # 热门商品折扣较小
            'normal': 0.8,   # 普通商品折扣适中
            'cold': 0.7      # 冷门商品折扣较大
        }
        
        # 获取基础折扣范围
        base_discount = base_discounts.get(promotion_type, (0.7, 0.9))
        
        # 计算最终折扣率
        discount_rate = random.uniform(
            base_discount[0],
            base_discount[1]
        ) * popularity_factor[product_popularity]
        
        # 确保折扣率在合理范围内
        discount_rate = max(0.3, min(0.95, discount_rate))
        
        return {
            'discount_rate': round(discount_rate, 2),
            'promotion_price': round(product_price * discount_rate, 2)
        }

    def _select_promotion_products(self, promotion_type, start_date):
        """选择参与促销的商品"""
        selected_products = []
        
        # 获取当前季节
        current_season = list(SEASONAL_PRODUCTS.keys())[
            start_date.month % 4
        ]
        
        # 根据促销类型筛选商品
        for product_id, info in self.product_info.items():
            category_id = info['category_id']
            seasonal_info = self.category_seasons.get(category_id, {})
            
            # 清仓促销优先选择非当季商品
            if promotion_type == 'clearance':
                if (seasonal_info and 
                    current_season != seasonal_info.get('primary_season')):
                    selected_products.append(product_id)
            
            # 限时特卖优先选择热门商品
            elif promotion_type == 'flash_sale':
                if info['popularity_level'] == 'hot':
                    selected_products.append(product_id)
            
            # 其他类型促销随机选择商品
            else:
                if random.random() < PROMOTIONS[promotion_type]['product_selection_rate']:
                    selected_products.append(product_id)
        
        # 控制商品数量
        max_products = PROMOTIONS[promotion_type]['max_products']
        return random.sample(
            selected_products,
            min(len(selected_products), max_products)
        )

    def generate_promotions(self, start_date, end_date):
        """生成促销活动数据"""
        print("开始生成促销活动数据...")
        values = []
        current_date = start_date
        
        try:
            while current_date <= end_date:
                # 检查是否为节假日
                holiday = None
                for holiday_name, config in HOLIDAYS.items():
                    holiday_date = datetime.strptime(config['date'], '%Y-%m-%d')
                    if (holiday_date.month == current_date.month and 
                        holiday_date.day == current_date.day):
                        holiday = {
                            'name': holiday_name,
                            'config': config,  # 保存完整的配置信息
                            'date': config['date'],  # 保持原始日期字符串
                            'duration': config['duration'],
                            'categories': config['categories_boost']
                        }
                        break
                
                # 生成节日促销
                if holiday:
                    # 生成一个活动组ID，用于关联同一活动的不同商品
                    activity_id = self.generate_uuid()
                    promotion_name = self._generate_promotion_name('holiday', holiday)
                    
                    # 节日促销时间跨度
                    promotion_start = datetime.strptime(holiday['date'], '%Y-%m-%d')
                    promotion_end = promotion_start + timedelta(days=holiday['duration'])
                    
                    # 选择参与节日促销的商品
                    selected_products = self._select_promotion_products('holiday', current_date)
                    
                    # 为每个商品生成一个唯一的促销记录
                    for product_id in selected_products:
                        promotion_id = self.generate_uuid()  # 为每个商品生成唯一的促销ID
                        product_info = self.product_info[product_id]
                        discount_info = self._calculate_discount(
                            'holiday',
                            product_info['price'],
                            product_info['popularity_level']
                        )
                        
                        values.append((
                            promotion_id,                # promotion_id
                            promotion_name,              # promotion_name
                            'holiday',                   # promotion_type
                            promotion_start,             # start_time
                            promotion_end,               # end_time
                            discount_info['discount_rate'],  # discount_rate
                            1,                          # status (1-进行中)
                            0,                          # participant_count
                            0,                          # order_count
                            0.0,                        # total_discount_amount
                            datetime.now(),             # created_at
                            datetime.now()              # updated_at
                        ))
                        
                        # 存储促销信息供后续使用
                        if activity_id not in self.promotion_info:
                            self.promotion_info[activity_id] = {
                                'name': promotion_name,
                                'type': 'holiday',
                                'start_time': promotion_start,
                                'end_time': promotion_end,
                                'products': {}
                            }
                        self.promotion_info[activity_id]['products'][product_id] = {
                            'promotion_id': promotion_id,  # 存储每个商品的促销ID
                            'discount_rate': discount_info['discount_rate'],
                            'promotion_price': discount_info['promotion_price']
                        }
                        
                        self.promotion_ids.append(promotion_id)
                
                # 生成常规促销
                for promotion_type in ['discount', 'flash_sale', 'bundle', 'clearance']:
                    if random.random() < PROMOTIONS[promotion_type]['frequency']:
                        # 生成一个活动组ID，用于关联同一活动的不同商品
                        activity_id = self.generate_uuid()
                        promotion_name = self._generate_promotion_name(promotion_type)
                        
                        # 设置促销时间
                        duration = random.randint(
                            PROMOTIONS[promotion_type]['duration_range'][0],
                            PROMOTIONS[promotion_type]['duration_range'][1]
                        )
                        promotion_start = current_date
                        promotion_end = current_date + timedelta(days=duration)
                        
                        # 选择参与促销的商品
                        selected_products = self._select_promotion_products(
                            promotion_type,
                            current_date
                        )
                        
                        # 为每个商品生成一个唯一的促销记录
                        for product_id in selected_products:
                            promotion_id = self.generate_uuid()  # 为每个商品生成唯一的促销ID
                            product_info = self.product_info[product_id]
                            discount_info = self._calculate_discount(
                                promotion_type,
                                product_info['price'],
                                product_info['popularity_level']
                            )
                            
                            values.append((
                                promotion_id,                # promotion_id
                                promotion_name,              # promotion_name
                                promotion_type,              # promotion_type
                                promotion_start,             # start_time
                                promotion_end,               # end_time
                                discount_info['discount_rate'],  # discount_rate
                                1,                          # status (1-进行中)
                                0,                          # participant_count
                                0,                          # order_count
                                0.0,                        # total_discount_amount
                                datetime.now(),             # created_at
                                datetime.now()              # updated_at
                            ))
                            
                            # 存储促销信息供后续使用
                            if activity_id not in self.promotion_info:
                                self.promotion_info[activity_id] = {
                                    'name': promotion_name,
                                    'type': promotion_type,
                                    'start_time': promotion_start,
                                    'end_time': promotion_end,
                                    'products': {}
                                }
                            self.promotion_info[activity_id]['products'][product_id] = {
                                'promotion_id': promotion_id,  # 存储每个商品的促销ID
                                'discount_rate': discount_info['discount_rate'],
                                'promotion_price': discount_info['promotion_price']
                            }
                            
                            self.promotion_ids.append(promotion_id)
                
                current_date += timedelta(days=1)
            
            # 批量插入数据
            if values:
                self.batch_insert(
                    'promotion',
                    [
                        'promotion_id', 'promotion_name', 'promotion_type',
                        'start_time', 'end_time', 'discount_rate', 'status',
                        'participant_count', 'order_count', 'total_discount_amount',
                        'created_at', 'updated_at'
                    ],
                    values
                )
                print(f"成功生成 {len(values)} 条促销数据")
            
            return self.promotion_ids, self.promotion_info
            
        except Exception as e:
            print(f"生成促销数据时发生错误: {str(e)}")
            raise 