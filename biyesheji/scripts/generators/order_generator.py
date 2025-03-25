from datetime import datetime, timedelta
import random
from ..base_generator import BaseGenerator
from ..utils import DataGeneratorUtils
from ..config import (
    USER_BEHAVIOR, SEASONAL_PRODUCTS, HOLIDAYS,
    PRODUCT_POPULARITY, REGIONS, WEATHER_IMPACT
)

class OrderGenerator(BaseGenerator):
    def __init__(self):
        super().__init__()
        self.order_ids = []
        self.order_details = {}

    def _get_active_promotions(self, date, product_id, promotion_data):
        """获取指定日期商品的有效促销活动"""
        promotion_ids, promotion_info = promotion_data
        active_promos = []
        
        # 遍历所有促销活动
        for activity_id, activity_info in promotion_info.items():
            # 检查日期是否在促销期间
            if (activity_info['start_time'].date() <= date.date() <= activity_info['end_time'].date()):
                # 检查商品是否参与该促销
                if product_id in activity_info['products']:
                    product_promo = activity_info['products'][product_id]
                    active_promos.append({
                        'promotion_id': product_promo['promotion_id'],
                        'name': activity_info['name'],
                        'type': activity_info['type'],
                        'discount_rate': product_promo['discount_rate'],
                        'promotion_price': product_promo['promotion_price']
                    })
        
        return active_promos

    def _get_weather(self, date):
        """根据日期生成天气情况"""
        season = list(SEASONAL_PRODUCTS.keys())[date.month % 4]
        weathers = WEATHER_IMPACT[season]['weather_types']
        weights = WEATHER_IMPACT[season]['weather_weights']
        return random.choices(weathers, weights=weights)[0]

    def _calculate_delivery_days(self, region, weather):
        """计算配送天数
        
        根据区域销售因子和天气情况计算配送天数：
        - 销售因子越高的区域，基础配送天数越短（物流更发达）
        - 天气会影响实际配送天数
        """
        # 使用区域销售因子的倒数作为基础配送天数的参考
        # 销售因子高的区域（如华东1.2）会有较短的配送时间（如2-3天）
        # 销售因子低的区域（如西北0.85）会有较长的配送时间（如4-5天）
        base_days = round(3 / REGIONS[region]['sales_factor'])
        
        # 天气影响
        weather_factor = WEATHER_IMPACT['delivery_factors'][weather]
        
        # 计算最终配送天数，确保至少1天
        return max(1, round(base_days * weather_factor))

    def _generate_review(self, rating):
        """根据评分生成评价"""
        if rating >= 4.5:
            comments = [
                "非常满意，质量很好！",
                "超出预期，很划算！",
                "包装很好，物流快，好评！",
                "性价比很高，推荐购买！"
            ]
        elif rating >= 3.5:
            comments = [
                "质量还不错，符合预期",
                "一般般，可以接受",
                "还行，价格合适",
                "基本满意，继续关注"
            ]
        else:
            comments = [
                "一般般，有待改进",
                "不太满意，期望更好",
                "质量一般，价格偏高",
                "物流有点慢，其他还行"
            ]
        return random.choice(comments)

    def _calculate_purchase_probability(self, user_behavior, product_popularity,
                                     weather, is_holiday, has_promotion):
        """计算购买概率"""
        # 基础购买概率
        base_prob = USER_BEHAVIOR[user_behavior]['promotion_sensitivity']
        
        # 商品热度影响
        popularity_factor = {
            'hot': 1.5,
            'normal': 1.0,
            'cold': 0.7
        }[product_popularity]
        
        # 天气影响
        weather_factor = WEATHER_IMPACT['purchase_factors'][weather]
        
        # 节假日影响
        holiday_factor = 1.5 if is_holiday else 1.0
        
        # 促销活动影响
        promotion_factor = 1.3 if has_promotion else 1.0
        
        final_prob = (base_prob * popularity_factor * weather_factor *
                     holiday_factor * promotion_factor)
        
        return min(1.0, final_prob)

    def generate_orders(self, start_date, end_date, user_data, product_data,
                      promotion_data, target_order_count, max_order_details):
        """生成订单数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            user_data: 用户数据 (user_ids, user_behaviors, user_regions)
            product_data: 商品数据 (product_ids, product_info)
            promotion_data: 促销数据
            target_order_count: 目标订单数量
            max_order_details: 每个订单最大商品数量
            
        Returns:
            dict: 订单详情数据，格式为 {order_id: {product_id: {'quantity': quantity, 'price': price}}}
        """
        print("开始生成订单数据...")
        order_values = []
        detail_values = []
        current_date = start_date
        
        # 解包数据
        user_ids, user_behaviors, user_regions = user_data
        product_ids, product_info = product_data
        
        # 计算每天平均需要生成的订单数量
        total_days = (end_date - start_date).days + 1
        avg_orders_per_day = max(1, target_order_count // total_days)
        remaining_orders = target_order_count
        
        while current_date <= end_date and remaining_orders > 0:
            # 获取当天的环境因素
            weather = self._get_weather(current_date)
            
            # 检查是否为节假日
            is_holiday = False
            for holiday_name, config in HOLIDAYS.items():
                holiday_date = datetime.strptime(config['date'], '%Y-%m-%d').date()
                if (holiday_date <= current_date.date() <= 
                    holiday_date + timedelta(days=config['duration'] - 1)):
                    is_holiday = True
                    break
            
            # 确定今天要生成的订单数量
            if current_date == end_date:
                # 最后一天，使用剩余的订单数量
                today_orders = remaining_orders
            else:
                # 在平均值的基础上随机浮动±20%
                variation = int(avg_orders_per_day * 0.2)
                today_orders = random.randint(
                    max(1, avg_orders_per_day - variation),
                    avg_orders_per_day + variation
                )
                today_orders = min(today_orders, remaining_orders)
            
            # 随机选择用户生成订单
            for _ in range(today_orders):
                user_id = random.choice(user_ids)
                user_behavior = user_behaviors[user_id]
                region, city = user_regions[user_id]
                
                # 生成订单
                order_id = self.generate_uuid()
                order_time = datetime.combine(
                    current_date,
                    datetime.strptime(
                        f"{random.randint(9, 22)}:{random.randint(0, 59)}",
                        "%H:%M"
                    ).time()
                )
                
                # 选择要购买的商品
                num_products = random.randint(1, max_order_details)
                order_products = {}
                total_amount = 0
                
                for _ in range(num_products):
                    # 随机选择一个商品
                    product_id = random.choice(product_ids)
                    product_info_item = product_info[product_id]
                    
                    # 检查是否有有效的促销活动
                    active_promos = self._get_active_promotions(
                        current_date, product_id, promotion_data
                    )
                    has_promotion = bool(active_promos)
                    
                    # 计算购买概率
                    purchase_prob = self._calculate_purchase_probability(
                        user_behavior,
                        product_info_item['popularity_level'],
                        weather,
                        is_holiday,
                        has_promotion
                    )
                    
                    if random.random() <= purchase_prob:
                        # 确定购买数量
                        quantity = random.randint(1, 3)
                        
                        # 确定商品价格
                        original_price = product_info_item['price']
                        final_price = original_price
                        promotion_id = None
                        is_promotion_price = False
                        
                        if active_promos:
                            # 使用最大折扣的促销
                            best_promo = min(
                                active_promos,
                                key=lambda x: x['promotion_price']
                            )
                            final_price = best_promo['promotion_price']
                            promotion_id = best_promo['promotion_id']
                            is_promotion_price = True
                        
                        # 生成评价
                        rating = round(random.uniform(3.0, 5.0), 1)
                        review = self._generate_review(rating)
                        
                        # 添加到订单商品中
                        order_products[product_id] = {
                            'quantity': quantity,
                            'price': final_price
                        }
                        
                        # 计算总金额
                        total_amount += final_price * quantity
                        
                        # 添加订单明细记录
                        detail_values.append((
                            self.generate_uuid(),    # detail_id
                            order_id,                # order_id
                            product_id,              # product_id
                            quantity,                # quantity
                            original_price,          # original_price
                            final_price,             # actual_price
                            promotion_id,            # promotion_id
                            is_promotion_price,      # is_promotion_price
                            rating,                  # rating
                            review,                  # review
                            datetime.now(),          # create_time
                            datetime.now()           # update_time
                        ))
                
                if order_products:  # 只有当有商品被购买时才创建订单
                    # 计算配送天数
                    delivery_days = self._calculate_delivery_days(region, weather)
                    
                    # 添加订单主表记录
                    order_values.append((
                        order_id,                    # order_id
                        user_id,                     # user_id
                        1,                          # order_status (1: 已付款)
                        total_amount,                # total_amount
                        total_amount,                # actual_amount
                        random.randint(1, 3),        # payment_method
                        f"{city}市某某街道",          # shipping_address
                        "1" + "".join([str(random.randint(0, 9)) for _ in range(10)]),  # shipping_phone
                        f"收货人{user_id[-4:]}",      # shipping_name
                        region,                      # region
                        city,                        # city
                        'holiday' if is_holiday else 'normal',  # order_source
                        None,                        # promotion_id
                        0,                          # promotion_discount
                        weather,                     # weather
                        delivery_days,               # delivery_days
                        0,                          # is_first_order
                        datetime.now(),              # created_at
                        datetime.now()               # updated_at
                    ))
                    
                    self.order_ids.append(order_id)
                    self.order_details[order_id] = order_products
                    remaining_orders -= 1
            
            current_date += timedelta(days=1)
        
        # 批量插入订单数据
        order_columns = [
            'order_id', 'user_id', 'order_status', 'total_amount',
            'actual_amount', 'payment_method', 'shipping_address',
            'shipping_phone', 'shipping_name', 'region', 'city',
            'order_source', 'promotion_id', 'promotion_discount',
            'weather', 'delivery_days', 'is_first_order',
            'created_at', 'updated_at'
        ]
        self.batch_insert('`order`', order_columns, order_values)
        
        # 批量插入订单明细数据
        detail_columns = [
            'detail_id', 'order_id', 'product_id', 'quantity',
            'unit_price', 'original_price', 'total_price',
            'discount_amount', 'is_seasonal_price', 'is_promotion_price',
            'rating', 'review', 'created_at', 'updated_at'
        ]
        
        # 修改订单明细数据的值
        new_detail_values = []
        for (detail_id, order_id, product_id, quantity,
             original_price, actual_price, promotion_id,
             is_promotion_price, rating, review, create_time,
             update_time) in detail_values:
            
            new_detail_values.append((
                detail_id,                   # detail_id
                order_id,                    # order_id
                product_id,                  # product_id
                quantity,                    # quantity
                actual_price,                # unit_price
                original_price,              # original_price
                actual_price * quantity,     # total_price
                (original_price - actual_price) * quantity,  # discount_amount
                False,                       # is_seasonal_price
                is_promotion_price,          # is_promotion_price
                rating,                      # rating
                review,                      # review
                create_time,                 # created_at
                update_time                  # updated_at
            ))
        
        self.batch_insert('order_detail', detail_columns, new_detail_values)
        
        print(f"成功生成 {len(order_values)} 条订单数据")
        print(f"成功生成 {target_order_count} 个订单")
        
        return self.order_details 