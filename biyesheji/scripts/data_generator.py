import uuid
import random
from datetime import datetime, timedelta
from faker import Faker
import mysql.connector
from mysql.connector import Error
from tqdm import tqdm
import hashlib
from config import (DB_CONFIG, DATA_CONFIG, CATEGORY_LEVELS, BATCH_SIZE,
                   REGIONS, HOLIDAYS, SEASONAL_PRODUCTS, PRODUCT_POPULARITY,
                   USER_BEHAVIOR, PROMOTIONS)

class DataGenerator:
    def __init__(self):
        self.fake = Faker(['zh_CN'])
        self.conn = None
        self.cursor = None
        self.connect_database()
        
        # 存储生成的ID以供关联
        self.user_ids = []
        self.category_ids = []
        self.supplier_ids = []
        self.product_ids = []
        self.promotion_ids = {}  # 存储活动ID映射
        
        # 存储商品属性
        self.product_attributes = {}  # 存储商品的季节性和热度信息
        
        # 初始化用户行为分布
        self.user_behaviors = {}

    def connect_database(self):
        try:
            self.conn = mysql.connector.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor()
            print("数据库连接成功")
        except Error as e:
            print(f"数据库连接失败: {e}")
            exit(1)

    def generate_uuid(self):
        return str(uuid.uuid4()).replace('-', '')

    def get_random_region(self):
        region = random.choice(list(REGIONS.keys()))
        city = random.choice(REGIONS[region])
        return region, city

    def generate_promotions(self):
        print("生成活动数据...")
        sql = """INSERT INTO promotion (promotion_id, promotion_name, promotion_type,
                start_time, end_time, discount_rate, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        
        promotions = []
        
        # 生成节假日活动
        for holiday_name, holiday_info in HOLIDAYS.items():
            promotion_id = self.generate_uuid()
            start_date = datetime.strptime(holiday_info['date'], '%Y-%m-%d')
            end_date = start_date + timedelta(days=holiday_info['duration'])
            discount_rate = round(1 / holiday_info['sales_boost'], 2)
            
            promotions.append((
                promotion_id,
                f"{holiday_name}促销活动",
                'holiday',
                start_date,
                end_date,
                discount_rate,
                2 if end_date < datetime.now() else 1
            ))
            self.promotion_ids[holiday_name] = promotion_id
        
        # 生成购物节活动
        for festival_name, festival_info in PROMOTIONS.items():
            promotion_id = self.generate_uuid()
            start_date = datetime.strptime(festival_info['start'], '%Y-%m-%d')
            end_date = start_date + timedelta(days=festival_info['duration'])
            
            promotions.append((
                promotion_id,
                festival_name,
                'festival',
                start_date,
                end_date,
                festival_info['discount'],
                2 if end_date < datetime.now() else 1
            ))
            self.promotion_ids[festival_name] = promotion_id

        self.cursor.executemany(sql, promotions)
        self.conn.commit()

    def generate_users(self):
        print("生成用户数据...")
        sql = """INSERT INTO user (user_id, username, password, email, phone, address,
                region, city, user_level, register_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        users = []
        user_levels = []
        
        # 根据用户行为配置分配用户等级
        for level, ratio in USER_BEHAVIOR.items():
            level_count = int(DATA_CONFIG['user_count'] * ratio)
            user_levels.extend([level] * level_count)
        
        # 补齐因四舍五入导致的差异
        while len(user_levels) < DATA_CONFIG['user_count']:
            user_levels.append('regular')
        
        random.shuffle(user_levels)
        
        for i in tqdm(range(DATA_CONFIG['user_count'])):
            user_id = self.generate_uuid()
            self.user_ids.append(user_id)
            region, city = self.get_random_region()
            
            register_time = self.fake.date_time_between(
                start_date='-1y',
                end_date='now'
            )
            
            user_level = user_levels[i]
            self.user_behaviors[user_id] = user_level
            
            users.append((
                user_id,
                self.fake.user_name(),
                hashlib.md5("123456".encode()).hexdigest(),
                self.fake.email(),
                self.fake.phone_number(),
                self.fake.address(),
                region,
                city,
                user_level,
                register_time
            ))

            if len(users) >= BATCH_SIZE:
                self.cursor.executemany(sql, users)
                self.conn.commit()
                users = []

        if users:
            self.cursor.executemany(sql, users)
            self.conn.commit()

    def generate_categories(self):
        print("生成商品类别数据...")
        sql = """INSERT INTO category (category_id, category_name, parent_id, category_level,
                sort_order, is_seasonal, season_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        
        categories = []
        parent_map = {0: None}
        seasonal_categories = ['服装', '鞋帽', '运动户外', '家居用品']
        
        for level, count in CATEGORY_LEVELS.items():
            for i in range(count):
                category_id = self.generate_uuid()
                self.category_ids.append(category_id)
                
                if level == 1:
                    parent_id = None
                    category_name = random.choice(seasonal_categories) if i < len(seasonal_categories) else f"类别{level}-{i+1}"
                    is_seasonal = category_name in seasonal_categories
                else:
                    parent_id = random.choice([cid for cid in self.category_ids 
                                            if cid in parent_map and parent_map[cid] == level-1])
                    category_name = f"类别{level}-{i+1}"
                    is_seasonal = False
                
                parent_map[category_id] = level
                season_type = random.choice(['spring', 'summer', 'autumn', 'winter']) if is_seasonal else None
                
                categories.append((
                    category_id,
                    category_name,
                    parent_id,
                    level,
                    i + 1,
                    is_seasonal,
                    season_type
                ))

        self.cursor.executemany(sql, categories)
        self.conn.commit()

    def generate_suppliers(self):
        print("生成供应商数据...")
        sql = """INSERT INTO supplier (supplier_id, supplier_name, contact_name, contact_phone,
                email, address, region, city, supply_capacity, cooperation_start_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        suppliers = []
        for _ in tqdm(range(DATA_CONFIG['supplier_count'])):
            supplier_id = self.generate_uuid()
            self.supplier_ids.append(supplier_id)
            region, city = self.get_random_region()
            
            cooperation_start_date = self.fake.date_between(
                start_date='-2y',
                end_date='today'
            )
            
            suppliers.append((
                supplier_id,
                f"{self.fake.company()}",
                self.fake.name(),
                self.fake.phone_number(),
                self.fake.company_email(),
                self.fake.address(),
                region,
                city,
                random.randint(1000, 10000),
                cooperation_start_date
            ))

            if len(suppliers) >= BATCH_SIZE:
                self.cursor.executemany(sql, suppliers)
                self.conn.commit()
                suppliers = []

        if suppliers:
            self.cursor.executemany(sql, suppliers)
            self.conn.commit()

    def generate_products(self):
        print("生成商品数据...")
        sql = """INSERT INTO product (product_id, product_name, category_id, supplier_id,
                price, stock, unit, description, popularity_level, is_seasonal,
                season_type, min_stock, max_stock)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        products = []
        units = ['个', '件', '箱', '千克', '米']
        
        # 生成商品热度分布
        popularity_levels = []
        for level, ratio in PRODUCT_POPULARITY.items():
            level_count = int(DATA_CONFIG['product_count'] * ratio)
            popularity_levels.extend([level] * level_count)
        
        # 补齐因四舍五入导致的差异
        while len(popularity_levels) < DATA_CONFIG['product_count']:
            popularity_levels.append('normal')
        
        random.shuffle(popularity_levels)
        
        for i in tqdm(range(DATA_CONFIG['product_count'])):
            product_id = self.generate_uuid()
            self.product_ids.append(product_id)
            
            category_id = random.choice(self.category_ids)
            supplier_id = random.choice(self.supplier_ids)
            
            # 获取类别的季节性信息
            self.cursor.execute("SELECT is_seasonal, season_type FROM category WHERE category_id = %s", (category_id,))
            category_info = self.cursor.fetchone()
            is_seasonal = category_info[0] if category_info[0] is not None else False
            season_type = category_info[1]
            
            # 设置商品热度
            popularity_level = popularity_levels[i]
            
            # 根据热度设置库存范围
            if popularity_level == 'hot':
                min_stock, max_stock = 500, 2000
            elif popularity_level == 'normal':
                min_stock, max_stock = 200, 1000
            else:
                min_stock, max_stock = 50, 200
            
            current_stock = random.randint(min_stock, max_stock)
            
            # 存储商品属性以供后续使用
            self.product_attributes[product_id] = {
                'popularity_level': popularity_level,
                'is_seasonal': is_seasonal,
                'season_type': season_type
            }
            
            products.append((
                product_id,
                f"{self.fake.word()}{random.choice(['手机', '电脑', '耳机', '手表', '相机'])}",
                category_id,
                supplier_id,
                round(random.uniform(10, 10000), 2),
                current_stock,
                random.choice(units),
                self.fake.text(max_nb_chars=200),
                popularity_level,
                is_seasonal,
                season_type,
                min_stock,
                max_stock
            ))

            if len(products) >= BATCH_SIZE:
                self.cursor.executemany(sql, products)
                self.conn.commit()
                products = []

        if products:
            self.cursor.executemany(sql, products)
            self.conn.commit()

    def calculate_order_amount(self, order_date, base_amount):
        """计算考虑节假日和促销活动的订单金额"""
        multiplier = 1.0
        order_source = 'normal'
        promotion_id = None
        
        # 检查是否是节假日
        for holiday_name, holiday_info in HOLIDAYS.items():
            holiday_date = datetime.strptime(holiday_info['date'], '%Y-%m-%d')
            holiday_end = holiday_date + timedelta(days=holiday_info['duration'])
            if holiday_date <= order_date <= holiday_end:
                multiplier = holiday_info['sales_boost']
                order_source = 'holiday'
                promotion_id = self.promotion_ids.get(holiday_name)
                break
        
        # 检查是否是促销活动
        for promotion_name, promotion_info in PROMOTIONS.items():
            promotion_start = datetime.strptime(promotion_info['start'], '%Y-%m-%d')
            promotion_end = promotion_start + timedelta(days=promotion_info['duration'])
            if promotion_start <= order_date <= promotion_end:
                multiplier = 1 / promotion_info['discount']  # 转换折扣为倍数
                order_source = 'promotion'
                promotion_id = self.promotion_ids.get(promotion_name)
                break
        
        actual_amount = round(base_amount * multiplier, 2)
        discount_amount = actual_amount - base_amount if actual_amount > base_amount else base_amount - actual_amount
        
        return actual_amount, discount_amount, order_source, promotion_id

    def generate_orders(self):
        print("生成订单数据...")
        order_sql = """INSERT INTO `order` (order_id, user_id, order_status, total_amount,
                    actual_amount, payment_method, shipping_address, shipping_phone,
                    shipping_name, region, city, order_source, promotion_id,
                    promotion_discount, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        detail_sql = """INSERT INTO order_detail (detail_id, order_id, product_id,
                    quantity, unit_price, original_price, total_price, discount_amount)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
        
        start_date = datetime.strptime(DATA_CONFIG['date_start'], '%Y-%m-%d')
        end_date = datetime.strptime(DATA_CONFIG['date_end'], '%Y-%m-%d')
        date_range = (end_date - start_date).days

        orders = []
        details = []
        
        # 根据用户行为分配订单数量
        user_order_counts = {}
        for user_id, behavior in self.user_behaviors.items():
            if behavior == 'frequent':
                order_count = random.randint(10, 20)
            elif behavior == 'regular':
                order_count = random.randint(5, 10)
            else:
                order_count = random.randint(1, 5)
            user_order_counts[user_id] = order_count
        
        total_orders = sum(user_order_counts.values())
        
        with tqdm(total=total_orders) as pbar:
            for user_id, order_count in user_order_counts.items():
                for _ in range(order_count):
                    order_id = self.generate_uuid()
                    order_date = start_date + timedelta(days=random.randint(0, date_range))
                    order_status = random.randint(0, 4)
                    payment_method = random.randint(1, 3)
                    
                    # 获取用户区域信息
                    self.cursor.execute("SELECT region, city FROM user WHERE user_id = %s", (user_id,))
                    user_region_info = self.cursor.fetchone()
                    region, city = user_region_info if user_region_info else self.get_random_region()
                    
                    # 生成订单详情
                    total_amount = 0
                    order_details = []
                    
                    # 根据用户行为和季节选择商品
                    available_products = [p_id for p_id, attrs in self.product_attributes.items()
                                       if attrs['popularity_level'] != 'cold' or random.random() < 0.2]
                    
                    order_products = random.sample(
                        available_products,
                        random.randint(1, DATA_CONFIG['max_order_details'])
                    )
                    
                    for product_id in order_products:
                        detail_id = self.generate_uuid()
                        quantity = random.randint(1, 5)
                        
                        # 考虑商品热度和季节性定价
                        base_price = round(random.uniform(10, 1000), 2)
                        product_attrs = self.product_attributes[product_id]
                        
                        # 热门商品溢价
                        if product_attrs['popularity_level'] == 'hot':
                            base_price *= 1.2
                        elif product_attrs['popularity_level'] == 'cold':
                            base_price *= 0.8
                        
                        # 季节性定价
                        if product_attrs['is_seasonal']:
                            current_month = order_date.month
                            for season, info in SEASONAL_PRODUCTS.items():
                                if current_month in info['months'] and product_attrs['season_type'] == season:
                                    base_price *= info['boost']
                                    break
                        
                        unit_price = round(base_price, 2)
                        total_price = round(quantity * unit_price, 2)
                        total_amount += total_price
                        
                        order_details.append((
                            detail_id, order_id, product_id, quantity,
                            unit_price, base_price, total_price, 0
                        ))
                    
                    # 计算订单最终金额（考虑节假日和促销）
                    actual_amount, discount_amount, order_source, promotion_id = self.calculate_order_amount(
                        order_date, total_amount
                    )
                    
                    orders.append((
                        order_id, user_id, order_status, total_amount, actual_amount,
                        payment_method, self.fake.address(), self.fake.phone_number(),
                        self.fake.name(), region, city, order_source, promotion_id,
                        discount_amount, order_date
                    ))
                    
                    details.extend(order_details)
                    
                    if len(orders) >= BATCH_SIZE:
                        self.cursor.executemany(order_sql, orders)
                        self.cursor.executemany(detail_sql, details)
                        self.conn.commit()
                        orders = []
                        details = []
                    
                    pbar.update(1)

        if orders:
            self.cursor.executemany(order_sql, orders)
            self.cursor.executemany(detail_sql, details)
            self.conn.commit()

    def generate_all_data(self):
        try:
            self.generate_promotions()
            self.generate_users()
            self.generate_categories()
            self.generate_suppliers()
            self.generate_products()
            self.generate_orders()
            print("所有数据生成完成！")
        except Exception as e:
            print(f"数据生成过程中发生错误: {e}")
        finally:
            if self.conn and self.conn.is_connected():
                self.cursor.close()
                self.conn.close()
                print("数据库连接已关闭")

if __name__ == "__main__":
    generator = DataGenerator()
    generator.generate_all_data() 