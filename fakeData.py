import random
import time
from datetime import datetime, timedelta
from faker import Faker
from dbutils.pooled_db import PooledDB
import pymysql
from typing import List, Dict, Any, Optional, Generator
import holidays
import json
import re
import logging
from pathlib import Path
from tqdm import tqdm
from config.data_config import *

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_generation.log'),
        logging.StreamHandler()
    ]
)

class DatabaseError(Exception):
    """数据库操作错误"""
    pass

class DataValidationError(Exception):
    """数据验证错误"""
    pass

class DataSimulator:
    """
    模拟生成业务数据的类。
    支持生成用户、员工、产品、订单等相关数据。

    Attributes:
        db_config (dict): 数据库配置信息
        start_time (datetime): 数据生成的开始时间
        end_time (datetime): 数据生成的结束时间
        frequency (int): 数据生成的频率(秒)
    """
    
    def __init__(self, db_config: Dict[str, Any], start_time: Optional[datetime] = None,
                 end_time: Optional[datetime] = None, frequency: int = 1):
        """
        初始化数据模拟器。
        
        Args:
            db_config: 数据库配置
            start_time: 开始时间
            end_time: 结束时间
            frequency: 生成频率
        """
        self.db_config = db_config
        self.fake = Faker('zh_CN')
        self.fake_en = Faker('en_US')  # 重命名为更清晰的名称
        self.start_time = start_time or datetime.now()
        self.end_time = end_time or (self.start_time + timedelta(days=30))
        self.frequency = frequency
        
        # 初始化数据库连接池
        self._init_db_pool()
        
        # 加载配置和断点信息
        self._load_config_data()
        self._load_checkpoint()
        
        # 初始化统计信息
        self.stats = {
            'generated': {k: 0 for k in DATA_GENERATION['base_rates'].keys()},
            'updated': {k: 0 for k in DATA_GENERATION['base_rates'].keys()},
            'errors': []
        }

        self.brands = [
            "华为", "小米", "苹果", "三星", "荣耀", "OPPO", "vivo",  # 电子品牌
            "耐克", "阿迪达斯", "彪马", "安踏", "李宁", "New Balance",  # 运动品牌
            "古驰", "路易威登", "香奈儿", "爱马仕", "迪奥", "普拉达",  # 奢侈品牌
            "ZARA", "H&M", "优衣库", "Gap", "Forever 21", "Levi's",  # 服装品牌
            "宜家", "苏宁", "京东", "天猫", "沃尔玛", "Target",  # 零售品牌
            "大众", "丰田", "宝马", "奔驰", "特斯拉", "奥迪",  # 汽车品牌
            "瑞士军刀", "凯迪拉克", "雪佛兰", "麦当劳",  # 一些混合品牌
        ]

        self.descriptors = [
            "Pro", "X", "Max", "Air", "Ultra", "Lite",  # 高端技术词汇
            "时尚", "经典", "奢华", "舒适", "流行", "运动", "高性能",  # 服饰、鞋类
            "智能", "迷你", "轻盈", "超薄", "高效", "坚固", "全新",  # 电子产品
            "环保", "健康", "高端", "艺术", "豪华", "限量版", "复古",  # 家居、配件
            "商务", "创新", "完美", "豪华", "优雅", "极致",  # 生活类商品
            "经典", "高街", "街头", "前卫", "运动风", "工装",  # 服装鞋类
            "透明", "可穿戴", "便捷", "耐用", "节能", "奢侈",  # 日常用品
            "全包围", "舒适性", "快速充电", "静音", "高清",  # 家电类
        ]

        self.categories = [
            "智能手机", "笔记本电脑", "无线耳机", "智能手表", "智能音响", "4K电视",  # 电子产品
            "运动鞋", "跑步鞋", "篮球鞋", "休闲鞋", "帆布鞋", "拖鞋",  # 鞋类
            "外套", "T恤", "衬衫", "牛仔裤", "裙子", "连衣裙", "羽绒服",  # 服装
            "运动服", "瑜伽裤", "运动内衣", "运动背心", "卫衣",  # 运动装备
            "背包", "手袋", "钱包", "皮带", "太阳镜", "帽子",  # 配饰
            "沙发", "床垫", "书桌", "餐桌", "椅子", "书架",  # 家居
            "茶具", "咖啡机", "厨房电器", "冰箱", "洗衣机", "微波炉",  # 家电
            "化妆品", "护肤品", "香水", "面膜", "口红", "睫毛膏",  # 美妆
            "保健品", "营养补充剂", "减肥药", "维生素", "蛋白粉",  # 健康产品
            "游戏机", "手柄", "桌游", "拼图", "积木", "电子书",  # 玩具、游戏
            "零食", "饮料", "巧克力", "糖果", "果汁", "即食食品",  # 食品
            "汽车", "电动滑板车", "摩托车", "自行车",  # 交通工具
        ]
        self.holiday_calendar = holidays.China()

    def is_holiday(self, date):
        """判断是否为节假日"""
        return date in self.holiday_calendar

    def generate_user_data(self, current_date):
        created_at = self.fake.date_time_between(current_date, current_date + timedelta(days=1))
        # strftime("%Y-%m-%d")
        email = self.fake.email()
        return {
            "user_id": None,  # 用户唯一标识
            "username": self.fake.user_name(),  # 用户名
            "email": email,  # 用户邮箱
            "phone": self.fake.phone_number(),  # 用户手机号
            "password": self.fake.password(),  # 密码
            "nickname": self.fake.first_name(),  # 用户昵称
            "avatar_url": self.fake.image_url(),  # 用户头像URL
            # "status": random.choice(['active', 'deleted']),  # 账户状态
            "is_verified": random.choice([True, False]),  # 邮箱/手机号是否验证
            "role": self.fake.job(),  # 用户角色
            "created_at": created_at,  # 账户创建时间
            "updated_at": None,  # 最后更新时间
            "last_login": None,  # 最后登录时间
            "account_balance": None,  # 账户余额
            "points_balance": None,  # 积分余额
            "membership_level": None,  # 会员等级
            "failed_attempts": None,  # 登录失败次数
            "lock_until": None,  # 锁定时间
            "two_factor_enabled": random.choice([True, False]),  # 是否启用双因素认证
            "preferred_language": self.fake.language_name(),  # 用户语言偏好
            "preferred_currency": random.choice(['CNY', 'USD', 'EUR']),  # 用户货币偏好
            "shipping_address": self.fake.address(),  # 收货地址
            "billing_address": email,  # 账单地址
            "newsletter_subscribed": random.choice([True, False]),  # 是否订阅邮件
            "referral_code": self.fake.bothify(text='??-####'),  # 推荐码
            "referred_by_user_id": None,  # 推荐用户ID
            "cart_id": None,  # 购物车ID
            "order_count": None,  # 订单数量
            "order_total": None  # 累计消费总额
        }

    def generate_employee_data(self, current_date):
        created_at = self.fake.date_time_between(current_date, current_date + timedelta(days=1))
        hire_date = created_at.strftime("%Y-%m-%d")
        probation_period_end = (created_at + timedelta(days=90)).strftime("%Y-%m-%d")
        return {
            "employee_id": None,  # 员工唯一标识
            "first_name": self.fake.first_name(),  # 员工名字
            "last_name": self.fake.last_name(),  # 员工姓氏
            "gender": random.choice(['male', 'female', 'other']),  # 性别
            "birth_date": self.fake.date_of_birth(minimum_age=15, maximum_age=70),  # 出生日期
            "email": self.fake.company_email(),  # 邮箱地址
            "phone": self.fake.phone_number(),  # 手机号
            "address": self.fake.address(),  # 员工住址
            "emergency_contact": self.fake.name(),  # 紧急联系人
            "hire_date": hire_date,  # 入职日期
            "position": self.fake.job(),  # 职位
            "department": self.fake.bs(),  # 部门
            # "employment_status": None,
            # random.choice(['active', 'inactive', 'resigned', 'on_leave', 'probation']),雇佣状态
            "work_status": random.choice(['full_time', 'part_time', 'intern']),  # 工作状态
            "probation_period_end": probation_period_end,  # 试用期结束日期
            "termination_date": None,  # 离职日期
            "resignation_reason": None,  # 离职原因
            "created_at": created_at,  # 创建时间
            "updated_at": None,  # 最后更新时间
            "last_login": None,  # 最后登录时间
        }

    def generate_product_data(self, current_date):
        brand = random.choice(self.brands)
        descriptor = random.choice(self.descriptors)
        category = random.choice(self.categories)
        product_name = f"{brand} {category} {descriptor}"
        price = round(random.uniform(10, 10000), 2)
        cost_price = round(random.uniform(5, price), 2)
        discount_price = round(price - cost_price, 2)
        create_at = self.fake.date_time_between(current_date, current_date + timedelta(days=1))
        launch_date = (create_at + timedelta(days=7)).strftime("%Y-%m-%d")
        return {
            "product_id": None,  # 产品唯一标识
            "product_name": product_name,  # 产品名称
            "product_description": self.fake.text(),  # 产品描述
            "sku": self.fake.bothify(text="???-#####"),  # 产品SKU
            "category_id": random.randint(1, 20),  # 产品分类ID，关联到分类表
            "brand": brand,  # 品牌名称
            "model": self.fake.bothify(text="Model-###"),  # 产品型号或系列
            "color": self.fake.color_name(),  # 产品颜色
            "price": price,  # 销售价格
            "cost_price": cost_price,  # 成本价格
            "discount_price": discount_price,  # 折扣价格
            "currency": random.choice(['CNY', 'USD', 'EUR']),  # 货币类型
            # "status": random.choice(['active', 'inactive', 'discontinued']),  # 产品状态
            "launch_date": launch_date,  # 上架日期
            "discontinued_date": None,  # 停产日期
            "supplier_id": random.randint(1, 100),  # 供应商ID
            "manufacturer": self.fake.company(),  # 制造商名称
            "country_of_origin": self.fake.country(),  # 生产国家
            "image_url": self.fake.image_url(),  # 主图URL
            "additional_images": ','.join([self.fake.image_url() for _ in range(3)]),  # 其他图片URL（多个以逗号分隔）
            "weight": round(random.uniform(0.5, 10), 2),  # 产品重量
            "dimensions": f"{random.randint(10, 50)}x{random.randint(10, 50)}x{random.randint(1, 20)}",  # 产品尺寸
            "warranty": self.fake.sentence(),  # 保修期
            "created_at": create_at,  # 创建时间
            "updated_at": None  # 最后更新时间
        }

    def generate_purchase_order_data(self, current_date):
        # print(self.query_from_db("SELECT employee_id FROM employees"))
        created_by = random.choices(self.query_from_db("SELECT employee_id FROM employees"))
        approved_by = random.choices(self.query_from_db("SELECT employee_id FROM employees"))
        order_date = self.fake.date_time_between(current_date, current_date + timedelta(days=1))
        expected_delivery_date = order_date + timedelta(days=self.fake.random_int(10, 100))
        # actual_delivery_date = expected_delivery_date - timedelta(days=self.fake.random_int(-10, 80))
        payment_date = order_date + timedelta(days=self.fake.random_int(10, 20))
        return {
            "purchase_order_id": None,  # 采购订单唯一标识
            "supplier_id": random.randint(1, 100),  # 供应商ID，关联供应商表
            "order_date": order_date,  # 下单日期
            "expected_delivery_date": expected_delivery_date,  # 预计交货日期
            "actual_delivery_date": None,  # 实际交货日期
            "status": random.choice(['pending', 'approved', 'shipped', 'received', 'completed', 'canceled']),  # 订单状态
            "total_amount": round(random.uniform(500, 50000), 2),  # 订单总金额
            "currency": random.choice(['CNY', 'USD', 'EUR']),  # 货币类型
            "payment_status": random.choice(['unpaid', 'paid', 'partial', 'overdue']),  # 支付状态
            "payment_method": self.fake.credit_card_provider(),  # 支付方式
            "payment_date": payment_date,  # 支付日期
            "shipping_cost": round(random.uniform(0, 100), 2),  # 配送费用
            "warehouse_location": self.fake.address(),  # 存放仓库位置
            "created_by": created_by,  # 创建者ID
            "approved_by": approved_by,  # 审批人ID
            "note": self.fake.text(),  # 备注
            "created_at": order_date,  # 创建时间
            "updated_at": None  # 最后更新时间
        }

    def generate_purchase_order_item_data(self, current_date):
        quantity = random.randint(1, 50)  # 随机选择数量
        unit_price = round(random.uniform(10, 500), 2)  # 随机生成单价
        total_price = quantity * unit_price  # 计算总价
        received_quantity = 0  # 初始时，已接收数量为0
        status = random.choice(['pending', 'received', 'canceled'])  # 随机选择项目状态
        create = self.fake.date_time_between(current_date, current_date + timedelta(days=1))
        expected_delivery_date = (create + timedelta(days=self.fake.random_int(10, 100))).strftime("%Y-%m-%d")  # 预计交货日期
        actual_delivery_date = None  # 实际交货日期，默认值为None

        purchase_order_id = random.choices(
            self.query_from_db("SELECT purchase_order_id FROM purchase_orders"))
        product_id = random.choices(self.query_from_db("SELECT product_id FROM products"))
        return {
            "purchase_order_item_id": None,  # 采购订单项ID由数据库自动生成
            "purchase_order_id": purchase_order_id,  # 关联到采购订单表的ID
            "product_id": product_id,  # 产品ID，关联到产品表
            "quantity": quantity,  # 数量
            "unit_price": unit_price,  # 单价
            "total_price": total_price,  # 总价（计算列）
            "received_quantity": received_quantity,  # 已接收数量(废弃)
            "status": status,  # 项目状态
            "expected_delivery_date": expected_delivery_date,  # 项目预计交货日期
            "actual_delivery_date": actual_delivery_date  # 项目实际交货日期
        }

    def generate_sales_order_data(self, current_date):
        user_id = random.choices(self.query_from_db("SELECT user_id FROM users"))
        created_by = random.choices(self.query_from_db("SELECT employee_id FROM employees"))
        order_date = self.fake.date_time_between(current_date, current_date + timedelta(days=1))
        expected_delivery_date = order_date + timedelta(days=self.fake.random_int(10, 100))
        total_amount = round(random.uniform(100, 10000), 2)
        discount = round(total_amount - round(random.uniform(0, total_amount), 2), 2)
        final_amount = round(total_amount - discount, 2)
        return {
            "sales_order_id": None,  # 销售订单唯一标识
            "user_id": user_id,  # 客户ID，关联到用户表
            "order_date": order_date,  # 下单日期
            "expected_delivery_date": expected_delivery_date,  # 预计交货日期
            "actual_delivery_date": None,  # 实际交货日期
            "shipping_method": self.fake.word(),  # 配送方式
            "shipping_cost": round(random.uniform(0, 100), 2),  # 配送费用
            "shipping_address": self.fake.address(),  # 收货地址
            "billing_address": self.fake.address(),  # 账单地址
            "status": random.choice(['pending', 'paid', 'shipped', 'completed', 'returned', 'canceled']),  # 订单状态
            "total_amount": total_amount,  # 订单总金额
            "currency": random.choice(['CNY', 'USD', 'EUR']),  # 货币类型
            "discount": discount,  # 订单总折扣
            "final_amount": final_amount,  # 最终应支付金额
            "payment_status": random.choice(['unpaid', 'paid', 'refunded']),  # 支付状态
            "payment_method": self.fake.credit_card_provider(),  # 支付方式
            "payment_date": order_date,  # 支付日期
            "tracking_number": self.fake.bothify(text="??-#######"),  # 快递单号
            "created_by": created_by,  # 创建者ID
            "note": self.fake.text(),  # 备注
            "referral_code": self.fake.bothify(text='??-####'),  # 推荐码
            "created_at": order_date,  # 创建时间
            "updated_at": None  # 最后更新时间
        }

    def generate_sales_order_item_data(self, current_date):
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(20, 500), 2)
        total_price = quantity * unit_price
        discount = round(random.uniform(0, 50), 2)
        final_price = round(total_price - discount, 2)
        expected_delivery_date = self.fake.date_time_between(current_date,
                                                             current_date + timedelta(days=1)) + timedelta(
            days=random.randint(1, 10))
        sales_order_id = random.choices(
            self.query_from_db("SELECT sales_order_id FROM sales_orders"))
        product_id = random.choices(self.query_from_db("SELECT product_id FROM products"))

        return {
            "sales_order_item_id": None,  # 销售订单项ID在数据库自动生成
            "sales_order_id": sales_order_id,  # 关联到销售订单表的ID
            "product_id": product_id,  # 产品ID，关联到产品表
            "quantity": quantity,  # 数量
            "unit_price": unit_price,  # 单价
            "total_price": total_price,  # 总价（计算列）
            "discount": discount,  # 项目折扣金额
            "final_price": final_price,  # 最终价格（计算列）
            "expected_delivery_date": expected_delivery_date,  # 项目预计交货日期
            "actual_delivery_date": None,  # 项目实际交货日期
            "status": random.choice(['pending', 'shipped', 'delivered', 'returned']),  # 项目状态
            "warehouse_location": self.fake.city(),  # 发货仓库位置
        }

    def connect_to_db(self):
        return self.pool.connection()


    def query_from_db(self, query: str) -> List[Any]:
        """
        执行数据库查询。
        
        Args:
            query: SQL查询语句
            
        Returns:
            查询结果列表
            
        Raises:
            DatabaseError: 数据库查询失败时抛出
        """
        connection = None
        try:
            connection = self.pool.connection()
            with connection.cursor(cursor=pymysql.cursors.Cursor) as cursor:
                cursor.execute(query)
                return [row[0] for row in cursor.fetchall()]
        except pymysql.Error as e:
            raise DatabaseError(f"Database query failed: {e}")
        finally:
            if connection:
                connection.close()

    def save_to_mysql(self, data: List[Dict[str, Any]], table_name: str) -> None:
        """
        批量保存数据到MySQL。
        
        Args:
            data: 要保存的数据列表
            table_name: 表名
            
        Raises:
            DatabaseError: 数据保存失败时抛出
        """
        if not data:
            return
            
        connection = None
        try:
            connection = self.pool.connection()
            with connection.cursor() as cursor:
                keys = data[0].keys()
                columns = ', '.join(keys)
                placeholders = ', '.join(['%s'] * len(keys))
                values = [tuple(record[key] for key in keys) for record in data]
                
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                cursor.executemany(query, values)
                
            connection.commit()
        except pymysql.Error as e:
            if connection:
                connection.rollback()
            raise DatabaseError(f"Failed to save data to {table_name}: {e}")
        finally:
            if connection:
                connection.close()

    def update_mysql(self, updates: List[Dict[str, Any]], table_name: str, primary_key: str):
        """Update data in a MySQL table."""
        if not updates:
            return
        connection = self.connect_to_db()
        try:
            with connection.cursor() as cursor:
                for update in updates:
                    set_clause = ', '.join([f"{key} = %s" for key in update.keys() if key != primary_key])
                    query = f"UPDATE {table_name} SET {set_clause}, updated_at = %s WHERE {primary_key} = %s"
                    values = tuple(update[key] for key in update.keys() if key != primary_key)
                    values += (datetime.now(), update[primary_key])
                    cursor.execute(query, values)
            connection.commit()
        except pymysql.MySQLError as e:
            print(f"Error while updating {table_name}: {e}")
        finally:
            connection.close()

    def generate_daily_data(self, current_date: datetime) -> None:
        """
        生成每日数据。
        
        Args:
            current_date: 当前日期
        """
        try:
            # 计算增长数量
            growth_rates = self._calculate_growth_rates(current_date)
            
            # 生成基础数据
            data_generators = {
                'users': (self.generate_user_data, growth_rates['user']),
                'employees': (self.generate_employee_data, growth_rates['employee']),
                'products': (self.generate_product_data, growth_rates['product']),
                'purchase_orders': (self.generate_purchase_order_data, growth_rates['purchase_order']),
                'sales_orders': (self.generate_sales_order_data, growth_rates['sales_order'])
            }
            
            # 批量生成并保存数据
            for table_name, (generator, count) in data_generators.items():
                data = [generator(current_date) for _ in range(count)]
                self.save_to_mysql(data, table_name)
                
            # 生成关联数据
            self._generate_related_data(current_date, growth_rates)
            
            # 更新现有数据
            self._update_existing_data(current_date)
            
            print(f"Successfully generated data for {current_date.strftime('%Y-%m-%d')}")
            
        except Exception as e:
            print(f"Error generating data for {current_date}: {e}")
            # 可以添加重试逻辑或告警机制

    def _calculate_growth_rates(self, current_date: datetime) -> Dict[str, int]:
        """计算各类数据的增长率"""
        is_holiday = self.is_holiday(current_date)
        base_rates = {
            'user': 500,
            'employee': 1,
            'product': 40,
            'purchase_order': 300,
            'sales_order': 600
        }
        
        # 节假日调整
        if is_holiday:
            return {k: v // 2 for k, v in base_rates.items()}
        return base_rates

    def _load_checkpoint(self) -> None:
        """加载断点信息"""
        checkpoint_file = Path('checkpoint.json')
        if checkpoint_file.exists():
            with open(checkpoint_file, 'r') as f:
                checkpoint = json.load(f)
                self.start_time = datetime.fromisoformat(checkpoint['last_date'])
                logging.info(f"Resuming from checkpoint: {self.start_time}")

    def _save_checkpoint(self, current_date: datetime) -> None:
        """保存断点信息"""
        with open('checkpoint.json', 'w') as f:
            json.dump({
                'last_date': current_date.isoformat(),
                'stats': self.stats
            }, f)

    def _validate_data(self, data: Dict[str, Any], data_type: str) -> bool:
        """
        验证数据是否符合规则
        
        Args:
            data: 要验证的数据
            data_type: 数据类型（user/product等）
            
        Returns:
            bool: 验证是否通过
            
        Raises:
            DataValidationError: 数据验证失败时抛出
        """
        rules = VALIDATION_RULES.get(data_type, {})
        
        try:
            if data_type == 'user':
                if not re.match(rules['email'], data['email']):
                    raise DataValidationError(f"Invalid email: {data['email']}")
                if not re.match(rules['phone'], data['phone']):
                    raise DataValidationError(f"Invalid phone: {data['phone']}")
                if len(data['password']) < rules['password_min_length']:
                    raise DataValidationError("Password too short")
                    
            elif data_type == 'product':
                if not rules['price_min'] <= data['price'] <= rules['price_max']:
                    raise DataValidationError(f"Price out of range: {data['price']}")
                    
            return True
            
        except DataValidationError as e:
            self.stats['errors'].append(str(e))
            logging.warning(f"Data validation failed: {e}")
            return False

    def _generate_related_data(self, current_date: datetime, growth_rates: Dict[str, int]) -> None:
        """生成关联数据"""
        # 生成订单项数据
        for order_type in ['purchase_orders', 'sales_orders']:
            base_count = growth_rates[order_type.rstrip('s')]
            items_count = base_count * random.randint(1, 5)
            
            generator = (
                self.generate_purchase_order_item_data if 'purchase' in order_type
                else self.generate_sales_order_item_data
            )
            
            items_data = [generator(current_date) for _ in range(items_count)]
            self.save_to_mysql(items_data, f"{order_type.rstrip('s')}_items")
            
            self.stats['generated'][f"{order_type.rstrip('s')}_items"] = items_count

    def _update_existing_data(self, current_date: datetime) -> None:
        """更新现有数据"""
        for table, id_field in [
            ('users', 'user_id'),
            ('employees', 'employee_id'),
            ('products', 'product_id')
        ]:
            ids = self.query_from_db(f"SELECT {id_field} FROM {table} WHERE status <> 'deleted'")
            if not ids:
                continue
                
            update_count = min(len(ids), max(1, len(ids) * random.randint(1, 5) // 100))
            updates = self._generate_updates(table, ids, update_count, current_date)
            
            self.update_mysql(updates, table, id_field)
            self.stats['updated'][table] = update_count

    def simulate_data(self) -> None:
        """模拟生成数据的主函数"""
        current_date = self.start_time
        total_days = (self.end_time - self.start_time).days
        
        try:
            with tqdm(total=total_days, desc="Generating data") as pbar:
                while current_date <= self.end_time:
                    self.generate_daily_data(current_date)
                    self._save_checkpoint(current_date)
                    
                    current_date += timedelta(days=1)
                    pbar.update(1)
                    time.sleep(self.frequency)
                    
            # 输出最终统计信息
            self._print_stats()
                    
        except KeyboardInterrupt:
            logging.info("\nData generation interrupted by user")
            self._save_checkpoint(current_date)
        except Exception as e:
            logging.error(f"Error during data simulation: {e}")
            self._save_checkpoint(current_date)
        finally:
            if hasattr(self, 'pool'):
                self.pool.close()

    def _print_stats(self) -> None:
        """打印统计信息"""
        logging.info("\nData Generation Statistics:")
        logging.info("\nGenerated Records:")
        for table, count in self.stats['generated'].items():
            logging.info(f"  {table}: {count:,}")
            
        logging.info("\nUpdated Records:")
        for table, count in self.stats['updated'].items():
            logging.info(f"  {table}: {count:,}")
            
        if self.stats['errors']:
            logging.info("\nErrors encountered:")
            for error in self.stats['errors'][:10]:  # 只显示前10个错误
                logging.info(f"  - {error}")
            if len(self.stats['errors']) > 10:
                logging.info(f"  ... and {len(self.stats['errors']) - 10} more errors")

    def _init_db_pool(self) -> None:
        """初始化数据库连接池"""
        try:
            self.pool = PooledDB(
                creator=pymysql,
                maxconnections=10,
                mincached=2,
                maxcached=5,
                blocking=True,
                maxshared=3,
                setsession=[],
                **self.db_config
            )
        except Exception as e:
            raise RuntimeError(f"Failed to initialize database pool: {e}")

    def _load_config_data(self) -> None:
        """加载配置数据到内存"""
        # 将原来的品牌、描述词等数据移到单独的配置文件或数据库中
        self.brands = self._load_data_from_config('brands')
        self.descriptors = self._load_data_from_config('descriptors')
        self.categories = self._load_data_from_config('categories')
        self.holiday_calendar = holidays.China()

    @staticmethod
    def _load_data_from_config(key: str) -> List[str]:
        """
        从配置文件加载数据
        
        Args:
            key: 配置键名
            
        Returns:
            配置数据列表
        """
        try:
            return globals()[key.upper()]
        except KeyError:
            logging.warning(f"Configuration key {key} not found, using empty list")
            return []

    def _generate_updates(self, table: str, ids: List[int], count: int, current_date: datetime) -> List[Dict[str, Any]]:
        """
        生成数据更新
        
        Args:
            table: 表名
            ids: ID列表
            count: 更新数量
            current_date: 当前日期
            
        Returns:
            更新数据列表
        """
        updates = []
        selected_ids = random.sample(ids, min(count, len(ids)))
        
        for id_value in selected_ids:
            if table == 'users':
                update = {
                    'user_id': id_value,
                    'nickname': self.fake.first_name(),
                    'email': self.fake.email(),
                    'status': random.choice(["deleted", "active"]),
                    'last_login': self.fake.date_time_between(
                        current_date,
                        current_date + timedelta(days=1)
                    )
                }
            elif table == 'employees':
                update = {
                    'employee_id': id_value,
                    'address': self.fake.address(),
                    'phone': self.fake.phone_number(),
                    'employment_status': random.choice(['active', 'inactive', 'resigned', 'on_leave', 'probation']),
                    'last_login': self.fake.date_time_between(
                        current_date,
                        current_date + timedelta(days=1)
                    )
                }
            elif table == 'products':
                update = {
                    'product_id': id_value,
                    'price': round(random.uniform(10, 10000), 2),
                    'cost_price': round(random.uniform(5, 1000), 2),
                    'discount_price': round(random.uniform(5, 500), 2),
                    'status': random.choice(['active', 'inactive', 'discontinued'])
                }
            
            if self._validate_data(update, table.rstrip('s')):
                updates.append(update)
            
        return updates
