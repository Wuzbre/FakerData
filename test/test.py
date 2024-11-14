import random
import time
from datetime import datetime, timedelta
from faker import Faker
import pymysql
import csv


class DataSimulator:
    def __init__(self, db_config, start_time=None, end_time=None, frequency=1):
        self.db_config = db_config
        self.fake = Faker('zh_CN')  # 使用中文数据
        self.fake1 = Faker('en_US')
        self.start_time = start_time if start_time else datetime.now()
        self.end_time = end_time if end_time else self.start_time + timedelta(days=30)  # 默认30天数据
        self.frequency = frequency  # 数据生成频率，单位：秒
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

    def generate_user_data(self):
        created_at = self.fake.date_time_this_decade(self.start_time, self.end_time)
        email = self.fake.email()
        return {
            "user_id": None,  # 用户唯一标识
            "username": self.fake.user_name(),  # 用户名
            "email": email,  # 用户邮箱
            "phone": self.fake.phone_number(),  # 用户手机号
            "password": self.fake.password(),  # 密码
            "nickname": self.fake.first_name(),  # 用户昵称
            "avatar_url": self.fake.image_url(),  # 用户头像URL
            "status": random.choice(['active', 'deleted']),  # 账户状态
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

    def generate_employee_data(self):
        created_at = self.fake.date_time_this_decade(self.start_time, self.end_time)
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
            "hire_date": created_at,  # 入职日期
            "position": self.fake.job(),  # 职位
            "department": self.fake.bs(),  # 部门
            "employment_status": None,
            # random.choice(['active', 'inactive', 'resigned', 'on_leave', 'probation']),雇佣状态
            "work_status": random.choice(['full_time', 'part_time', 'intern']),  # 工作状态
            "probation_period_end": created_at + timedelta(days=90),  # 试用期结束日期
            "termination_date": None,  # 离职日期
            "resignation_reason": None,  # 离职原因
            "created_at": created_at,  # 创建时间
            "updated_at": None,  # 最后更新时间
            "last_login": None,  # 最后登录时间
        }

    def generate_product_data(self):
        brand = random.choice(self.brands)
        descriptor = random.choice(self.descriptors)
        category = random.choice(self.categories)
        product_name = f"{brand} {category} {descriptor}"
        price = round(random.uniform(10, 10000), 2)
        cost_price = round(random.uniform(5, price), 2)
        discount_price = price - cost_price
        create_at = self.fake.date_time_this_decade(self.start_time, self.end_time)
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
            "status": random.choice(['active', 'inactive', 'discontinued']),  # 产品状态
            "launch_date": create_at+timedelta(days=7),  # 上架日期
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

    def generate_purchase_order_data(self):
        return {
            "purchase_order_id": None,  # 采购订单唯一标识
            "supplier_id": random.randint(1, 100),  # 供应商ID，关联供应商表
            "order_date": self.fake.date_this_year(),  # 下单日期
            "expected_delivery_date": self.fake.date_this_year(),  # 预计交货日期
            "actual_delivery_date": None,  # 实际交货日期
            "status": random.choice(['pending', 'approved', 'shipped', 'received', 'completed', 'canceled']),  # 订单状态
            "total_amount": round(random.uniform(500, 50000), 2),  # 订单总金额
            "currency": random.choice(['CNY', 'USD', 'EUR']),  # 货币类型
            "payment_status": random.choice(['unpaid', 'paid', 'partial', 'overdue']),  # 支付状态
            "payment_method": self.fake.credit_card_provider(),  # 支付方式
            "payment_date": self.fake.date_this_year(),  # 支付日期
            "shipping_cost": round(random.uniform(0, 100), 2),  # 配送费用
            "warehouse_location": self.fake.address(),  # 存放仓库位置
            "created_by": random.randint(1, 100),  # 创建者ID
            "approved_by": random.randint(1, 100),  # 审批人ID
            "note": self.fake.text(),  # 备注
            "created_at": self.fake.date_time_this_decade(),  # 创建时间
            "updated_at": self.fake.date_time_this_decade()  # 最后更新时间
        }

    def generate_purchase_order_item_data(self, purchase_order_id):
        product_id = random.randint(1, 100)  # 假设产品ID在1到100之间
        quantity = random.randint(1, 50)  # 随机选择数量
        unit_price = round(random.uniform(10, 500), 2)  # 随机生成单价
        total_price = quantity * unit_price  # 计算总价
        received_quantity = 0  # 初始时，已接收数量为0
        status = random.choice(['pending', 'received', 'canceled'])  # 随机选择项目状态
        expected_delivery_date = self.fake.date_this_year()  # 预计交货日期
        actual_delivery_date = None  # 实际交货日期，默认值为None

        return {
            'purchase_order_item_id': None,  # 采购订单项ID由数据库自动生成
            'purchase_order_id': purchase_order_id,  # 关联到采购订单表的ID
            'product_id': product_id,  # 产品ID，关联到产品表
            'quantity': quantity,  # 数量
            'unit_price': unit_price,  # 单价
            'total_price': total_price,  # 总价（计算列）
            'received_quantity': received_quantity,  # 已接收数量
            'status': status,  # 项目状态
            'expected_delivery_date': expected_delivery_date,  # 项目预计交货日期
            'actual_delivery_date': actual_delivery_date  # 项目实际交货日期
        }

    def generate_sales_order_data(self, user_id):
        return {
            "sales_order_id": None,  # 销售订单唯一标识
            "user_id": random.randint(1, 100),  # 客户ID，关联到用户表
            "order_date": self.fake.date_this_year(),  # 下单日期
            "expected_delivery_date": self.fake.date_this_year(),  # 预计交货日期
            "actual_delivery_date": None,  # 实际交货日期
            "shipping_method": self.fake.word(),  # 配送方式
            "shipping_cost": round(random.uniform(0, 100), 2),  # 配送费用
            "shipping_address": self.fake.address(),  # 收货地址
            "billing_address": self.fake.address(),  # 账单地址
            "status": random.choice(['pending', 'paid', 'shipped', 'completed', 'returned', 'canceled']),  # 订单状态
            "total_amount": round(random.uniform(100, 10000), 2),  # 订单总金额
            "currency": random.choice(['CNY', 'USD', 'EUR']),  # 货币类型
            "discount": round(random.uniform(0, 500), 2),  # 订单总折扣
            "final_amount": None,  # 最终应支付金额
            "payment_status": random.choice(['unpaid', 'paid', 'refunded']),  # 支付状态
            "payment_method": self.fake.credit_card_provider(),  # 支付方式
            "payment_date": self.fake.date_this_year(),  # 支付日期
            "tracking_number": self.fake.bothify(text="??-#######"),  # 快递单号
            "created_by": random.randint(1, 100),  # 创建者ID
            "note": self.fake.text(),  # 备注
            "referral_code": self.fake.bothify(text='??-####'),  # 推荐码
            "created_at": self.fake.date_time_this_decade(),  # 创建时间
            "updated_at": self.fake.date_time_this_decade()  # 最后更新时间
        }

    def generate_sales_order_item_data(self, sales_order_id):
        product_id = random.randint(1, 100)  # 假设产品ID在1到100之间
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(20, 500), 2)
        total_price = quantity * unit_price
        discount = round(random.uniform(0, 50), 2)
        final_price = total_price - discount
        return {
            'sales_order_item_id': None,  # 销售订单项ID在数据库自动生成
            'sales_order_id': sales_order_id,  # 关联到销售订单表的ID
            'product_id': product_id,  # 产品ID，关联到产品表
            'quantity': quantity,  # 数量
            'unit_price': unit_price,  # 单价
            'total_price': total_price,  # 总价（计算列）
            'discount': discount,  # 项目折扣金额
            'final_price': final_price,  # 最终价格（计算列）
            'expected_delivery_date': self.fake.date_this_year(),  # 项目预计交货日期
            'actual_delivery_date': self.fake.date_this_year(),  # 项目实际交货日期
            'status': random.choice(['pending', 'shipped', 'delivered', 'returned']),  # 项目状态
            'warehouse_location': self.fake.city(),  # 发货仓库位置
        }

    def update_db_data(self):
        connection = pymysql.connect(**self.db_config)
        try:
            with connection.cursor() as cursor:
                table_to_update = random.choice(['users', 'employees', 'products'])
                if table_to_update == 'users':
                    cursor.execute(
                        "SELECT user_id, username FROM users WHERE status = 'active' ORDER BY RAND() LIMIT 1")
                    user = cursor.fetchone()
                    if user:
                        cursor.execute("UPDATE users SET email = %s, phone = %s WHERE user_id = %s",
                                       (self.fake.email(), self.fake.phone_number(), user['user_id']))
                elif table_to_update == 'employees':
                    cursor.execute(
                        "SELECT employee_id, employee_name FROM employees WHERE status = 'active' ORDER BY RAND() LIMIT 1")
                    employee = cursor.fetchone()
                    if employee:
                        cursor.execute("UPDATE employees SET salary = %s, position = %s WHERE employee_id = %s",
                                       (round(random.uniform(5000, 15000), 2), random.choice(['manager', 'assistant']),
                                        employee['employee_id']))
                elif table_to_update == 'products':
                    cursor.execute(
                        "SELECT product_id, product_name FROM products WHERE stock_status = 'in_stock' ORDER BY RAND() LIMIT 1")
                    product = cursor.fetchone()
                    if product:
                        cursor.execute("UPDATE products SET price = %s WHERE product_id = %s",
                                       (round(random.uniform(20, 1000), 2), product['product_id']))
            connection.commit()
        finally:
            connection.close()

    def save_to_file(self, data, filename="data.csv"):
        with open(filename, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=data[0].keys())
            writer.writeheader()
            for row in data:
                writer.writerow(row)

    def save_to_mysql(self, data, table_name):
        connection = pymysql.connect(**self.db_config)
        try:
            with connection.cursor() as cursor:
                for record in data:
                    keys = ', '.join(record.keys())
                    values = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in record.values()])
                    query = f"INSERT INTO {table_name} ({keys}) VALUES ({values})"
                    print(query)
                    cursor.execute(query)
            connection.commit()
        finally:
            connection.close()

    def simulate_data(self):
        current_time = self.start_time
        while current_time < self.end_time:
            # 生成数据并保存
            user_data = [self.generate_user_data() for _ in range(5)]  # 模拟5个用户
            employee_data = [self.generate_employee_data() for _ in range(5)]  # 模拟5个员工
            product_data = [self.generate_product_data() for _ in range(5)]  # 模拟5个产品

            # 插入用户数据并获得用户ID
            self.save_to_mysql(user_data, 'users')
            for user in user_data:
                sales_order_data = [self.generate_sales_order_data(user['user_id']) for _ in range(1)]  # 每个用户生成1个销售订单
                self.save_to_mysql(sales_order_data, 'sales_orders')

                for sales_order in sales_order_data:
                    sales_order_item_data = [self.generate_sales_order_item_data(sales_order['sales_order_id']) for _ in
                                             range(3)]  # 每个销售订单生成3个销售订单项
                    self.save_to_mysql(sales_order_item_data, 'sales_order_items')

            self.save_to_mysql(employee_data, 'employees')
            self.save_to_mysql(product_data, 'products')

            # 更新数据库中的数据
            self.update_db_data()

            # 等待下一次生成
            time.sleep(self.frequency)
            current_time += timedelta(seconds=self.frequency)


# 使用时的示例
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'erp',
    'charset': 'utf8mb4'
}

simulator = DataSimulator(db_config, start_time=datetime(2024, 11, 1), end_time=datetime(2024, 11, 30), frequency=10)
simulator.simulate_data()
