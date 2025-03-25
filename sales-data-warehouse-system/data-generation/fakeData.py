#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
购销数据仓库系统 - 数据生成模块
生成模拟的用户、产品、订单、销售和库存数据，作为数据仓库的数据源
"""

import argparse
import random
import time
import datetime
import os
import sys
from datetime import timedelta, datetime
import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np
from faker import Faker
from tqdm import tqdm
from loguru import logger
from dbutils.pooled_db import PooledDB
import pymysql
from typing import List, Dict, Any, Optional, Generator
import holidays
import json
import re
import logging
from pathlib import Path
import yaml
from chinese_calendar import is_holiday

# 设置项目根目录
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PROJECT_ROOT)

# 导入配置
from config.data_config import (
    BRANDS, DESCRIPTORS, CATEGORIES, PRICE_RANGES,
    USER_TYPE_DISTRIBUTION, USER_AGE_DISTRIBUTION,
    REGION_DISTRIBUTION, REGIONS, PAYMENT_METHODS,
    CREATE_TABLES_SQL, DIMENSION_TABLES,
    DIMENSION_DATA_GENERATION, ACTIVITY_CONFIG,
    ACTIVITY_DATES, ORDER_STATUS, ORDER_QUANTITY_RANGE,
    SEASONAL_FACTORS, INVENTORY_TURNOVER_REFERENCE,
    USER_PURCHASE_FREQUENCY
)
from config.db_config import DB_CONFIG
from config.log_config import LOG_CONFIG

# 设置日志
logger.remove()
logger.add(sys.stderr, level="INFO")
logger.add("data_generation.log", rotation="10 MB", level="DEBUG")

# 初始化Faker
fake = Faker('zh_CN')
Faker.seed(42)  # 设置随机种子，确保可重复性
random.seed(42)
np.random.seed(42)

class Config:
    """配置管理类"""
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()
        self._validate_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            raise
    
    def _validate_config(self):
        """验证配置有效性"""
        required_sections = ['database', 'generation', 'performance', 'logging']
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"配置文件缺少必要的 {section} 部分")
        
        # 验证数据库配置
        db_config = self.config['database']
        required_db_fields = ['host', 'port', 'user', 'password', 'database']
        for field in required_db_fields:
            if field not in db_config:
                raise ValueError(f"数据库配置缺少必要的 {field} 字段")
        
        # 验证生成配置
        gen_config = self.config['generation']
        required_gen_fields = ['base_metrics', 'update_ratios', 'price_changes']
        for field in required_gen_fields:
            if field not in gen_config:
                raise ValueError(f"生成配置缺少必要的 {field} 字段")
    
    def get(self, *keys: str, default: Any = None) -> Any:
        """获取配置值"""
        value = self.config
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key, default)
            else:
                return default
        return value

def connect_to_database():
    """连接到MySQL数据库"""
    try:
        connection = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            port=DB_CONFIG['port']
        )
        
        if connection.is_connected():
            db_info = connection.get_server_info()
            logger.info(f"已连接到MySQL服务器版本: {db_info}")
            
            # 创建数据库（如果不存在）
            cursor = connection.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
            cursor.execute(f"USE {DB_CONFIG['database']}")
            logger.info(f"使用数据库: {DB_CONFIG['database']}")
            
            # 创建表
            for create_table_sql in CREATE_TABLES_SQL:
                cursor.execute(create_table_sql)
            connection.commit()
            logger.info("数据库表结构已创建或已存在")
            
            return connection
            
    except Error as e:
        logger.error(f"连接数据库时发生错误: {e}")
        return None

def clear_database(connection):
    """清空数据库中的所有表"""
    try:
        cursor = connection.cursor()
        
        # 临时禁用外键约束
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        # 获取所有表名
        cursor.execute(f"USE {DB_CONFIG['database']}")
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        
        # 清空所有表
        for table in tables:
            cursor.execute(f"TRUNCATE TABLE {table}")
            logger.info(f"表 {table} 已清空")
        
        # 重新启用外键约束
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        connection.commit()
        logger.info("所有表已清空")
    
    except Error as e:
        logger.error(f"清空数据库时发生错误: {e}")
        connection.rollback()

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
        config (Config): 配置对象
        start_time (datetime): 数据生成的开始时间
        end_time (datetime): 数据生成的结束时间
        scale_factor (float): 数据量缩放因子
    """
    
    def __init__(self, config: Config, start_time: datetime,
                 end_time: datetime, scale_factor: float = 0.01):
        """
        初始化数据模拟器
        
        Args:
            config: 配置对象
            start_time: 开始时间
            end_time: 结束时间
            scale_factor: 数据量缩放因子
        """
        self.config = config
        self.start_time = start_time
        self.end_time = end_time
        self.scale_factor = scale_factor
        
        # 初始化数据库连接池
        self._init_db_pool()
        
        # 设置每日数据量基准
        self.daily_metrics = {
            k: int(v * scale_factor)
            for k, v in self.config.get('generation', 'base_metrics').items()
        }
        
        # 初始化统计信息
        self.stats = {
            'generated': {k: 0 for k in self.daily_metrics.keys()},
            'updated': {k: 0 for k in self.daily_metrics.keys()},
            'errors': []
        }

        # 初始化Faker
        self.fake = Faker(['zh_CN'])
        
        # 设置随机种子
        random.seed(int(time.time()))

    def _init_db_pool(self):
        """初始化数据库连接池"""
        try:
            db_config = self.config.get('database')
            pool_config = {
                'pool_name': 'mypool',
                'pool_size': db_config.get('pool_size', 5),
                'host': db_config.get('host'),
                'port': db_config.get('port'),
                'user': db_config.get('user'),
                'password': db_config.get('password'),
                'database': db_config.get('database'),
                'charset': db_config.get('charset', 'utf8mb4')
            }
            self.pool = pooling.MySQLConnectionPool(**pool_config)
            logger.info("数据库连接池初始化成功")
        except Error as e:
            logger.error(f"初始化数据库连接池失败: {e}")
            raise

    def _get_connection(self) -> Optional[pooling.MySQLConnection]:
        """获取数据库连接"""
        try:
            return self.pool.get_connection()
        except Error as e:
            logger.error(f"获取数据库连接失败: {e}")
            return None

    def _batch_insert(self, connection, table: str, data: List[Dict[str, Any]]):
        """批量插入数据"""
        if not data:
            return
            
        cursor = connection.cursor()
        columns = data[0].keys()
        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # 分批插入数据
        batch_size = self.config.get('performance', 'batch_size', default=1000)
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            values = [tuple(record[col] for col in columns) for record in batch]
            try:
                cursor.executemany(query, values)
                connection.commit()
            except Error as e:
                connection.rollback()
                logger.error(f"批量插入数据失败: {e}")
                raise

    def _update_products(self, connection, current_date):
        """更新商品信息"""
        try:
            cursor = connection.cursor()
            
            # 获取要更新的商品
            update_ratio = self.config.get('generation', 'update_ratios', 'products', default=0.15)
            cursor.execute("""
                SELECT product_id, price, category, subcategory 
                FROM products 
                WHERE is_active = TRUE 
                ORDER BY RAND() 
                LIMIT %s
            """, (int(self.daily_metrics['products'] * update_ratio),))
            
            products_to_update = cursor.fetchall()
            
            for product_id, current_price, category, subcategory in products_to_update:
                # 生成更新时间和更新内容
                update_time = self.fake.date_time_between(current_date, current_date + timedelta(days=1))
                
                # 根据商品类别调整价格范围
                price_changes = self.config.get('generation', 'price_changes')
                if category == '电子产品':
                    price_change = random.uniform(-price_changes['electronics'], price_changes['electronics'])
                elif category == '服装':
                    price_change = random.uniform(-price_changes['clothing'], price_changes['clothing'])
                else:
                    price_change = random.uniform(-price_changes['others'], price_changes['others'])
                
                new_price = round(current_price * (1 + price_change), 2)
                
                # 更新商品信息
                update_sql = """
                UPDATE products 
                SET price = %s, 
                    description = %s,
                    weight = %s,
                    dimensions = %s,
                    updated_at = %s
                WHERE product_id = %s
                """
                
                # 根据商品类别生成新的重量和尺寸
                if category == '电子产品':
                    weight = round(random.uniform(0.2, 15.0), 2)
                    dimensions = f"{random.randint(10, 100)}x{random.randint(10, 100)}x{random.randint(5, 50)}"
                elif category == '服装':
                    weight = round(random.uniform(0.1, 2.0), 2)
                    dimensions = random.choice(['S', 'M', 'L', 'XL', 'XXL'])
                else:
                    weight = round(random.uniform(0.05, 5.0), 2)
                    dimensions = f"{random.randint(5, 30)}x{random.randint(5, 30)}x{random.randint(2, 20)}"
                
                cursor.execute(update_sql, (
                    new_price,
                    self.fake.paragraph(),
                    weight,
                    dimensions,
                    update_time,
                    product_id
                ))
            
            connection.commit()
            logger.info(f"成功更新 {len(products_to_update)} 个商品信息")
            
        except Error as e:
            logger.error(f"更新商品信息时发生错误: {e}")
            connection.rollback()

    def _adjust_data_volume(self, connection, current_date):
        """根据节假日和季节调整数据量"""
        # 获取当前日期是否为节假日
        is_holiday = is_holiday(current_date.date())
        
        # 获取当前季节
        month = current_date.month
        season = (month - 1) // 3 + 1
        
        # 调整因子
        holiday_factor = 1.0
        season_factor = 1.0
        day_of_week_factor = 1.0
        
        # 节假日调整
        holiday_factors = self.config.get('generation', 'holiday_factors')
        if is_holiday:
            holiday_factor = holiday_factors['holiday']
        elif is_holiday(current_date.date() + timedelta(days=1)):
            holiday_factor = holiday_factors['before']
        elif is_holiday(current_date.date() - timedelta(days=1)):
            holiday_factor = holiday_factors['after']
        
        # 特殊节日调整
        special_dates = holiday_factors.get('special', {})
        date_key = f"{month:02d}-{current_date.day:02d}"
        if date_key in special_dates:
            holiday_factor = special_dates[date_key]
        
        # 工作日调整
        weekday_factors = self.config.get('generation', 'weekday_factors')
        if current_date.weekday() >= 5:  # 周六日
            day_of_week_factor = weekday_factors['weekend']
        elif current_date.weekday() == 4:  # 周五
            day_of_week_factor = weekday_factors['friday']
        elif current_date.weekday() == 0:  # 周一
            day_of_week_factor = weekday_factors['monday']
        
        # 季节调整
        season_factors = self.config.get('generation', 'season_factors')
        if season == 1:  # 春季
            season_factor = season_factors['spring']
        elif season == 2:  # 夏季
            season_factor = season_factors['summer']
        elif season == 3:  # 秋季
            season_factor = season_factors['autumn']
        else:  # 冬季
            season_factor = season_factors['winter']
        
        # 应用调整因子
        self.daily_metrics['orders'] = int(self.daily_metrics['orders'] * 
                                         holiday_factor * 
                                         season_factor * 
                                         day_of_week_factor)
        self.daily_metrics['inventory_updates'] = int(self.daily_metrics['inventory_updates'] * 
                                                     holiday_factor * 
                                                     season_factor * 
                                                     day_of_week_factor)

    def generate_all_data(self, connection):
        """生成所有数据"""
        current_date = self.start_time
        total_days = (self.end_time - self.start_time).days
        commit_interval = self.config.get('performance', 'commit_interval', default=7)
        
        try:
            with tqdm(total=total_days, desc="生成数据") as pbar:
                while current_date <= self.end_time:
                    # 生成每日数据
                    self._generate_daily_data(connection, current_date)
                    
                    # 更新进度
                    current_date += timedelta(days=1)
                    pbar.update(1)
                    
                    # 定期提交事务
                    if (current_date - self.start_time).days % commit_interval == 0:
                        connection.commit()
            
            connection.commit()
            
            # 输出最终统计信息
            self._print_stats()
            
        except Exception as e:
            logger.error(f"数据生成过程中发生错误: {e}")
            connection.rollback()
            raise

def generate_users(connection, num_users):
    """生成用户数据"""
    logger.info(f"开始生成 {num_users} 个用户数据...")
    
    users = []
    user_types = list(USER_TYPE_DISTRIBUTION.keys())
    user_type_weights = list(USER_TYPE_DISTRIBUTION.values())
    age_groups = list(USER_AGE_DISTRIBUTION.keys())
    age_group_weights = list(USER_AGE_DISTRIBUTION.values())
    
    # 按区域权重生成用户分布
    regions = list(REGION_DISTRIBUTION.keys())
    region_weights = list(REGION_DISTRIBUTION.values())
    user_regions = random.choices(regions, weights=region_weights, k=num_users)
    
    now = datetime.now()
    registration_date_min = now - timedelta(days=365*2)  # 最早注册时间为2年前
    
    for i in tqdm(range(num_users), desc="生成用户"):
        region = user_regions[i]
        province = random.choice(REGIONS[region])
        
        # 随机确定用户类型和年龄组
        user_type = random.choices(user_types, weights=user_type_weights, k=1)[0]
        age_group = random.choices(age_groups, weights=age_group_weights, k=1)[0]
        
        # 随机生成注册时间
        registration_date = fake.date_time_between(start_date=registration_date_min, end_date=now)
        
        # 随机生成最后登录时间，必须在注册时间之后
        last_login = fake.date_time_between(start_date=registration_date, end_date=now)
        
        user = {
            'username': fake.user_name(),
            'password': fake.password(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address': fake.street_address(),
            'city': fake.city(),
            'province': province,
            'postal_code': fake.postcode(),
            'region': region,
            'age_group': age_group,
            'user_type': user_type,
            'registration_date': registration_date,
            'last_login': last_login
        }
        users.append(user)
    
    # 批量插入到数据库
    try:
        cursor = connection.cursor()
        
        sql = """
        INSERT INTO users (username, password, email, phone, address, city, province, 
                          postal_code, region, age_group, user_type, registration_date, last_login)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        values = [(user['username'], user['password'], user['email'], user['phone'], 
                  user['address'], user['city'], user['province'], user['postal_code'], 
                  user['region'], user['age_group'], user['user_type'], 
                  user['registration_date'], user['last_login']) for user in users]
        
        cursor.executemany(sql, values)
        connection.commit()
        logger.info(f"成功插入 {len(users)} 个用户数据")
        
    except Error as e:
        logger.error(f"插入用户数据时发生错误: {e}")
        connection.rollback()
    
    return users

def generate_suppliers(connection, num_suppliers=50):
    """生成供应商数据"""
    logger.info(f"开始生成 {num_suppliers} 个供应商数据...")
    
    suppliers = []
    
    # 按区域权重生成供应商分布
    regions = list(REGION_DISTRIBUTION.keys())
    region_weights = list(REGION_DISTRIBUTION.values())
    supplier_regions = random.choices(regions, weights=region_weights, k=num_suppliers)
    
    now = datetime.now()
    create_time_min = now - timedelta(days=365*3)  # 最早创建时间为3年前
    
    for i in tqdm(range(num_suppliers), desc="生成供应商"):
        region = supplier_regions[i]
        province = random.choice(REGIONS[region])
        
        # 随机生成创建时间
        create_time = fake.date_time_between(start_date=create_time_min, end_date=now)
        
        supplier = {
            'supplier_name': fake.company(),
            'contact_name': fake.name(),
            'email': fake.company_email(),
            'phone': fake.phone_number(),
            'address': fake.street_address(),
            'city': fake.city(),
            'province': province,
            'postal_code': fake.postcode(),
            'region': region,
            'create_time': create_time
        }
        suppliers.append(supplier)
    
    # 批量插入到数据库
    try:
        cursor = connection.cursor()
        
        sql = """
        INSERT INTO suppliers (supplier_name, contact_name, email, phone, address, city, 
                              province, postal_code, region, create_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        values = [(supplier['supplier_name'], supplier['contact_name'], supplier['email'], 
                  supplier['phone'], supplier['address'], supplier['city'], supplier['province'], 
                  supplier['postal_code'], supplier['region'], supplier['create_time']) 
                  for supplier in suppliers]
        
        cursor.executemany(sql, values)
        connection.commit()
        logger.info(f"成功插入 {len(suppliers)} 个供应商数据")
        
    except Error as e:
        logger.error(f"插入供应商数据时发生错误: {e}")
        connection.rollback()
    
    return suppliers

def generate_products(connection, num_products):
    """生成产品数据"""
    logger.info(f"开始生成 {num_products} 个产品数据...")
    
    products = []
    
    # 获取供应商ID列表
    cursor = connection.cursor()
    cursor.execute("SELECT supplier_id FROM suppliers")
    supplier_ids = [row[0] for row in cursor.fetchall()]
    
    if not supplier_ids:
        logger.error("没有找到供应商数据，请先生成供应商数据")
        return []
    
    # 按类别权重分配产品数量
    categories = list(PRODUCT_CATEGORIES.keys())
    category_weights = list(PRODUCT_CATEGORIES.values())
    product_counts = [int(num_products * weight) for weight in category_weights]
    
    # 确保总数为num_products
    diff = num_products - sum(product_counts)
    product_counts[0] += diff
    
    now = datetime.now()
    create_time_min = now - timedelta(days=365*2)  # 最早创建时间为2年前
    
    product_id = 1
    
    for i, category in enumerate(categories):
        subcategory_dict = globals()[f"{category.upper().replace('产品', '')}_SUBCATEGORIES"]
        subcategories = list(subcategory_dict.keys())
        subcategory_weights = list(subcategory_dict.values())
        
        # 按子类别权重分配产品数量
        subcategory_counts = [int(product_counts[i] * weight) for weight in subcategory_weights]
        
        # 确保总数为该类别的产品数量
        diff = product_counts[i] - sum(subcategory_counts)
        subcategory_counts[0] += diff
        
        for j, subcategory in enumerate(subcategories):
            for _ in tqdm(range(subcategory_counts[j]), desc=f"生成{category}-{subcategory}产品"):
                # 随机生成价格
                min_price, max_price = PRICE_RANGES[category][subcategory]
                price = round(random.uniform(min_price, max_price), 2)
                
                # 设置成本为价格的60-80%
                cost = round(price * random.uniform(0.6, 0.8), 2)
                
                # 随机生成创建时间
                create_time = fake.date_time_between(start_date=create_time_min, end_date=now)
                
                # 随机选择供应商
                supplier = random.choice(supplier_ids)
                
                # 随机生成尺寸和重量
                if category in ['电子产品', '家居']:
                    weight = round(random.uniform(0.2, 15.0), 2)
                    dimensions = f"{random.randint(10, 100)}x{random.randint(10, 100)}x{random.randint(5, 50)}"
                elif category == '服装':
                    weight = round(random.uniform(0.1, 2.0), 2)
                    dimensions = random.choice(['S', 'M', 'L', 'XL', 'XXL'])
                elif category == '食品':
                    weight = round(random.uniform(0.05, 5.0), 2)
                    dimensions = f"{random.randint(5, 30)}x{random.randint(5, 30)}x{random.randint(2, 20)}"
                else:  # 图书
                    weight = round(random.uniform(0.2, 3.0), 2)
                    dimensions = f"{random.randint(15, 30)}x{random.randint(10, 25)}x{random.randint(1, 5)}"
                
                product = {
                    'product_name': f"{subcategory} {fake.word()} {product_id}",
                    'category': category,
                    'subcategory': subcategory,
                    'description': fake.paragraph(),
                    'price': price,
                    'cost': cost,
                    'supplier': str(supplier),
                    'sku': f"SKU-{category[:2]}-{subcategory[:2]}-{product_id:06d}",
                    'weight': weight,
                    'dimensions': dimensions,
                    'is_active': random.choices([True, False], weights=[0.95, 0.05], k=1)[0],
                    'create_time': create_time
                }
                products.append(product)
                product_id += 1
    
    # 批量插入到数据库
    try:
        cursor = connection.cursor()
        
        sql = """
        INSERT INTO products (product_name, category, subcategory, description, price, cost, 
                             supplier, sku, weight, dimensions, is_active, create_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        values = [(product['product_name'], product['category'], product['subcategory'], 
                  product['description'], product['price'], product['cost'], product['supplier'], 
                  product['sku'], product['weight'], product['dimensions'], 
                  product['is_active'], product['create_time']) for product in products]
        
        cursor.executemany(sql, values)
        connection.commit()
        logger.info(f"成功插入 {len(products)} 个产品数据")
        
    except Error as e:
        logger.error(f"插入产品数据时发生错误: {e}")
        connection.rollback()
    
    return products

class DataGenerator:
    def __init__(self, db_config):
        """初始化数据生成器"""
        self.db_config = db_config
        self.connection = None
        self.cursor = None
        self.dimension_data = {}  # 存储维表数据
        self.connect()

    def connect(self):
        """建立数据库连接"""
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            self.cursor = self.connection.cursor()
            logger.info("数据库连接成功")
        except Error as e:
            logger.error(f"数据库连接失败: {e}")
            raise

    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logger.info("数据库连接已关闭")

    def generate_dimension_data(self):
        """生成维表数据"""
        try:
            # 生成时间维表数据
            self.generate_date_dimension()
            
            # 生成地区维表数据
            self.generate_location_dimension()
            
            # 生成支付方式维表数据
            self.generate_payment_method_dimension()
            
            # 生成商品类别维表数据
            self.generate_product_category_dimension()
            
            # 生成供应商维表数据
            self.generate_supplier_dimension()
            
            # 生成仓库维表数据
            self.generate_warehouse_dimension()
            
            # 生成员工维表数据
            self.generate_employee_dimension()
            
            # 生成客户维表数据
            self.generate_customer_dimension()
            
            # 生成活动维表数据
            self.generate_activity_dimension()
            
            logger.info("维表数据生成完成")
        except Error as e:
            logger.error(f"维表数据生成失败: {e}")
            raise

    def generate_date_dimension(self):
        """生成时间维表数据"""
        start_date = datetime.datetime.strptime(DIMENSION_DATA_GENERATION["dim_date"]["start_date"], "%Y-%m-%d")
        end_date = datetime.datetime.strptime(DIMENSION_DATA_GENERATION["dim_date"]["end_date"], "%Y-%m-%d")
        current_date = start_date
        
        while current_date <= end_date:
            date_key = int(current_date.strftime("%Y%m%d"))
            is_weekend = current_date.weekday() >= 5
            is_holiday = current_date.strftime("%Y-%m-%d") in DIMENSION_DATA_GENERATION["dim_date"]["holidays"]
            holiday_name = DIMENSION_DATA_GENERATION["dim_date"]["holidays"].get(current_date.strftime("%Y-%m-%d"))
            
            sql = """
            INSERT INTO dim_date (
                date_key, date, year, quarter, month, day,
                day_of_week, day_of_year, week_of_year,
                is_weekend, is_holiday, holiday_name, is_workday,
                season, fiscal_year, fiscal_quarter, fiscal_month
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            values = (
                date_key,
                current_date.date(),
                current_date.year,
                (current_date.month - 1) // 3 + 1,
                current_date.month,
                current_date.day,
                current_date.weekday() + 1,
                current_date.timetuple().tm_yday,
                current_date.isocalendar()[1],
                is_weekend,
                is_holiday,
                holiday_name,
                not is_weekend and not is_holiday,
                self.get_season(current_date.month),
                current_date.year,
                (current_date.month - 1) // 3 + 1,
                current_date.month
            )
            
            self.cursor.execute(sql, values)
            current_date += datetime.timedelta(days=1)
        
        self.connection.commit()
        logger.info("时间维表数据生成完成")

    def generate_location_dimension(self):
        """生成地区维表数据"""
        location_key = 1
        for region, provinces in DIMENSION_DATA_GENERATION["dim_location"]["regions"].items():
            for province in provinces:
                # 为每个省份生成3-5个城市
                for _ in range(random.randint(3, 5)):
                    city = fake.city()
                    # 为每个城市生成2-4个区
                    for _ in range(random.randint(2, 4)):
                        district = fake.district()
                        # 为每个区生成3-5条街道
                        for _ in range(random.randint(3, 5)):
                            sql = """
                            INSERT INTO dim_location (
                                location_key, country, province, city, district,
                                street, postal_code, region, latitude, longitude
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                            """
                            
                            values = (
                                location_key,
                                "中国",
                                province,
                                city,
                                district,
                                fake.street_name(),
                                fake.postcode(),
                                region,
                                round(random.uniform(18.0, 53.0), 6),
                                round(random.uniform(73.0, 135.0), 6)
                            )
                            
                            self.cursor.execute(sql, values)
                            location_key += 1
        
        self.connection.commit()
        logger.info("地区维表数据生成完成")

    def generate_payment_method_dimension(self):
        """生成支付方式维表数据"""
        for method in DIMENSION_DATA_GENERATION["dim_payment_method"]["methods"]:
            sql = """
            INSERT INTO dim_payment_method (
                payment_method_key, payment_method_code,
                payment_method_name, payment_type, description
            ) VALUES (
                %s, %s, %s, %s, %s
            )
            """
            
            values = (
                method["code"],
                method["code"],
                method["name"],
                method["type"],
                f"{method['name']}支付方式"
            )
            
            self.cursor.execute(sql, values)
        
        self.connection.commit()
        logger.info("支付方式维表数据生成完成")

    def generate_product_category_dimension(self):
        """生成商品类别维表数据"""
        category_key = 1
        for main_category, subcategories in DIMENSION_DATA_GENERATION["dim_product_category"]["categories"].items():
            # 插入主类别
            main_category_key = category_key
            sql = """
            INSERT INTO dim_product_category (
                category_key, category_code, category_name,
                category_level, category_path, description
            ) VALUES (
                %s, %s, %s, %s, %s, %s
            )
            """
            
            values = (
                main_category_key,
                f"CAT_{main_category_key:03d}",
                main_category,
                1,
                main_category,
                f"{main_category}类别"
            )
            
            self.cursor.execute(sql, values)
            category_key += 1
            
            # 插入子类别
            for subcategory, products in subcategories.items():
                subcategory_key = category_key
                sql = """
                INSERT INTO dim_product_category (
                    category_key, category_code, category_name,
                    parent_category_key, category_level, category_path,
                    description
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s
                )
                """
                
                values = (
                    subcategory_key,
                    f"CAT_{subcategory_key:03d}",
                    subcategory,
                    main_category_key,
                    2,
                    f"{main_category}/{subcategory}",
                    f"{subcategory}类别"
                )
                
                self.cursor.execute(sql, values)
                category_key += 1
                
                # 插入具体产品类别
                for product in products:
                    sql = """
                    INSERT INTO dim_product_category (
                        category_key, category_code, category_name,
                        parent_category_key, category_level, category_path,
                        description
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    
                    values = (
                        category_key,
                        f"CAT_{category_key:03d}",
                        product,
                        subcategory_key,
                        3,
                        f"{main_category}/{subcategory}/{product}",
                        f"{product}类别"
                    )
                    
                    self.cursor.execute(sql, values)
                    category_key += 1
        
        self.connection.commit()
        logger.info("商品类别维表数据生成完成")

    def generate_supplier_dimension(self):
        """生成供应商维表数据"""
        for i in range(DIMENSION_DATA_GENERATION["dim_supplier"]["count"]):
            supplier_type = random.choice(DIMENSION_DATA_GENERATION["dim_supplier"]["types"])
            region = random.choice(list(REGIONS.keys()))
            province = random.choice(REGIONS[region])
            city = fake.city()
            
            sql = """
            INSERT INTO dim_supplier (
                supplier_key, supplier_code, supplier_name,
                contact_name, contact_phone, contact_email,
                address, city, province, postal_code, region,
                business_license, tax_number, credit_rating
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s
            )
            """
            
            values = (
                i + 1,
                f"SUP_{i+1:03d}",
                f"{supplier_type}{i+1}",
                fake.name(),
                fake.phone_number(),
                fake.email(),
                fake.address(),
                city,
                province,
                fake.postcode(),
                region,
                f"BL_{i+1:08d}",
                f"TN_{i+1:08d}",
                random.choice(["A", "B", "C", "D"])
            )
            
            self.cursor.execute(sql, values)
        
        self.connection.commit()
        logger.info("供应商维表数据生成完成")

    def generate_warehouse_dimension(self):
        """生成仓库维表数据"""
        for i in range(DIMENSION_DATA_GENERATION["dim_warehouse"]["count"]):
            warehouse_type = random.choice(DIMENSION_DATA_GENERATION["dim_warehouse"]["types"])
            region = random.choice(list(REGIONS.keys()))
            province = random.choice(REGIONS[region])
            city = fake.city()
            
            sql = """
            INSERT INTO dim_warehouse (
                warehouse_key, warehouse_code, warehouse_name,
                warehouse_type, address, city, province,
                postal_code, region, manager, contact_phone,
                capacity
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            values = (
                i + 1,
                f"WH_{i+1:03d}",
                f"{warehouse_type}{i+1}",
                warehouse_type,
                fake.address(),
                city,
                province,
                fake.postcode(),
                region,
                fake.name(),
                fake.phone_number(),
                random.randint(1000, 10000)
            )
            
            self.cursor.execute(sql, values)
        
        self.connection.commit()
        logger.info("仓库维表数据生成完成")

    def generate_employee_dimension(self):
        """生成员工维表数据"""
        for i in range(DIMENSION_DATA_GENERATION["dim_employee"]["count"]):
            department = random.choice(DIMENSION_DATA_GENERATION["dim_employee"]["departments"])
            position = random.choice(DIMENSION_DATA_GENERATION["dim_employee"]["positions"])
            hire_date = fake.date_between(start_date="-5y", end_date="today")
            
            sql = """
            INSERT INTO dim_employee (
                employee_key, employee_code, employee_name,
                department, position, hire_date, email,
                phone, status
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            values = (
                i + 1,
                f"EMP_{i+1:03d}",
                fake.name(),
                department,
                position,
                hire_date,
                fake.email(),
                fake.phone_number(),
                random.choice(["在职", "离职", "休假"])
            )
            
            self.cursor.execute(sql, values)
        
        self.connection.commit()
        logger.info("员工维表数据生成完成")

    def generate_customer_dimension(self):
        """生成客户维表数据"""
        for i in range(DIMENSION_DATA_GENERATION["dim_customer"]["count"]):
            customer_type = random.choice(DIMENSION_DATA_GENERATION["dim_customer"]["types"])
            customer_level = random.choice(DIMENSION_DATA_GENERATION["dim_customer"]["levels"])
            region = random.choice(list(REGIONS.keys()))
            province = random.choice(REGIONS[region])
            city = fake.city()
            registration_date = fake.date_between(start_date="-3y", end_date="today")
            
            sql = """
            INSERT INTO dim_customer (
                customer_key, customer_code, customer_name,
                customer_type, gender, birth_date, email,
                phone, address, city, province, postal_code,
                region, registration_date, customer_level,
                points
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            """
            
            values = (
                i + 1,
                f"CUS_{i+1:03d}",
                fake.name(),
                customer_type,
                random.choice(["男", "女"]),
                fake.date_of_birth(minimum_age=18, maximum_age=80),
                fake.email(),
                fake.phone_number(),
                fake.address(),
                city,
                province,
                fake.postcode(),
                region,
                registration_date,
                customer_level,
                random.randint(0, 10000)
            )
            
            self.cursor.execute(sql, values)
        
        self.connection.commit()
        logger.info("客户维表数据生成完成")

    def get_season(self, month):
        """获取季节"""
        if 3 <= month <= 5:
            return "春季"
        elif 6 <= month <= 8:
            return "夏季"
        elif 9 <= month <= 11:
            return "秋季"
        else:
            return "冬季"

    def generate_activity_dimension(self):
        """生成活动维表数据"""
        activity_key = 1
        start_date = datetime.datetime.strptime(DIMENSION_DATA_GENERATION["dim_date"]["start_date"], "%Y-%m-%d")
        end_date = datetime.datetime.strptime(DIMENSION_DATA_GENERATION["dim_date"]["end_date"], "%Y-%m-%d")
        current_date = start_date
        
        while current_date <= end_date:
            # 检查是否有预定义的活动
            month_day = current_date.strftime("%m-%d")
            year = current_date.year
            
            # 检查大促活动
            for activity_name, date_range in ACTIVITY_DATES["大促活动"].items():
                if date_range["start"] <= month_day <= date_range["end"]:
                    self._generate_activity(
                        activity_key,
                        activity_name,
                        "大促活动",
                        current_date,
                        current_date + datetime.timedelta(days=random.randint(3, 7))
                    )
                    activity_key += 1
            
            # 检查节日活动
            for activity_name, date_range in ACTIVITY_DATES["节日活动"].items():
                if date_range["start"] <= month_day <= date_range["end"]:
                    self._generate_activity(
                        activity_key,
                        activity_name,
                        "节日活动",
                        current_date,
                        current_date + datetime.timedelta(days=random.randint(1, 3))
                    )
                    activity_key += 1
            
            # 随机生成其他类型的活动
            if random.random() < 0.3:  # 30%的概率生成周末活动
                if current_date.weekday() >= 5:  # 周末
                    self._generate_activity(
                        activity_key,
                        f"周末特惠{current_date.strftime('%Y%m%d')}",
                        "周末活动",
                        current_date,
                        current_date + datetime.timedelta(days=random.randint(1, 2))
                    )
                    activity_key += 1
            
            if random.random() < 0.5:  # 50%的概率生成日常活动
                self._generate_activity(
                    activity_key,
                    f"每日特惠{current_date.strftime('%Y%m%d')}",
                    "日常活动",
                    current_date,
                    current_date
                )
                activity_key += 1
            
            current_date += datetime.timedelta(days=1)
        
        self.connection.commit()
        logger.info("活动维表数据生成完成")

    def _generate_activity(self, activity_key, activity_name, activity_type, start_date, end_date):
        """生成单个活动数据"""
        activity_config = ACTIVITY_CONFIG["activity_types"][activity_type]
        
        # 计算活动状态
        current_date = datetime.datetime.now()
        if start_date > current_date:
            status = "筹备中"
        elif end_date < current_date:
            status = "已结束"
        else:
            status = "进行中"
        
        # 生成活动数据
        sql = """
        INSERT INTO dim_activity (
            activity_key, activity_code, activity_name,
            activity_type, start_date, end_date,
            status, channel, target_customer,
            discount_rate, sales_increase_rate,
            customer_increase_rate, views,
            participants, conversion_rate,
            avg_order_value, description
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        # 生成活动指标
        views = random.randint(*ACTIVITY_CONFIG["activity_metrics"]["views"])
        participants = random.randint(*ACTIVITY_CONFIG["activity_metrics"]["participants"])
        conversion_rate = random.uniform(*ACTIVITY_CONFIG["activity_metrics"]["conversion_rate"])
        avg_order_value = random.uniform(*ACTIVITY_CONFIG["activity_metrics"]["avg_order_value"])
        
        values = (
            activity_key,
            f"ACT_{activity_key:06d}",
            activity_name,
            activity_type,
            start_date,
            end_date,
            status,
            random.choice(ACTIVITY_CONFIG["activity_channels"]),
            random.choice(ACTIVITY_CONFIG["activity_targets"]),
            random.uniform(*activity_config["discount_range"]),
            random.uniform(*activity_config["sales_increase"]),
            random.uniform(*activity_config["customer_increase"]),
            views,
            participants,
            conversion_rate,
            avg_order_value,
            f"{activity_name}活动描述"
        )
        
        self.cursor.execute(sql, values)

    def generate_fact_data(self):
        """生成事实表数据"""
        try:
            # 生成订单事实表数据
            self.generate_order_facts()
            
            # 生成库存事实表数据
            self.generate_inventory_facts()
            
            # 生成销售事实表数据
            self.generate_sales_facts()
            
            # 生成活动事实表数据
            self.generate_activity_facts()
            
            logger.info("事实表数据生成完成")
        except Error as e:
            logger.error(f"事实表数据生成失败: {e}")
            raise

    def generate_order_facts(self):
        """生成订单事实表数据"""
        # 获取维表数据
        self.cursor.execute("SELECT date_key FROM dim_date")
        date_keys = [row[0] for row in self.cursor.fetchall()]
        
        self.cursor.execute("SELECT customer_key FROM dim_customer")
        customer_keys = [row[0] for row in self.cursor.fetchall()]
        
        self.cursor.execute("SELECT employee_key FROM dim_employee")
        employee_keys = [row[0] for row in self.cursor.fetchall()]
        
        self.cursor.execute("SELECT payment_method_key FROM dim_payment_method")
        payment_method_keys = [row[0] for row in self.cursor.fetchall()]
        
        self.cursor.execute("SELECT location_key FROM dim_location")
        location_keys = [row[0] for row in self.cursor.fetchall()]
        
        # 生成订单数据
        for _ in range(DEFAULT_ORDERS):
            order_date_key = random.choice(date_keys)
            customer_key = random.choice(customer_keys)
            employee_key = random.choice(employee_keys)
            payment_method_key = random.choice(payment_method_keys)
            shipping_location_key = random.choice(location_keys)
            
            # 获取订单日期
            self.cursor.execute(f"SELECT date FROM dim_date WHERE date_key = {order_date_key}")
            order_date = self.cursor.fetchone()[0]
            
            # 生成订单状态
            status = self.generate_order_status()
            
            # 生成订单金额
            total_amount = round(random.uniform(100, 10000), 2)
            shipping_fee = round(random.uniform(0, 50), 2)
            discount = round(random.uniform(0, total_amount * 0.2), 2)
            
            sql = """
            INSERT INTO orders (
                order_date, customer_key, employee_key,
                payment_method_key, shipping_location_key,
                status, shipping_fee, total_amount, discount
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            values = (
                order_date,
                customer_key,
                employee_key,
                payment_method_key,
                shipping_location_key,
                status,
                shipping_fee,
                total_amount,
                discount
            )
            
            self.cursor.execute(sql, values)
            order_id = self.cursor.lastrowid
            
            # 生成订单项数据
            self.generate_order_items(order_id, total_amount)
        
        self.connection.commit()
        logger.info("订单事实表数据生成完成")

    def generate_inventory_facts(self):
        """生成库存事实表数据"""
        # 获取维表数据
        self.cursor.execute("SELECT product_key, category FROM dim_product")
        products = self.cursor.fetchall()
        
        self.cursor.execute("SELECT warehouse_key FROM dim_warehouse")
        warehouse_keys = [row[0] for row in self.cursor.fetchall()]
        
        # 生成库存数据
        for product_key, category in products:
            for warehouse_key in warehouse_keys:
                # 根据商品类别和周转率计算库存水平
                turnover_rate = INVENTORY_TURNOVER_REFERENCE[category]
                base_stock = random.randint(100, 1000)
                quantity = int(base_stock / turnover_rate)
                
                # 设置库存预警阈值
                low_stock_threshold = int(quantity * 0.2)  # 20%作为预警阈值
                reorder_quantity = int(quantity * 0.5)  # 50%作为补货量
                
                # 生成最后补货时间
                last_restock_date = fake.date_time_between(start_date="-1y", end_date="now")
                
                sql = """
                INSERT INTO inventory (
                    product_key, warehouse_key, quantity,
                    low_stock_threshold, reorder_quantity,
                    last_restock_date
                ) VALUES (
                    %s, %s, %s, %s, %s, %s
                )
                """
                
                values = (
                    product_key,
                    warehouse_key,
                    quantity,
                    low_stock_threshold,
                    reorder_quantity,
                    last_restock_date
                )
                
                self.cursor.execute(sql, values)
        
        self.connection.commit()
        logger.info("库存事实表数据生成完成")

    def generate_sales_facts(self):
        """生成销售事实表数据"""
        # 获取维表数据
        self.cursor.execute("SELECT date_key, month FROM dim_date")
        dates = self.cursor.fetchall()
        
        self.cursor.execute("SELECT product_key, category FROM dim_product")
        products = self.cursor.fetchall()
        
        self.cursor.execute("SELECT customer_key, customer_type FROM dim_customer")
        customers = self.cursor.fetchall()
        
        self.cursor.execute("SELECT employee_key FROM dim_employee")
        employee_keys = [row[0] for row in self.cursor.fetchall()]
        
        # 生成销售数据
        for date_key, month in dates:
            seasonal_factor = SEASONAL_FACTORS[month - 1]
            
            for product_key, category in products:
                for customer_key, customer_type in customers:
                    # 根据客户类型和购买频率决定是否生成销售记录
                    purchase_frequency = USER_PURCHASE_FREQUENCY[customer_type]
                    if random.random() < purchase_frequency:
                        for employee_key in random.sample(employee_keys, random.randint(1, 3)):
                            # 基础数量
                            base_quantity = random.randint(1, 10)
                            
                            # 应用季节性因子
                            quantity = int(base_quantity * seasonal_factor)
                            
                            # 获取商品价格
                            self.cursor.execute("""
                                SELECT price FROM dim_product WHERE product_key = %s
                            """, (product_key,))
                            price = self.cursor.fetchone()[0]
                            
                            # 计算总金额
                            total_amount = quantity * price
                            
                            sql = """
                            INSERT INTO sales (
                                date_key, product_key, customer_key,
                                employee_key, quantity, unit_price,
                                total_amount
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s
                            )
                            """
                            
                            values = (
                                date_key,
                                product_key,
                                customer_key,
                                employee_key,
                                quantity,
                                price,
                                total_amount
                            )
                            
                            self.cursor.execute(sql, values)
        
        self.connection.commit()
        logger.info("销售事实表数据生成完成")

    def generate_activity_facts(self):
        """生成活动事实表数据"""
        # 获取活动数据
        self.cursor.execute("""
            SELECT activity_key, start_date, end_date, 
                   discount_rate, sales_increase_rate, 
                   customer_increase_rate
            FROM dim_activity
            WHERE status != '已取消'
        """)
        activities = self.cursor.fetchall()
        
        # 获取销售数据
        self.cursor.execute("""
            SELECT s.date_key, s.product_key, s.customer_key,
                   s.employee_key, s.quantity, s.unit_price,
                   s.total_amount
            FROM sales s
        """)
        sales = self.cursor.fetchall()
        
        # 生成活动销售数据
        for sale in sales:
            sale_date = sale[0]
            
            # 检查销售日期是否在活动期间
            for activity in activities:
                activity_key, start_date, end_date, discount_rate, sales_increase, customer_increase = activity
                
                if start_date <= sale_date <= end_date:
                    # 应用活动效果
                    adjusted_quantity = int(sale[4] * sales_increase)
                    adjusted_unit_price = round(sale[5] * (1 - discount_rate), 2)
                    adjusted_total_amount = adjusted_quantity * adjusted_unit_price
                    
                    sql = """
                    INSERT INTO activity_sales (
                        activity_key, date_key, product_key,
                        customer_key, employee_key, quantity,
                        unit_price, total_amount, discount_amount
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    
                    values = (
                        activity_key,
                        sale_date,
                        sale[1],
                        sale[2],
                        sale[3],
                        adjusted_quantity,
                        adjusted_unit_price,
                        adjusted_total_amount,
                        sale[6] - adjusted_total_amount
                    )
                    
                    self.cursor.execute(sql, values)
        
        self.connection.commit()
        logger.info("活动事实表数据生成完成")

    def generate_order_items(self, order_id, total_amount):
        """生成订单项数据"""
        # 获取商品数据
        self.cursor.execute("""
            SELECT p.product_key, p.price, p.category
            FROM dim_product p
            ORDER BY RAND() LIMIT 1
        """)
        product = self.cursor.fetchone()
        
        if product:
            product_key, price, category = product
            # 使用配置的数量范围
            min_quantity, max_quantity = ORDER_QUANTITY_RANGE[category]
            quantity = random.randint(min_quantity, max_quantity)
            
            # 应用季节性因子
            self.cursor.execute("""
                SELECT d.month
                FROM orders o
                JOIN dim_date d ON o.order_date = d.date
                WHERE o.order_id = %s
            """, (order_id,))
            month = self.cursor.fetchone()[0]
            seasonal_factor = SEASONAL_FACTORS[month - 1]
            
            # 调整价格和数量
            adjusted_price = price * seasonal_factor
            adjusted_quantity = int(quantity * seasonal_factor)
            
            # 计算折扣
            discount = round(random.uniform(0, adjusted_price * 0.1), 2)
            total_price = adjusted_quantity * (adjusted_price - discount)
            
            sql = """
            INSERT INTO order_items (
                order_id, product_key, quantity,
                price, discount, total_price
            ) VALUES (
                %s, %s, %s, %s, %s, %s
            )
            """
            
            values = (
                order_id,
                product_key,
                adjusted_quantity,
                adjusted_price,
                discount,
                total_price
            )
            
            self.cursor.execute(sql, values)

    def generate_order_status(self):
        """根据配置生成订单状态"""
        status_probability = random.random()
        if status_probability < ORDER_STATUS['已取消']:
            return "已取消"
        elif status_probability < ORDER_STATUS['已支付']:
            return "已支付"
        elif status_probability < ORDER_STATUS['已发货']:
            return "已发货"
        elif status_probability < ORDER_STATUS['已完成']:
            return "已完成"
        elif status_probability < ORDER_STATUS['已退货']:
            return "已退货"
        else:
            return "处理中"

    def validate_generated_data(self):
        """验证生成的数据是否符合配置要求"""
        try:
            # 验证订单状态分布
            self._validate_order_status_distribution()
            
            # 验证库存周转率
            self._validate_inventory_turnover()
            
            # 验证用户购买频率
            self._validate_user_purchase_frequency()
            
            # 验证活动效果
            self._validate_activity_effects()
            
            logger.info("数据验证完成")
        except DataValidationError as e:
            logger.error(f"数据验证失败: {e}")
            raise

    def _validate_order_status_distribution(self):
        """验证订单状态分布"""
        self.cursor.execute("""
            SELECT status, COUNT(*) as count
            FROM orders
            GROUP BY status
        """)
        status_counts = dict(self.cursor.fetchall())
        total_orders = sum(status_counts.values())
        
        for status, expected_prob in ORDER_STATUS.items():
            if status in status_counts:
                actual_prob = status_counts[status] / total_orders
                if abs(actual_prob - expected_prob) > 0.1:  # 允许10%的偏差
                    raise DataValidationError(
                        f"订单状态 {status} 的分布不符合预期: "
                        f"期望概率 {expected_prob}, 实际概率 {actual_prob}"
                    )

    def _validate_inventory_turnover(self):
        """验证库存周转率"""
        self.cursor.execute("""
            SELECT p.category, 
                   COUNT(DISTINCT i.product_key) as product_count,
                   SUM(i.quantity) as total_quantity,
                   COUNT(DISTINCT s.product_key) as sold_products,
                   SUM(s.quantity) as total_sold
            FROM inventory i
            JOIN dim_product p ON i.product_key = p.product_key
            LEFT JOIN sales s ON i.product_key = s.product_key
            GROUP BY p.category
        """)
        
        for category, product_count, total_quantity, sold_products, total_sold in self.cursor.fetchall():
            if product_count > 0:
                turnover_rate = total_sold / total_quantity if total_quantity > 0 else 0
                expected_turnover = INVENTORY_TURNOVER_REFERENCE.get(category, 0)
                
                if abs(turnover_rate - expected_turnover) > 0.2:  # 允许20%的偏差
                    raise DataValidationError(
                        f"商品类别 {category} 的库存周转率不符合预期: "
                        f"期望周转率 {expected_turnover}, 实际周转率 {turnover_rate}"
                    )

    def _validate_user_purchase_frequency(self):
        """验证用户购买频率"""
        self.cursor.execute("""
            SELECT c.customer_type,
                   COUNT(DISTINCT o.order_id) as order_count,
                   COUNT(DISTINCT o.customer_key) as customer_count
            FROM orders o
            JOIN dim_customer c ON o.customer_key = c.customer_key
            GROUP BY c.customer_type
        """)
        
        for customer_type, order_count, customer_count in self.cursor.fetchall():
            if customer_count > 0:
                avg_frequency = order_count / customer_count
                expected_frequency = USER_PURCHASE_FREQUENCY.get(customer_type, 0)
                
                if abs(avg_frequency - expected_frequency) > 0.2:  # 允许20%的偏差
                    raise DataValidationError(
                        f"客户类型 {customer_type} 的购买频率不符合预期: "
                        f"期望频率 {expected_frequency}, 实际频率 {avg_frequency}"
                    )

    def _validate_activity_effects(self):
        """验证活动效果"""
        self.cursor.execute("""
            SELECT a.activity_type,
                   COUNT(DISTINCT as.activity_key) as activity_count,
                   AVG(as.discount_amount) as avg_discount,
                   AVG(as.quantity) as avg_quantity,
                   AVG(as.total_amount) as avg_amount
            FROM activity_sales as
            JOIN dim_activity a ON as.activity_key = a.activity_key
            GROUP BY a.activity_type
        """)
        
        for activity_type, activity_count, avg_discount, avg_quantity, avg_amount in self.cursor.fetchall():
            if activity_count > 0:
                activity_config = ACTIVITY_CONFIG["activity_types"].get(activity_type, {})
                
                # 验证折扣率
                if activity_config.get("discount_range"):
                    min_discount, max_discount = activity_config["discount_range"]
                    if not (min_discount <= avg_discount <= max_discount):
                        raise DataValidationError(
                            f"活动类型 {activity_type} 的折扣率不符合预期: "
                            f"期望范围 [{min_discount}, {max_discount}], "
                            f"实际值 {avg_discount}"
                        )
                
                # 验证销量提升
                if activity_config.get("sales_increase"):
                    min_increase, max_increase = activity_config["sales_increase"]
                    if not (min_increase <= avg_quantity <= max_increase):
                        raise DataValidationError(
                            f"活动类型 {activity_type} 的销量提升不符合预期: "
                            f"期望范围 [{min_increase}, {max_increase}], "
                            f"实际值 {avg_quantity}"
                        )

    def generate_data(self):
        """生成所有数据"""
        try:
            # 生成维表数据
            self.generate_dimension_data()
            
            # 生成事实表数据
            self.generate_fact_data()
            
            # 验证生成的数据
            self.validate_generated_data()
            
            logger.info("所有数据生成完成")
        except Error as e:
            logger.error(f"数据生成失败: {e}")
            raise
        finally:
            self.close()

def main():
    """主函数"""
    try:
        # 创建数据生成器实例
        generator = DataGenerator(DB_CONFIG)
        
        # 生成数据
        generator.generate_data()
        
        logger.info("数据生成成功")
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
