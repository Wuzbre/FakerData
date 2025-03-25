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
import pymysql
from pymysql import Error
from pymysql.cursors import DictCursor
import pandas as pd
import numpy as np
from faker import Faker
from tqdm import tqdm
from loguru import logger
from dbutils.pooled_db import PooledDB
from typing import List, Dict, Any, Optional, Generator
import holidays
import json
import re
import logging
from pathlib import Path
import yaml
from chinese_calendar import is_holiday
import decimal

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
    USER_PURCHASE_FREQUENCY, PRODUCT_CATEGORIES,
    ELECTRONICS_SUBCATEGORIES, CLOTHING_SUBCATEGORIES,
    HOME_SUBCATEGORIES, FOOD_SUBCATEGORIES, BOOK_SUBCATEGORIES
)

# 读取配置文件，创建DB_CONFIG
config_path = os.path.join(PROJECT_ROOT, 'config.yaml')
with open(config_path, 'r', encoding='utf-8') as f:
    config_data = yaml.safe_load(f)
    
# 创建DB_CONFIG
DB_CONFIG = config_data['database']

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
    """连接到MySQL数据库，使用pymysql"""
    try:
        connection = pymysql.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            port=DB_CONFIG['port'],
            charset='utf8mb4',
            cursorclass=DictCursor
        )
        
        cursor = connection.cursor()
        # 创建数据库（如果不存在）
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
        tables = cursor.fetchall()
        
        # 清空所有表
        for table_row in tables:
            table = table_row[0] if isinstance(table_row, tuple) else list(table_row.values())[0]
            try:
                cursor.execute(f"TRUNCATE TABLE {table}")
                logger.info(f"表 {table} 已清空")
            except Exception as e:
                logger.warning(f"清空表 {table} 时发生错误: {e}")
        
        # 重新启用外键约束
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        connection.commit()
        logger.info("所有表已清空")
    
    except Exception as e:
        logger.error(f"清空数据库时发生错误: {e}")
        connection.rollback()
        raise

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
            
            # 确保所有必需的字段都存在
            required_fields = ['host', 'user', 'password', 'database']
            for field in required_fields:
                if field not in db_config:
                    raise ValueError(f"数据库配置缺少必需字段: {field}")
            
            pool_config = {
                'creator': pymysql,  # 使用pymysql作为连接创建器
                'maxconnections': db_config.get('pool_size', 5),
                'host': db_config.get('host'),
                'port': db_config.get('port'),
                'user': db_config.get('user'),
                'password': db_config.get('password'),
                'database': db_config.get('database'),
                'charset': db_config.get('charset', 'utf8mb4'),
                'cursorclass': DictCursor
            }
            
            logger.debug(f"初始化数据库连接池: {pool_config['host']}:{pool_config['port']}/{pool_config['database']}")
            
            self.pool = PooledDB(**pool_config)
            logger.info("数据库连接池初始化成功")
        except Exception as e:
            logger.error(f"初始化数据库连接池失败: {e}")
            raise

    def _get_connection(self) -> Optional[pymysql.connections.Connection]:
        """获取数据库连接"""
        try:
            return self.pool.connection()
        except Exception as e:
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
        holiday_flag = is_holiday(current_date.date())
        
        # 获取当前季节
        month = current_date.month
        season = (month - 1) // 3 + 1
        
        # 调整因子
        holiday_factor = 1.0
        season_factor = 1.0
        day_of_week_factor = 1.0
        
        # 节假日调整
        holiday_factors = self.config.get('generation', 'holiday_factors')
        if holiday_flag:
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
        try:
            # 验证表结构是否存在
            self._check_tables(connection)
            
            current_date = self.start_time
            total_days = (self.end_time - self.start_time).days + 1
            commit_interval = self.config.get('performance', 'commit_interval', default=7)
            
            logger.info(f"开始生成数据: 从{self.start_time.date()}到{self.end_time.date()}, 共{total_days}天")
            
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
                        logger.debug(f"提交事务: {current_date.date()}")
            
            connection.commit()
            logger.info("所有数据已提交到数据库")
            
            # 输出最终统计信息
            self._print_stats()
            
        except Exception as e:
            logger.error(f"数据生成过程中发生错误: {e}")
            connection.rollback()
            raise
    
    def _check_tables(self, connection):
        """检查必要的表是否存在"""
        required_tables = ['users', 'suppliers', 'products', 'orders', 'order_items', 'inventory', 'inventory_transactions']
        
        try:
            cursor = connection.cursor()
            cursor.execute("SHOW TABLES")
            tables_result = cursor.fetchall()
            
            # 处理DictCursor返回的结果
            existing_tables = []
            for row in tables_result:
                # 如果是字典（DictCursor）
                if isinstance(row, dict):
                    existing_tables.append(list(row.values())[0])
                # 如果是元组（常规cursor）
                else:
                    existing_tables.append(row[0])
            
            missing_tables = [table for table in required_tables if table not in existing_tables]
            
            if missing_tables:
                logger.error(f"缺少必要的表: {', '.join(missing_tables)}")
                raise ValueError(f"数据库缺少必要的表，请先创建表结构")
                
            logger.info(f"所有必要的表都已存在: {', '.join(required_tables)}")
            
        except Exception as e:
            logger.error(f"检查表结构时发生错误: {e}")
            raise

    def _generate_daily_data(self, connection, current_date):
        """生成每日数据"""
        # 调整当日数据量
        self._adjust_data_volume(connection, current_date)
        
        try:
            cursor = connection.cursor()
            
            # 1. 更新产品数据
            self._update_products(connection, current_date)
            
            # 检查用户表是否有数据
            cursor.execute("SELECT COUNT(*) as count FROM users")
            user_count = cursor.fetchone()['count']
            
            if user_count < self.daily_metrics.get('users', 500):
                # 生成用户数据
                generate_users(connection, self.daily_metrics.get('users', 500))
                self.stats['generated']['users'] += self.daily_metrics.get('users', 500)
            
            # 检查商品表是否有数据
            cursor.execute("SELECT COUNT(*) as count FROM products")
            product_count = cursor.fetchone()['count']
            
            if product_count < self.daily_metrics.get('products', 200):
                # 生成供应商数据
                generate_suppliers(connection, 50)
                # 生成商品数据
                generate_products(connection, self.daily_metrics.get('products', 200))
                self.stats['generated']['products'] += self.daily_metrics.get('products', 200)
            
            # 获取用户和商品ID
            cursor.execute("SELECT user_id FROM users ORDER BY RAND() LIMIT 100")
            user_ids = [row['user_id'] for row in cursor.fetchall()]
            
            if not user_ids:
                logger.warning("没有找到用户数据，无法生成订单")
                return
                
            cursor.execute("SELECT product_id, price, category FROM products WHERE is_active = 1 ORDER BY RAND() LIMIT 200")
            products = cursor.fetchall()
            
            if not products:
                logger.warning("没有找到商品数据，无法生成订单")
                return
                
            # 转换所有Decimal类型为float，避免类型错误
            for p in products:
                if isinstance(p['price'], decimal.Decimal):
                    p['price'] = float(p['price'])
                
            # 生成订单数据
            order_count = 0
            inventory_update_count = 0
            
            for _ in range(self.daily_metrics['orders']):
                try:
                    # 开始订单事务
                    cursor = connection.cursor()
                    
                    # 随机选择用户
                    user_id = random.choice(user_ids)
                    
                    # 生成订单基本信息
                    order_date = self.fake.date_time_between(
                        start_date=current_date, 
                        end_date=current_date + timedelta(days=1)
                    )
                    
                    # 随机选择支付方式 - 转换浮点数权重为整数权重
                    payment_methods = list(PAYMENT_METHODS.keys())
                    payment_weights = [int(weight * 1000) for weight in PAYMENT_METHODS.values()]  # 转换为整数
                    payment_method = random.choices(payment_methods, weights=payment_weights, k=1)[0]
                    
                    # 获取用户的地址信息
                    cursor.execute("SELECT province, city, address, region FROM users WHERE user_id = %s", (user_id,))
                    user_info = cursor.fetchone()
                    
                    # 计算订单中商品数量 (1-5个商品)
                    num_products = random.randint(1, 5)
                    order_products = random.sample(products, min(num_products, len(products)))
                    
                    # 计算订单总金额
                    total_amount = 0.0
                    total_discount = 0.0
                    shipping_fee = 0.0
                    
                    # 预先计算每个订单项
                    order_items = []
                    for product in order_products:
                        quantity = random.randint(1, 3)
                        price = float(product['price'])
                        item_discount = 0.0
                        
                        # 有10%几率有单品折扣
                        if random.random() < 0.1:
                            item_discount = round(price * random.uniform(0.05, 0.15), 2)
                            
                        item_total = (price - item_discount) * quantity
                        total_amount += item_total
                        
                        order_items.append({
                            'product_id': product['product_id'],
                            'quantity': quantity,
                            'price': price,
                            'discount': item_discount,
                            'total_price': item_total,
                            'is_gift': random.random() < 0.05  # 5%几率是赠品
                        })
                    
                    # 应用整单折扣 (0-20%)
                    order_discount_rate = random.uniform(0, 0.2)
                    total_discount = round(total_amount * order_discount_rate, 2)
                    total_amount = round(total_amount - total_discount, 2)
                    
                    # 添加运费（订单金额低于100元需要运费）
                    if total_amount < 100:
                        shipping_fee = round(random.uniform(5, 15), 2)
                        total_amount += shipping_fee
                    
                    # 生成订单状态
                    status = '已下单'
                    
                    # 根据订单日期和当前日期计算可能的状态变化
                    days_passed = (datetime.now().date() - order_date.date()).days
                    if days_passed > 0:
                        if random.random() < ORDER_STATUS['已支付']:
                            status = '已支付'
                            if days_passed > 1 and random.random() < ORDER_STATUS['已发货']:
                                status = '已发货'
                                if days_passed > 3 and random.random() < ORDER_STATUS['已完成']:
                                    status = '已完成'
                                    if days_passed > 10 and random.random() < ORDER_STATUS['已退货']:
                                        status = '已退货'
                        elif random.random() < ORDER_STATUS['已取消']:
                            status = '已取消'
                    
                    # 生成发货日期（如果已发货或更高状态）
                    delivery_date = None
                    if status in ['已发货', '已完成', '已退货']:
                        delivery_date = self.fake.date_time_between(
                            start_date=order_date,
                            end_date=order_date + timedelta(days=3)
                        )
                    
                    # 插入订单记录
                    order_sql = """
                    INSERT INTO orders (
                        user_id, order_date, status, payment_method, shipping_fee, 
                        total_amount, discount, shipping_address, shipping_city, 
                        shipping_province, shipping_region, shipping_postal_code, delivery_date
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    
                    order_values = (
                        user_id, order_date, status, payment_method, shipping_fee, 
                        total_amount, total_discount, user_info['address'], user_info['city'],
                        user_info['province'], user_info['region'], self.fake.postcode(), delivery_date
                    )
                    
                    cursor.execute(order_sql, order_values)
                    
                    # 获取新插入的订单ID
                    cursor.execute("SELECT LAST_INSERT_ID() as order_id")
                    order_id = cursor.fetchone()['order_id']
                    
                    # 插入订单项
                    for item in order_items:
                        item_sql = """
                        INSERT INTO order_items (
                            order_id, product_id, quantity, price, 
                            discount, total_price, is_gift
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s
                        )
                        """
                        
                        item_values = (
                            order_id, item['product_id'], item['quantity'], 
                            item['price'], item['discount'], item['total_price'], 
                            item['is_gift']
                        )
                        
                        cursor.execute(item_sql, item_values)
                        
                        # 如果订单状态不是已取消，则生成库存交易记录
                        if status != '已取消':
                            inventory_sql = """
                            INSERT INTO inventory_transactions (
                                product_id, transaction_type, quantity, 
                                transaction_date, order_id, notes
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s
                            )
                            """
                            
                            inventory_values = (
                                item['product_id'], '出库', -item['quantity'],
                                order_date, order_id, f'订单出库: {order_id}'
                            )
                            
                            cursor.execute(inventory_sql, inventory_values)
                            inventory_update_count += 1
                    
                    # 提交事务
                    connection.commit()
                    order_count += 1
                    
                except Exception as e:
                    logger.error(f"生成订单失败: {e}")
                    connection.rollback()
                    self.stats['errors'].append(str(e))
            
            # 生成额外的库存交易记录（入库、调整等）
            remaining_updates = self.daily_metrics['inventory_updates'] - inventory_update_count
            if remaining_updates > 0 and products:
                for _ in range(remaining_updates):
                    try:
                        # 随机选择一个商品
                        product = random.choice(products)
                        
                        # 70%是入库，30%是库存调整 - 将浮点数权重转换为整数
                        transaction_type = random.choices(['入库', '调整'], weights=[700, 300], k=1)[0]
                        
                        if transaction_type == '入库':
                            quantity = random.randint(10, 100)
                        else:  # 库存调整
                            quantity = random.randint(-10, 10)
                            # 避免将库存调整为负数
                            if quantity < 0:
                                # 检查当前库存
                                cursor.execute("""
                                    SELECT SUM(quantity) as current_stock 
                                    FROM inventory_transactions 
                                    WHERE product_id = %s
                                """, (product['product_id'],))
                                result = cursor.fetchone()
                                current_stock = result['current_stock'] if result['current_stock'] is not None else 0
                                
                                # 确保调整后库存不为负
                                if current_stock + quantity < 0:
                                    quantity = -current_stock if current_stock > 0 else 0
                        
                        if quantity != 0:  # 只在数量不为0时插入记录
                            inventory_sql = """
                            INSERT INTO inventory_transactions (
                                product_id, transaction_type, quantity, 
                                transaction_date, notes
                            ) VALUES (
                                %s, %s, %s, %s, %s
                            )
                            """
                            
                            transaction_date = self.fake.date_time_between(
                                start_date=current_date,
                                end_date=current_date + timedelta(days=1)
                            )
                            
                            inventory_values = (
                                product['product_id'], transaction_type, quantity,
                                transaction_date, f'{transaction_type}: {quantity}'
                            )
                            
                            cursor.execute(inventory_sql, inventory_values)
                            inventory_update_count += 1
                    
                    except Exception as e:
                        logger.error(f"生成库存交易记录失败: {e}")
                        connection.rollback()
                        self.stats['errors'].append(str(e))
                
                # 提交剩余的库存交易记录
                connection.commit()
            
            # 更新库存表
            self._update_inventory(connection)
            
            # 更新统计信息
            self.stats['generated']['orders'] += order_count
            self.stats['generated']['inventory_updates'] += inventory_update_count
            
            logger.debug(f"日期：{current_date.date()} - 生成订单：{order_count}，更新库存：{inventory_update_count}")
            
        except Exception as e:
            logger.error(f"生成每日数据失败: {e}")
            connection.rollback()
            self.stats['errors'].append(str(e))

    def _update_inventory(self, connection):
        """根据库存交易记录更新库存表"""
        try:
            cursor = connection.cursor()
            
            # 查询所有产品ID
            cursor.execute("SELECT product_id FROM products WHERE is_active = 1")
            product_ids = [row['product_id'] for row in cursor.fetchall()]
            
            for product_id in product_ids:
                # 计算库存数量
                cursor.execute("""
                    SELECT SUM(quantity) as total_quantity 
                    FROM inventory_transactions 
                    WHERE product_id = %s
                """, (product_id,))
                result = cursor.fetchone()
                current_quantity = result['total_quantity'] if result['total_quantity'] is not None else 0
                
                # 查询库存表中是否已有该产品记录
                cursor.execute("SELECT inventory_id FROM inventory WHERE product_id = %s", (product_id,))
                inventory_exists = cursor.fetchone()
                
                # 设置低库存阈值和再订购数量
                low_stock_threshold = random.randint(5, 20)
                reorder_quantity = random.randint(20, 100)
                
                if inventory_exists:
                    # 更新库存记录
                    cursor.execute("""
                        UPDATE inventory 
                        SET quantity = %s, 
                            low_stock_threshold = %s, 
                            reorder_quantity = %s,
                            updated_at = NOW()
                        WHERE product_id = %s
                    """, (current_quantity, low_stock_threshold, reorder_quantity, product_id))
                else:
                    # 插入新库存记录
                    cursor.execute("""
                        INSERT INTO inventory 
                        (product_id, quantity, low_stock_threshold, reorder_quantity, warehouse, last_restock_date) 
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        product_id, 
                        current_quantity, 
                        low_stock_threshold, 
                        reorder_quantity, 
                        "主仓库",
                        datetime.now()
                    ))
            
            connection.commit()
            
        except Error as e:
            logger.error(f"更新库存失败: {e}")
            connection.rollback()

    def _print_stats(self):
        """打印统计信息"""
        logger.info("数据生成统计信息:")
        for key, value in self.stats['generated'].items():
            logger.info(f"- 生成{key}: {value}条")
        
        for key, value in self.stats['updated'].items():
            logger.info(f"- 更新{key}: {value}条")
        
        if self.stats['errors']:
            logger.warning(f"期间发生{len(self.stats['errors'])}个错误")

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
    
    # 已生成的用户名，避免唯一键冲突
    generated_usernames = set()
    generated_emails = set()
    
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
        
        # 生成唯一的用户名
        username = fake.user_name()
        attempts = 0
        while username in generated_usernames and attempts < 10:
            username = fake.user_name() + str(random.randint(1, 9999))
            attempts += 1
        generated_usernames.add(username)
        
        # 生成唯一的邮箱
        email = fake.email()
        attempts = 0
        while email in generated_emails and attempts < 10:
            email = fake.email()
            attempts += 1
        generated_emails.add(email)
        
        user = {
            'username': username,
            'password': fake.password(),
            'email': email,
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
    supplier_result = cursor.fetchall()
    
    # 适配DictCursor返回的结果
    supplier_ids = []
    for row in supplier_result:
        if isinstance(row, dict):
            supplier_ids.append(row['supplier_id'])
        else:
            supplier_ids.append(row[0])
    
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
    
    # 已生成的SKU，避免唯一键冲突
    generated_skus = set()
    
    product_id = 1
    
    for i, category in enumerate(categories):
        # 根据类别确定对应的子类别变量名
        if category == '电子产品':
            subcategory_dict = ELECTRONICS_SUBCATEGORIES
        elif category == '服装' or category == '服装类':
            subcategory_dict = CLOTHING_SUBCATEGORIES
        elif category == '家居' or category == '家居用品':
            subcategory_dict = HOME_SUBCATEGORIES
        elif category == '食品':
            subcategory_dict = FOOD_SUBCATEGORIES
        elif category == '图书':
            subcategory_dict = BOOK_SUBCATEGORIES
        else:
            # 如果没有对应的子类别，创建一个默认子类别
            subcategory_dict = {"默认子类别": 1.0}
            
        subcategories = list(subcategory_dict.keys())
        subcategory_weights = list(subcategory_dict.values())
        
        # 按子类别权重分配
        for j in range(product_counts[i]):
            subcategory = random.choices(subcategories, weights=subcategory_weights, k=1)[0]
            
            # 随机生成创建时间
            create_time = fake.date_time_between(start_date=create_time_min, end_date=now)
            
            # 随机选择供应商
            supplier = random.choice(supplier_ids)
            
            # 确定价格范围
            price_min, price_max = 50, 500  # 默认价格范围
            
            # 根据类别设置适当价格范围
            if category == '电子产品':
                price_min, price_max = 500, 5000
            elif category == '服装' or category == '服装类':
                price_min, price_max = 100, 1000
            elif category == '家居' or category == '家居用品':
                price_min, price_max = 200, 2000
            elif category == '食品':
                price_min, price_max = 10, 200
            elif category == '图书':
                price_min, price_max = 20, 100
                
            price = round(random.uniform(price_min, price_max), 2)
            cost = round(price * random.uniform(0.5, 0.8), 2)  # 成本是售价的50%-80%
            
            # 根据商品类别生成重量和尺寸
            if category == '电子产品':
                weight = round(random.uniform(0.2, 15.0), 2)
                dimensions = f"{random.randint(10, 100)}x{random.randint(10, 100)}x{random.randint(5, 50)}"
            elif category == '服装' or category == '服装类':
                weight = round(random.uniform(0.1, 2.0), 2)
                dimensions = random.choice(['S', 'M', 'L', 'XL', 'XXL'])
            elif category == '食品':
                weight = round(random.uniform(0.05, 5.0), 2)
                dimensions = f"{random.randint(5, 30)}x{random.randint(5, 30)}x{random.randint(2, 20)}"
            else:  # 其他类别
                weight = round(random.uniform(0.2, 3.0), 2)
                dimensions = f"{random.randint(15, 30)}x{random.randint(10, 25)}x{random.randint(1, 5)}"
            
            # 生成唯一的SKU
            sku = f"SKU-{category[:2]}-{subcategory[:2]}-{product_id:06d}"
            while sku in generated_skus:
                product_id += 1
                sku = f"SKU-{category[:2]}-{subcategory[:2]}-{product_id:06d}"
            generated_skus.add(sku)
            
            # 生成产品名称，使用品牌和描述词增加多样性
            brand = ""
            descriptor = ""
            
            # 获取对应类别的品牌（如果存在）
            for brand_category, brand_list in BRANDS.items():
                if category in brand_category or brand_category in category:
                    brand = random.choice(brand_list)
                    break
                    
            # 获取对应类别的描述词（如果存在）
            for desc_category, desc_list in DESCRIPTORS.items():
                if category in desc_category or desc_category in category:
                    descriptor = random.choice(desc_list)
                    break
                
            product_name = f"{brand} {descriptor} {subcategory} {fake.word()} {product_id}".strip()
            
            product = {
                'product_name': product_name,
                'category': category,
                'subcategory': subcategory,
                'description': fake.paragraph(),
                'price': price,
                'cost': cost,
                'supplier': supplier,
                'sku': sku,
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
        INSERT INTO products (product_name, category, subcategory, description, price, cost, supplier, 
                            sku, weight, dimensions, is_active, create_time)
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

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="销售数据仓库系统数据生成工具")
    parser.add_argument("--start_date", required=False, default="2023-01-01",
                        help="数据开始日期，格式YYYY-MM-DD，默认为2023-01-01")
    parser.add_argument("--end_date", required=False, default="2023-12-31",
                        help="数据结束日期，格式YYYY-MM-DD，默认为2023-12-31")
    parser.add_argument("--scale", required=False, type=float, default=0.01,
                        help="数据量比例因子，范围0.001-1.0，默认为0.01")
    parser.add_argument("--config", required=False, default="config.yaml",
                        help="配置文件路径，默认为config.yaml")
    parser.add_argument("--clear", action="store_true", default=False,
                        help="清空现有数据")
    
    try:
        args = parser.parse_args()
        
        # 解析日期和比例因子
        try:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
            
            # 确保日期在chinese_calendar支持的范围内
            if start_date.year > 2023 or end_date.year > 2023:
                logger.warning("chinese_calendar库只支持2004-2023年的日期，将使用2023年的数据")
                # 调整为2023年的对应日期
                start_date = datetime(2023, start_date.month, start_date.day)
                end_date = datetime(2023, end_date.month, end_date.day)
                # 确保开始日期在结束日期之前
                if start_date > end_date:
                    end_date = start_date + timedelta(days=30)  # 默认生成1个月的数据
                    
            # 限制日期范围不超过一年
            max_days = 365
            if (end_date - start_date).days > max_days:
                logger.warning(f"日期范围超过{max_days}天，将被限制为{max_days}天")
                end_date = start_date + timedelta(days=max_days)
            
            scale_factor = max(0.001, min(args.scale, 1.0))  # 限制在0.001-1.0范围内
        except ValueError as e:
            logger.error(f"日期格式错误: {e}")
            sys.exit(1)
        
        # 加载配置
        config_path = os.path.join(PROJECT_ROOT, args.config)
        try:
            config = Config(config_path)
            logger.info(f"已加载配置文件: {config_path}")
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            sys.exit(1)
        
        # 连接数据库
        try:
            connection = connect_to_database()
            if not connection:
                logger.error("数据库连接失败，程序退出")
                sys.exit(1)
                
            logger.info("数据库连接成功")
        except Exception as e:
            logger.error(f"连接数据库时发生错误: {e}")
            sys.exit(1)
        
        # 清空数据
        if args.clear:
            try:
                clear_database(connection)
            except Exception as e:
                logger.error(f"清空数据库时发生错误: {e}")
                sys.exit(1)
        
        # 创建数据模拟器
        try:
            simulator = DataSimulator(config, start_date, end_date, scale_factor)
            logger.info(f"数据模拟器已创建，时间范围: {start_date.date()} 至 {end_date.date()}, 缩放因子: {scale_factor}")
            simulator.generate_all_data(connection)
        except Exception as e:
            logger.error(f"数据生成失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            sys.exit(1)
        finally:
            if connection:
                connection.close()
                logger.info("数据库连接已关闭")
        
        logger.info("数据生成完成")
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
