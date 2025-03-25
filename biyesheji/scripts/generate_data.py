import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error

# 设置项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

# 导入配置
from scripts.config import (
    DB_CONFIG, DATA_CONFIG, REGIONS, HOLIDAYS, 
    SEASONAL_PRODUCTS, PRODUCT_POPULARITY,
    USER_BEHAVIOR, PROMOTIONS, INVENTORY_TURNOVER,
    BATCH_SIZE
)

# 导入所有生成器
from scripts.generators.user_generator import UserGenerator
from scripts.generators.supplier_generator import SupplierGenerator
from scripts.generators.category_generator import CategoryGenerator
from scripts.generators.product_generator import ProductGenerator
from scripts.generators.promotion_generator import PromotionGenerator
from scripts.generators.order_generator import OrderGenerator
from scripts.generators.inventory_generator import InventoryGenerator

def setup_database():
    """初始化数据库连接并创建表结构"""
    try:
        # 连接数据库
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 读取并执行SQL文件
        sql_file = os.path.join(PROJECT_ROOT, 'sql', 'create_tables.sql')
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_commands = f.read().split(';')
            for command in sql_commands:
                if command.strip():
                    cursor.execute(command)
            conn.commit()
        
        print("数据库表结构创建成功")
        return True
        
    except Error as e:
        print(f"数据库初始化失败: {e}")
        return False
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def generate_data():
    """生成所有数据"""
    try:
        print("\n=== 开始生成数据 ===\n")
        
        # 从配置中获取数据量
        num_users = DATA_CONFIG['user_count']
        num_suppliers = DATA_CONFIG['supplier_count']
        num_products = DATA_CONFIG['product_count']
        
        # 从配置中获取日期范围
        start_date = datetime.strptime(DATA_CONFIG['date_start'], '%Y-%m-%d')
        end_date = datetime.strptime(DATA_CONFIG['date_end'], '%Y-%m-%d')
        
        # 1. 生成用户数据
        print("\n--- 生成用户数据 ---")
        user_generator = UserGenerator()
        user_data = user_generator.generate_users(num_users)
        user_generator.close_connection()
        print(f"成功生成 {num_users} 个用户")
        
        # 2. 生成供应商数据
        print("\n--- 生成供应商数据 ---")
        supplier_generator = SupplierGenerator()
        supplier_data = supplier_generator.generate_suppliers(num_suppliers)
        supplier_generator.close_connection()
        print(f"成功生成 {num_suppliers} 个供应商")
        
        # 3. 生成商品分类数据
        print("\n--- 生成商品分类数据 ---")
        category_generator = CategoryGenerator()
        category_data = category_generator.generate_categories()
        category_generator.close_connection()
        
        # 4. 生成商品数据
        print("\n--- 生成商品数据 ---")
        product_generator = ProductGenerator()
        product_data = product_generator.generate_products(
            num_products,
            category_data,
            supplier_data
        )
        product_generator.close_connection()
        print(f"成功生成 {num_products} 个商品")
        
        # 5. 生成促销数据
        print("\n--- 生成促销数据 ---")
        product_ids, product_info = product_data
        category_ids, category_seasons = category_data
        promotion_generator = PromotionGenerator(
            product_ids,
            product_info,
            category_seasons
        )
        
        promotion_data = promotion_generator.generate_promotions(
            start_date,
            end_date
        )
        promotion_generator.close_connection()
        
        # 6. 生成订单数据
        print("\n--- 生成订单数据 ---")
        order_generator = OrderGenerator()
        target_order_count = DATA_CONFIG['order_count']
        max_order_details = DATA_CONFIG['max_order_details']
        
        order_data = order_generator.generate_orders(
            start_date,
            end_date,
            user_data,
            product_data,
            promotion_data,
            target_order_count,
            max_order_details
        )
        order_generator.close_connection()
        print(f"成功生成 {target_order_count} 个订单")
        
        # 7. 生成库存和价格历史数据
        inventory_generator = InventoryGenerator(
            product_info,
            order_data,
            category_seasons
        )
        # 生成初始库存和价格记录
        inventory_generator.generate_initial_inventory()
        # 生成订单引起的库存变动
        inventory_generator.generate_order_inventory_changes()
        # 生成季节性价格变动
        inventory_generator.generate_seasonal_price_changes(start_date, end_date)
        
        print("\n=== 数据生成完成 ===\n")
        
        # 打印生成统计信息
        print("\n=== 数据生成统计 ===")
        print(f"- 用户数量: {num_users}")
        print(f"- 供应商数量: {num_suppliers}")
        print(f"- 商品数量: {num_products}")
        print(f"- 订单数量: {target_order_count}")
        print(f"- 数据时间范围: {start_date.date()} 至 {end_date.date()}")
        print("\n数据已准备就绪，可以开始进行分析！")
        
        return True
        
    except Exception as e:
        print(f"\n数据生成过程中发生错误: {e}")
        return False

def main():
    """主函数"""
    # 1. 初始化数据库
    print("\n=== 初始化数据库 ===")
    if not setup_database():
        print("数据库初始化失败，程序退出")
        sys.exit(1)
    
    # 2. 生成数据
    if not generate_data():
        print("数据生成失败，程序退出")
        sys.exit(1)

if __name__ == "__main__":
    main() 