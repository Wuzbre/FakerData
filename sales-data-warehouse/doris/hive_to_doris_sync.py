#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Hive到Doris数据同步工具
用于将Hive的DWD和DWS层数据同步到Doris中，以支持实时分析查询
"""

import os
import sys
import argparse
import logging
import datetime
import subprocess
import pymysql
import pyhive.hive as hive
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'hive_to_doris_sync_{datetime.datetime.now().strftime("%Y%m%d")}.log')
    ]
)
logger = logging.getLogger('hive_to_doris_sync')

# Doris连接配置
DORIS_CONFIG = {
    'host': 'doris-fe',
    'port': 9030,
    'user': 'root',
    'password': '',
    'database': 'purchase_sales_dw'
}

# Hive连接配置
HIVE_CONFIG = {
    'host': 'hadoop-master',
    'port': 10000,
    'username': '',
    'database': 'default'
}

# 同步配置
SYNC_CONFIG = [
    # DWD层同步
    {
        'hive_db': 'purchase_sales_dwd',
        'hive_table': 'dwd_dim_user',
        'doris_db': 'purchase_sales_dw',
        'doris_table': 'dwd_dim_user',
        'partition_field': 'dt',
        'order_by': 'user_id'
    },
    {
        'hive_db': 'purchase_sales_dwd',
        'hive_table': 'dwd_dim_product',
        'doris_db': 'purchase_sales_dw',
        'doris_table': 'dwd_dim_product',
        'partition_field': 'dt',
        'order_by': 'product_id'
    },
    {
        'hive_db': 'purchase_sales_dwd',
        'hive_table': 'dwd_fact_sales_order',
        'doris_db': 'purchase_sales_dw',
        'doris_table': 'dwd_fact_sales_order',
        'partition_field': 'dt',
        'order_by': 'sales_order_id'
    },
    {
        'hive_db': 'purchase_sales_dwd',
        'hive_table': 'dwd_fact_sales_order_item',
        'doris_db': 'purchase_sales_dw',
        'doris_table': 'dwd_fact_sales_order_item',
        'partition_field': 'dt',
        'order_by': 'sales_order_item_id'
    },
    # DWS层同步
    {
        'hive_db': 'purchase_sales_dws',
        'hive_table': 'dws_sales_day_agg',
        'doris_db': 'purchase_sales_dw',
        'doris_table': 'dws_sales_day_agg',
        'partition_field': 'dt',
        'order_by': 'sales_date'
    },
    {
        'hive_db': 'purchase_sales_dws',
        'hive_table': 'dws_sales_month_agg',
        'doris_db': 'purchase_sales_dw',
        'doris_table': 'dws_sales_month_agg',
        'partition_field': 'dt',
        'order_by': 'year_month'
    },
    {
        'hive_db': 'purchase_sales_dws',
        'hive_table': 'dws_product_sales_agg',
        'doris_db': 'purchase_sales_dw',
        'doris_table': 'dws_product_sales_agg',
        'partition_field': 'dt',
        'order_by': 'product_id'
    },
    {
        'hive_db': 'purchase_sales_dws',
        'hive_table': 'dws_customer_behavior_agg',
        'doris_db': 'purchase_sales_dw',
        'doris_table': 'dws_customer_behavior_agg',
        'partition_field': 'dt',
        'order_by': 'user_id'
    }
]

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Hive到Doris数据同步工具')
    parser.add_argument('--date', required=False, help='处理日期，格式yyyyMMdd，默认昨天')
    parser.add_argument('--tables', required=False, help='要同步的表，逗号分隔，默认全部表')
    return parser.parse_args()

def get_hive_connection(db=None):
    """获取Hive连接"""
    conn_params = HIVE_CONFIG.copy()
    if db:
        conn_params['database'] = db
    try:
        return hive.Connection(**conn_params)
    except Exception as e:
        logger.error(f"Hive连接失败: {e}")
        raise

def get_doris_connection():
    """获取Doris连接"""
    try:
        return pymysql.connect(
            host=DORIS_CONFIG['host'],
            port=DORIS_CONFIG['port'],
            user=DORIS_CONFIG['user'],
            password=DORIS_CONFIG['password'],
            database=DORIS_CONFIG['database'],
            charset='utf8mb4'
        )
    except Exception as e:
        logger.error(f"Doris连接失败: {e}")
        raise

def export_hive_data(config, dt):
    """将Hive数据导出到CSV文件"""
    hive_db = config['hive_db']
    hive_table = config['hive_table']
    output_dir = f"/tmp/doris_sync/{hive_db}/{hive_table}/{dt}"
    output_file = f"{output_dir}/{hive_table}.csv"
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 构造Hive查询
    query = f"""
    INSERT OVERWRITE LOCAL DIRECTORY '{output_dir}'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\\001'
    SELECT * FROM {hive_db}.{hive_table}
    WHERE {config['partition_field']} = '{dt}'
    """
    
    # 执行Hive命令
    logger.info(f"从Hive导出数据: {hive_db}.{hive_table} 分区 {dt}")
    try:
        cmd = ['hive', '-e', query]
        subprocess.run(cmd, check=True)
        logger.info(f"导出成功: {output_file}")
        return output_dir
    except subprocess.CalledProcessError as e:
        logger.error(f"导出失败: {e}")
        raise

def import_to_doris(config, data_dir, dt):
    """将数据导入到Doris"""
    doris_db = config['doris_db']
    doris_table = config['doris_table']
    
    # 查找CSV文件
    csv_files = []
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith('.csv') or not '.' in file:  # Hive导出的文件可能没有扩展名
                csv_files.append(os.path.join(root, file))
    
    if not csv_files:
        logger.warning(f"未找到CSV文件: {data_dir}")
        return False
    
    # 构造Doris导入命令
    # 使用stream load方式
    for csv_file in csv_files:
        logger.info(f"导入数据到Doris: {doris_db}.{doris_table} 文件 {csv_file}")
        try:
            # 使用curl命令调用Stream Load导入
            cmd = [
                'curl', '--location-trusted', '-u', f"{DORIS_CONFIG['user']}:{DORIS_CONFIG['password']}",
                '-H', 'column_separator: \\001',
                '-H', f'columns: *,dt="{dt}"',
                '-H', f'partitions: dt="{dt}"',
                '-T', csv_file,
                f'http://{DORIS_CONFIG["host"]}:8030/api/{doris_db}/{doris_table}/_stream_load'
            ]
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            logger.info(f"导入结果: {result.stdout}")
        except subprocess.CalledProcessError as e:
            logger.error(f"导入失败: {e.stderr}")
            raise
    
    return True

def sync_table(config, dt):
    """同步一个表"""
    try:
        # 导出Hive数据
        data_dir = export_hive_data(config, dt)
        
        # 导入Doris
        success = import_to_doris(config, data_dir, dt)
        
        # 清理临时文件
        if success:
            logger.info(f"同步成功: {config['hive_db']}.{config['hive_table']} -> {config['doris_db']}.{config['doris_table']}")
            # 考虑到可能需要重新处理，暂时不删除临时文件
            # subprocess.run(['rm', '-rf', data_dir])
            return True
        else:
            logger.error(f"同步失败: {config['hive_db']}.{config['hive_table']}")
            return False
    except Exception as e:
        logger.error(f"同步出错 {config['hive_db']}.{config['hive_table']}: {e}")
        return False

def main():
    """主函数"""
    args = parse_args()
    
    # 处理日期
    if args.date:
        dt = args.date
    else:
        dt = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    
    logger.info(f"开始Hive到Doris数据同步，处理日期: {dt}")
    
    # 获取要同步的表
    tables_to_sync = []
    if args.tables:
        table_list = args.tables.split(',')
        for config in SYNC_CONFIG:
            if config['hive_table'] in table_list:
                tables_to_sync.append(config)
    else:
        tables_to_sync = SYNC_CONFIG
    
    # 创建Doris数据库和表（如果不存在）
    try:
        create_doris_tables()
    except Exception as e:
        logger.error(f"创建Doris表失败: {e}")
        sys.exit(1)
    
    # 并行同步数据
    results = {}
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_table = {executor.submit(sync_table, config, dt): config for config in tables_to_sync}
        for future in as_completed(future_to_table):
            config = future_to_table[future]
            try:
                success = future.result()
                results[f"{config['hive_db']}.{config['hive_table']}"] = 'Success' if success else 'Failed'
            except Exception as e:
                results[f"{config['hive_db']}.{config['hive_table']}"] = f'Error: {e}'
    
    # 打印同步结果
    logger.info("同步完成，结果如下:")
    for table, result in results.items():
        logger.info(f"{table}: {result}")

def create_doris_tables():
    """创建Doris数据库和表（如果不存在）"""
    conn = get_doris_connection()
    try:
        cursor = conn.cursor()
        
        # 创建数据库
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DORIS_CONFIG['database']}")
        
        # 为每个表创建对应的Doris表
        for config in SYNC_CONFIG:
            hive_db = config['hive_db']
            hive_table = config['hive_table']
            doris_db = config['doris_db']
            doris_table = config['doris_table']
            order_by = config['order_by']
            
            logger.info(f"检查Doris表是否存在: {doris_db}.{doris_table}")
            cursor.execute(f"SHOW TABLES FROM {doris_db} LIKE '{doris_table}'")
            if cursor.fetchone():
                logger.info(f"表已存在: {doris_db}.{doris_table}")
                continue
            
            # 根据表类型和层级创建不同的表结构
            if hive_db.endswith('dwd'):
                if hive_table.startswith('dwd_dim_'):
                    # 维度表
                    create_dim_table(cursor, config)
                elif hive_table.startswith('dwd_fact_'):
                    # 事实表
                    create_fact_table(cursor, config)
            elif hive_db.endswith('dws'):
                # 汇总表
                create_summary_table(cursor, config)
        
        conn.commit()
    except Exception as e:
        logger.error(f"创建Doris表时出错: {e}")
        raise
    finally:
        conn.close()

def create_dim_table(cursor, config):
    """创建维度表"""
    # 这里简化处理，实际应该根据Hive表结构动态生成
    doris_db = config['doris_db']
    doris_table = config['doris_table']
    order_by = config['order_by']
    
    # 用户维度表
    if doris_table == 'dwd_dim_user':
        sql = f"""
        CREATE TABLE IF NOT EXISTS {doris_db}.{doris_table} (
            user_id INT,
            username VARCHAR(255),
            email VARCHAR(255),
            phone VARCHAR(255),
            nickname VARCHAR(255),
            status VARCHAR(255),
            is_verified BOOLEAN,
            role VARCHAR(255),
            created_date VARCHAR(255),
            updated_date VARCHAR(255),
            last_login_date VARCHAR(255),
            membership_level VARCHAR(255),
            preferred_language VARCHAR(255),
            preferred_currency VARCHAR(255),
            shipping_address VARCHAR(255),
            billing_address VARCHAR(255),
            newsletter_subscribed BOOLEAN,
            referral_code VARCHAR(255),
            active_flag BOOLEAN,
            start_date VARCHAR(255),
            end_date VARCHAR(255),
            current_flag BOOLEAN,
            etl_time DATETIME,
            dt VARCHAR(255)
        )
        UNIQUE KEY(user_id, dt)
        DISTRIBUTED BY HASH(user_id) BUCKETS 10
        PROPERTIES (
            "replication_num" = "3"
        );
        """
    # 产品维度表
    elif doris_table == 'dwd_dim_product':
        sql = f"""
        CREATE TABLE IF NOT EXISTS {doris_db}.{doris_table} (
            product_id INT,
            product_name VARCHAR(255),
            product_description VARCHAR(255),
            sku VARCHAR(255),
            category_id INT,
            brand VARCHAR(255),
            model VARCHAR(255),
            color VARCHAR(255),
            price DECIMAL(10,2),
            cost_price DECIMAL(10,2),
            discount_price DECIMAL(10,2),
            profit_margin DECIMAL(10,4),
            currency VARCHAR(255),
            status VARCHAR(255),
            launch_date VARCHAR(255),
            discontinued_date VARCHAR(255),
            supplier_id INT,
            manufacturer VARCHAR(255),
            country_of_origin VARCHAR(255),
            start_date VARCHAR(255),
            end_date VARCHAR(255),
            current_flag BOOLEAN,
            etl_time DATETIME,
            dt VARCHAR(255)
        )
        UNIQUE KEY(product_id, dt)
        DISTRIBUTED BY HASH(product_id) BUCKETS 10
        PROPERTIES (
            "replication_num" = "3"
        );
        """
    else:
        logger.error(f"不支持的维度表: {doris_table}")
        return
    
    logger.info(f"创建Doris维度表: {doris_db}.{doris_table}")
    cursor.execute(sql)

def create_fact_table(cursor, config):
    """创建事实表"""
    doris_db = config['doris_db']
    doris_table = config['doris_table']
    order_by = config['order_by']
    
    # 销售订单事实表
    if doris_table == 'dwd_fact_sales_order':
        sql = f"""
        CREATE TABLE IF NOT EXISTS {doris_db}.{doris_table} (
            sales_order_id INT,
            user_id INT,
            created_by INT,
            order_date VARCHAR(255),
            order_year INT,
            order_month INT,
            order_day INT,
            order_hour INT,
            expected_delivery_date VARCHAR(255),
            actual_delivery_date VARCHAR(255),
            shipping_method VARCHAR(255),
            shipping_cost DECIMAL(10,2),
            status VARCHAR(255),
            total_amount DECIMAL(12,2),
            currency VARCHAR(255),
            discount DECIMAL(10,2),
            final_amount DECIMAL(12,2),
            payment_status VARCHAR(255),
            payment_method VARCHAR(255),
            payment_date VARCHAR(255),
            created_date VARCHAR(255),
            delivery_days INT,
            is_delayed BOOLEAN,
            etl_time DATETIME,
            dt VARCHAR(255)
        )
        UNIQUE KEY(sales_order_id, dt)
        DISTRIBUTED BY HASH(sales_order_id) BUCKETS 20
        PROPERTIES (
            "replication_num" = "3"
        );
        """
    # 销售订单明细事实表
    elif doris_table == 'dwd_fact_sales_order_item':
        sql = f"""
        CREATE TABLE IF NOT EXISTS {doris_db}.{doris_table} (
            sales_order_item_id INT,
            sales_order_id INT,
            product_id INT,
            quantity INT,
            unit_price DECIMAL(10,2),
            total_price DECIMAL(12,2),
            discount DECIMAL(10,2),
            final_price DECIMAL(12,2),
            order_date VARCHAR(255),
            expected_delivery_date VARCHAR(255),
            actual_delivery_date VARCHAR(255),
            status VARCHAR(255),
            warehouse_location VARCHAR(255),
            profit DECIMAL(12,2),
            etl_time DATETIME,
            dt VARCHAR(255)
        )
        UNIQUE KEY(sales_order_item_id, dt)
        DISTRIBUTED BY HASH(sales_order_item_id) BUCKETS 20
        PROPERTIES (
            "replication_num" = "3"
        );
        """
    else:
        logger.error(f"不支持的事实表: {doris_table}")
        return
    
    logger.info(f"创建Doris事实表: {doris_db}.{doris_table}")
    cursor.execute(sql)

def create_summary_table(cursor, config):
    """创建汇总表"""
    doris_db = config['doris_db']
    doris_table = config['doris_table']
    order_by = config['order_by']
    
    # 销售日汇总表
    if doris_table == 'dws_sales_day_agg':
        sql = f"""
        CREATE TABLE IF NOT EXISTS {doris_db}.{doris_table} (
            sales_date VARCHAR(255),
            year INT,
            month INT,
            day INT,
            total_orders INT,
            successful_orders INT,
            cancelled_orders INT,
            total_sales DECIMAL(20,2),
            total_discount DECIMAL(20,2),
            net_sales DECIMAL(20,2),
            total_profit DECIMAL(20,2),
            profit_margin DECIMAL(10,4),
            total_customers INT,
            new_customers INT,
            returning_customers INT,
            total_products INT,
            avg_order_value DECIMAL(12,2),
            etl_time DATETIME,
            dt VARCHAR(255)
        )
        UNIQUE KEY(sales_date, dt)
        DISTRIBUTED BY HASH(sales_date) BUCKETS 10
        PROPERTIES (
            "replication_num" = "3"
        );
        """
    # 销售月汇总表
    elif doris_table == 'dws_sales_month_agg':
        sql = f"""
        CREATE TABLE IF NOT EXISTS {doris_db}.{doris_table} (
            year_month VARCHAR(255),
            year INT,
            month INT,
            total_orders INT,
            successful_orders INT,
            cancelled_orders INT,
            total_sales DECIMAL(20,2),
            total_discount DECIMAL(20,2),
            net_sales DECIMAL(20,2),
            total_profit DECIMAL(20,2),
            profit_margin DECIMAL(10,4),
            total_customers INT,
            new_customers INT,
            returning_customers INT,
            total_products INT,
            avg_order_value DECIMAL(12,2),
            month_over_month_growth DECIMAL(10,4),
            etl_time DATETIME,
            dt VARCHAR(255)
        )
        UNIQUE KEY(year_month, dt)
        DISTRIBUTED BY HASH(year_month) BUCKETS 5
        PROPERTIES (
            "replication_num" = "3"
        );
        """
    # 产品销售汇总表
    elif doris_table == 'dws_product_sales_agg':
        sql = f"""
        CREATE TABLE IF NOT EXISTS {doris_db}.{doris_table} (
            product_id INT,
            product_name VARCHAR(255),
            brand VARCHAR(255),
            category_id INT,
            total_quantity INT,
            total_sales DECIMAL(20,2),
            total_profit DECIMAL(20,2),
            profit_margin DECIMAL(10,4),
            avg_unit_price DECIMAL(10,2),
            max_unit_price DECIMAL(10,2),
            min_unit_price DECIMAL(10,2),
            total_discount DECIMAL(10,2),
            discount_rate DECIMAL(10,4),
            etl_time DATETIME,
            dt VARCHAR(255)
        )
        UNIQUE KEY(product_id, dt)
        DISTRIBUTED BY HASH(product_id) BUCKETS 10
        PROPERTIES (
            "replication_num" = "3"
        );
        """
    # 客户购买行为汇总表
    elif doris_table == 'dws_customer_behavior_agg':
        sql = f"""
        CREATE TABLE IF NOT EXISTS {doris_db}.{doris_table} (
            user_id INT,
            username VARCHAR(255),
            total_orders INT,
            first_order_date VARCHAR(255),
            last_order_date VARCHAR(255),
            total_spend DECIMAL(20,2),
            avg_order_value DECIMAL(12,2),
            purchase_frequency DECIMAL(10,4),
            most_purchased_product_id INT,
            most_purchased_product_name VARCHAR(255),
            most_purchased_category_id INT,
            avg_days_between_orders DECIMAL(10,2),
            customer_lifetime_value DECIMAL(20,2),
            is_active BOOLEAN,
            membership_level VARCHAR(255),
            etl_time DATETIME,
            dt VARCHAR(255)
        )
        UNIQUE KEY(user_id, dt)
        DISTRIBUTED BY HASH(user_id) BUCKETS 10
        PROPERTIES (
            "replication_num" = "3"
        );
        """
    else:
        logger.error(f"不支持的汇总表: {doris_table}")
        return
    
    logger.info(f"创建Doris汇总表: {doris_db}.{doris_table}")
    cursor.execute(sql)

if __name__ == '__main__':
    main() 