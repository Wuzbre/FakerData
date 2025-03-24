#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import subprocess
import argparse
from datetime import datetime, timedelta
import logging

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('datax_jobs.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# DataX路径
DATAX_HOME = "/opt/module/datax"
# 配置文件目录
CONFIG_DIR = "./jobs"

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='DataX作业批量运行工具')
    parser.add_argument('-d', '--date', help='指定数据日期(yyyy-MM-dd), 默认为当天', default=datetime.now().strftime('%Y-%m-%d'))
    parser.add_argument('-j', '--job', help='指定要运行的作业配置文件(不带路径和扩展名)', default=None)
    parser.add_argument('-a', '--all', help='运行所有作业', action='store_true')
    return parser.parse_args()

def get_job_list():
    """获取所有作业配置文件"""
    if not os.path.exists(CONFIG_DIR):
        logger.error(f"配置目录不存在: {CONFIG_DIR}")
        return []
    
    return [f for f in os.listdir(CONFIG_DIR) if f.endswith('.json')]

def replace_parameters(config_file, date):
    """替换配置文件中的日期参数"""
    with open(config_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    dt = datetime.strptime(date, '%Y-%m-%d')
    yesterday = (dt - timedelta(days=1)).strftime('%Y-%m-%d')
    today = dt.strftime('%Y%m%d')
    
    content = content.replace('${yesterday}', yesterday)
    content = content.replace('${today}', today)
    
    temp_file = f"{config_file}.tmp"
    with open(temp_file, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return temp_file

def run_datax_job(job_file, date):
    """运行指定的DataX作业"""
    if not os.path.exists(job_file):
        logger.error(f"作业配置文件不存在: {job_file}")
        return False
    
    logger.info(f"开始处理作业: {os.path.basename(job_file)}, 日期: {date}")
    
    # 替换参数
    temp_file = replace_parameters(job_file, date)
    
    # 构建DataX命令
    cmd = f"python {DATAX_HOME}/bin/datax.py {temp_file}"
    
    try:
        # 运行DataX作业
        logger.info(f"执行命令: {cmd}")
        start_time = time.time()
        
        process = subprocess.Popen(
            cmd, 
            shell=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE
        )
        
        # 实时获取输出
        for line in iter(process.stdout.readline, b''):
            logger.info(line.decode().strip())
        
        process.wait()
        end_time = time.time()
        
        # 删除临时文件
        if os.path.exists(temp_file):
            os.remove(temp_file)
        
        if process.returncode == 0:
            logger.info(f"作业 {os.path.basename(job_file)} 执行成功, 耗时: {end_time - start_time:.2f}秒")
            return True
        else:
            logger.error(f"作业 {os.path.basename(job_file)} 执行失败, 返回码: {process.returncode}")
            # 输出错误信息
            for line in iter(process.stderr.readline, b''):
                logger.error(line.decode().strip())
            return False
            
    except Exception as e:
        logger.error(f"执行作业时出错: {str(e)}")
        
        # 删除临时文件
        if os.path.exists(temp_file):
            os.remove(temp_file)
        
        return False

def run_jobs(job_name=None, date=None, run_all=False):
    """运行DataX作业"""
    if job_name:
        # 运行指定作业
        job_file = os.path.join(CONFIG_DIR, f"{job_name}.json")
        return run_datax_job(job_file, date)
    elif run_all:
        # 运行所有作业
        jobs = get_job_list()
        if not jobs:
            logger.warning("没有找到任何作业配置文件")
            return False
        
        success = True
        for job in jobs:
            job_file = os.path.join(CONFIG_DIR, job)
            if not run_datax_job(job_file, date):
                success = False
        
        return success
    else:
        logger.error("请指定要运行的作业或使用 -a 参数运行所有作业")
        return False

def create_hive_add_partition_sql(date):
    """创建Hive添加分区的SQL语句"""
    dt = datetime.strptime(date, '%Y-%m-%d').strftime('%Y%m%d')
    
    tables = [
        'ods_users',
        'ods_employees',
        'ods_products',
        'ods_purchase_orders',
        'ods_purchase_order_items',
        'ods_sales_orders',
        'ods_sales_order_items'
    ]
    
    sql_file = 'add_partitions.sql'
    
    with open(sql_file, 'w', encoding='utf-8') as f:
        f.write("-- 自动生成的Hive添加分区脚本\n")
        f.write(f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("USE purchase_sales_ods;\n\n")
        
        for table in tables:
            f.write(f"-- 为{table}添加分区\n")
            f.write(f"ALTER TABLE {table} ADD IF NOT EXISTS PARTITION (dt='{dt}');\n\n")
    
    logger.info(f"已生成Hive添加分区脚本: {sql_file}")
    return sql_file

def main():
    """主函数"""
    args = parse_args()
    
    logger.info("=" * 50)
    logger.info("开始执行DataX数据同步作业")
    logger.info(f"日期: {args.date}")
    
    if args.job:
        logger.info(f"指定作业: {args.job}")
    elif args.all:
        logger.info("运行所有作业")
    
    # 运行作业
    success = run_jobs(args.job, args.date, args.all)
    
    if success:
        # 创建Hive添加分区SQL
        sql_file = create_hive_add_partition_sql(args.date)
        logger.info(f"请使用以下命令添加Hive分区:\n  hive -f {sql_file}")
    
    logger.info("DataX数据同步作业执行完成")
    logger.info("=" * 50)
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main()) 