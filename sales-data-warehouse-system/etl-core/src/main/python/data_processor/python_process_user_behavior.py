#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
用户行为数据处理脚本
"""

import os
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class UserBehaviorProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("User Behavior Processor") \
            .enableHiveSupport() \
            .getOrCreate()
        
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    def process_user_behavior(self, dt):
        try:
            # 读取原始数据
            df = self.spark.sql(f"""
                SELECT *
                FROM ods.mysql2hive_ods_user_behavior
                WHERE dt = '{dt}'
            """)
            
            # 数据清洗和转换
            cleaned_df = self._clean_data(df)
            
            # 会话分析
            session_df = self._analyze_sessions(cleaned_df)
            
            # 用户行为路径分析
            path_df = self._analyze_behavior_path(cleaned_df)
            
            # 保存处理后的数据
            self._save_results(session_df, path_df, dt)
            
            logger.info(f"数据处理完成: {dt}")
            
        except Exception as e:
            logger.error(f"数据处理失败: {str(e)}")
            raise e
        
    def _clean_data(self, df):
        """数据清洗"""
        return df.dropDuplicates() \
            .filter(col("user_id").isNotNull()) \
            .filter(col("behavior_type").isin(["view", "click", "cart", "order"])) \
            .withColumn("timestamp", to_timestamp("behavior_time"))
    
    def _analyze_sessions(self, df):
        """会话分析"""
        return df.withWatermark("timestamp", "30 minutes") \
            .groupBy(
                "user_id",
                window("timestamp", "30 minutes"),
                "session_id"
            ) \
            .agg(
                count("*").alias("actions"),
                collect_list("behavior_type").alias("behaviors"),
                min("timestamp").alias("session_start"),
                max("timestamp").alias("session_end")
            )
    
    def _analyze_behavior_path(self, df):
        """行为路径分析"""
        return df.withColumn("next_behavior", 
                lead("behavior_type").over(
                    Window.partitionBy("user_id", "session_id")
                    .orderBy("timestamp")
                )
            ) \
            .groupBy("behavior_type", "next_behavior") \
            .count()
    
    def _save_results(self, session_df, path_df, dt):
        """保存处理结果"""
        # 保存会话分析结果
        session_df.write \
            .mode("overwrite") \
            .partitionBy("dt") \
            .saveAsTable("dwd.fact_user_sessions")
        
        # 保存行为路径分析结果
        path_df.write \
            .mode("overwrite") \
            .partitionBy("dt") \
            .saveAsTable("dwd.fact_user_behavior_path")

def main():
    if len(sys.argv) != 2:
        print("Usage: python process_user_behavior.py <date>")
        sys.exit(1)
    
    dt = sys.argv[1]
    processor = UserBehaviorProcessor()
    processor.process_user_behavior(dt)

if __name__ == "__main__":
    main() 