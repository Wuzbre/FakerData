import uuid
import random
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import Error
from .config import DB_CONFIG, DATA_CONFIG, BATCH_SIZE

class BaseGenerator:
    """生成器基类"""
    
    def __init__(self):
        """初始化数据库连接"""
        try:
            self.conn = mysql.connector.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor(dictionary=True)
        except Error as e:
            print(f"数据库连接失败: {e}")
            raise
    
    def close_connection(self):
        """关闭数据库连接"""
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'conn') and self.conn.is_connected():
            self.conn.close()
    
    def generate_uuid(self):
        """生成UUID"""
        return str(uuid.uuid4()).replace('-', '')
    
    def execute_query(self, query, params=None):
        """执行查询"""
        try:
            self.cursor.execute(query, params)
            return self.cursor.fetchall()
        except Error as e:
            print(f"查询执行失败: {e}")
            print(f"查询语句: {query}")
            print(f"参数: {params}")
            raise
    
    def batch_insert(self, table_name, columns, values):
        """批量插入数据"""
        if not values:
            return
            
        try:
            # 构建插入语句
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join(columns)
            query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            
            # 分批插入
            for i in range(0, len(values), BATCH_SIZE):
                batch = values[i:i + BATCH_SIZE]
                self.cursor.executemany(query, batch)
                self.conn.commit()
                
        except Error as e:
            print(f"批量插入失败: {e}")
            print(f"表名: {table_name}")
            print(f"列名: {columns}")
            self.conn.rollback()
            raise
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close_connection()
        if exc_type:
            return False  # 重新抛出异常

    def get_random_date(self, start_date, end_date):
        """生成随机日期"""
        time_between_dates = end_date - start_date
        days_between_dates = time_between_dates.days
        random_number_of_days = random.randrange(days_between_dates)
        return start_date + timedelta(days=random_number_of_days)

    def get_weighted_random(self, options, weights=None):
        """获取加权随机选择"""
        if weights is None:
            return random.choice(options)
        return random.choices(options, weights=weights, k=1)[0]

    def update_record(self, table, set_values, where_clause, where_values):
        """更新记录"""
        try:
            set_clause = ', '.join([f"{k} = %s" for k in set_values.keys()])
            sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
            values = list(set_values.values()) + where_values
            self.cursor.execute(sql, values)
            self.conn.commit()
        except Error as e:
            print(f"更新记录时发生错误: {e}")
            self.conn.rollback()

    def get_progress_bar(self, total, desc="Processing"):
        """获取进度条"""
        try:
            from tqdm import tqdm
            return tqdm(total=total, desc=desc)
        except ImportError:
            class SimpleProg:
                def __init__(self, total):
                    self.total = total
                    self.n = 0
                def update(self, n):
                    self.n += n
                    print(f"\rProgress: {self.n}/{self.total}", end="")
                def close(self):
                    print()
            return SimpleProg(total) 