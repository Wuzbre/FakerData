import pymysql
from pymysql.cursors import DictCursor
import json
from datetime import datetime, date
import decimal

def json_serial(obj):
    """处理日期和时间的JSON序列化"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError (f"Type {type(obj)} not serializable")

# 连接数据库
conn = pymysql.connect(
    host='localhost',
    user='root',
    password='123456',
    database='sales_data_warehouse',
    charset='utf8mb4',
    cursorclass=DictCursor
)

try:
    with conn.cursor() as cursor:
        # 查询用户表
        cursor.execute("SELECT * FROM users LIMIT 3")
        print("用户样例:")
        for row in cursor.fetchall():
            print(json.dumps(row, default=json_serial, ensure_ascii=False, indent=2))
        
        # 查询产品表
        cursor.execute("SELECT * FROM products LIMIT 3")
        print("\n产品样例:")
        for row in cursor.fetchall():
            print(json.dumps(row, default=json_serial, ensure_ascii=False, indent=2))
        
        # 查询订单表
        cursor.execute("SELECT * FROM orders LIMIT 3")
        print("\n订单样例:")
        for row in cursor.fetchall():
            print(json.dumps(row, default=json_serial, ensure_ascii=False, indent=2))
        
        # 查询订单项表
        cursor.execute("SELECT * FROM order_items LIMIT 3")
        print("\n订单项样例:")
        for row in cursor.fetchall():
            print(json.dumps(row, default=json_serial, ensure_ascii=False, indent=2))
        
        # 查询库存事务表
        cursor.execute("SELECT * FROM inventory_transactions LIMIT 3")
        print("\n库存事务样例:")
        for row in cursor.fetchall():
            print(json.dumps(row, default=json_serial, ensure_ascii=False, indent=2))
        
        # 查询表的计数
        print("\n数据统计:")
        tables = ["users", "products", "suppliers", "orders", "order_items", "inventory_transactions", "inventory"]
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            count = cursor.fetchone()["count"]
            print(f"{table}: {count}条记录")
        
finally:
    conn.close() 