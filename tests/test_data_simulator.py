import unittest
from datetime import datetime, timedelta
from fakeData import DataSimulator, DatabaseError, DataValidationError
import pymysql
import logging

class TestDataSimulator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        cls.db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': '123456',
            'database': 'yewu_erp_test',  # 使用测试数据库
            'charset': 'utf8mb4'
        }
        
        # 创建测试数据库和表
        cls._setup_test_database()
        
        # 初始化模拟器
        cls.simulator = DataSimulator(
            cls.db_config,
            start_time=datetime.now(),
            end_time=datetime.now() + timedelta(days=1),
            frequency=0  # 测试时不需要等待
        )

    @classmethod
    def _setup_test_database(cls):
        """设置测试数据库"""
        conn = pymysql.connect(
            host=cls.db_config['host'],
            user=cls.db_config['user'],
            password=cls.db_config['password']
        )
        try:
            with conn.cursor() as cursor:
                # 创建测试数据库
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {cls.db_config['database']}")
                cursor.execute(f"USE {cls.db_config['database']}")
                
                # 创建测试表（这里只创建测试需要的最小表结构）
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        user_id INT AUTO_INCREMENT PRIMARY KEY,
                        username VARCHAR(50),
                        email VARCHAR(100),
                        phone VARCHAR(20),
                        password VARCHAR(100),
                        status VARCHAR(20) DEFAULT 'active',
                        created_at DATETIME,
                        updated_at DATETIME
                    )
                """)
                conn.commit()
        finally:
            conn.close()

    def setUp(self):
        """每个测试方法前执行"""
        # 清空测试表
        conn = pymysql.connect(**self.db_config)
        try:
            with conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE users")
            conn.commit()
        finally:
            conn.close()

    def test_generate_user_data(self):
        """测试用户数据生成"""
        user_data = self.simulator.generate_user_data(datetime.now())
        self.assertIsInstance(user_data, dict)
        self.assertIn('email', user_data)
        self.assertIn('phone', user_data)
        
        # 验证数据格式
        self.assertTrue(self.simulator._validate_data(user_data, 'user'))

    def test_save_to_mysql(self):
        """测试数据保存到MySQL"""
        test_data = [self.simulator.generate_user_data(datetime.now())]
        self.simulator.save_to_mysql(test_data, 'users')
        
        # 验证数据已保存
        result = self.simulator.query_from_db("SELECT COUNT(*) FROM users")
        self.assertEqual(result[0], 1)

    def test_data_validation(self):
        """测试数据验证"""
        # 测试无效的邮箱
        invalid_user = {
            'email': 'invalid_email',
            'phone': '13800138000',
            'password': '12345678'
        }
        self.assertFalse(self.simulator._validate_data(invalid_user, 'user'))
        
        # 测试有效数据
        valid_user = {
            'email': 'test@example.com',
            'phone': '13800138000',
            'password': '12345678'
        }
        self.assertTrue(self.simulator._validate_data(valid_user, 'user'))

    def test_checkpoint(self):
        """测试断点续传"""
        test_date = datetime.now()
        self.simulator._save_checkpoint(test_date)
        
        # 创建新的模拟器实例，应该从断点处继续
        new_simulator = DataSimulator(self.db_config)
        self.assertEqual(new_simulator.start_time.date(), test_date.date())

    def test_error_handling(self):
        """测试错误处理"""
        # 测试数据库连接错误
        invalid_config = self.db_config.copy()
        invalid_config['host'] = 'invalid_host'
        
        with self.assertRaises(RuntimeError):
            DataSimulator(invalid_config)
        
        # 测试无效的SQL查询
        with self.assertRaises(DatabaseError):
            self.simulator.query_from_db("SELECT * FROM non_existent_table")

    @classmethod
    def tearDownClass(cls):
        """测试类清理"""
        # 删除测试数据库
        conn = pymysql.connect(
            host=cls.db_config['host'],
            user=cls.db_config['user'],
            password=cls.db_config['password']
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"DROP DATABASE IF EXISTS {cls.db_config['database']}")
        finally:
            conn.close()

if __name__ == '__main__':
    unittest.main() 