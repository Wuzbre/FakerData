from datetime import datetime
from fakeData import DataSimulator

def main():
    # 数据库配置
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': '123456',
        'database': 'yewu_erp',
        'charset': 'utf8mb4'
    }

    # 创建数据模拟器实例
    simulator = DataSimulator(
        db_config,
        start_time=datetime(2024, 11, 1),
        end_time=datetime(2024, 11, 30),
        frequency=5
    )

    try:
        # 开始生成数据
        simulator.simulate_data()
    except KeyboardInterrupt:
        print("\nData generation interrupted by user")
    except Exception as e:
        print(f"Error during data generation: {e}")

if __name__ == '__main__':
    main() 