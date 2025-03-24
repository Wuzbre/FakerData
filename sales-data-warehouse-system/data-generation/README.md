# 数据生成模块

## 概述

数据生成模块用于创建模拟的购销数据，为整个数据仓库系统提供测试数据源。该模块使用Python和Faker库生成符合业务场景的模拟数据，并将其存储到MySQL数据库中，作为数据仓库ETL过程的数据源。

## 功能特点

- 生成模拟的用户、产品、订单、销售和库存数据
- 支持大规模数据生成，可配置生成数量和时间范围
- 保证数据的一致性和合理性，符合真实业务场景
- 支持自定义数据分布和业务规则

## 文件说明

- `fakeData.py` - 主要的数据生成脚本
- `requirements.txt` - Python依赖项列表
- `config/data_config.py` - 数据生成配置参数

## 环境要求

- Python 3.7+
- MySQL 5.7+
- 相关Python库（见requirements.txt）

## 安装步骤

1. 安装Python依赖：

```bash
pip install -r requirements.txt
```

2. 配置MySQL连接：

修改`config/data_config.py`文件中的数据库连接信息：

```python
DB_CONFIG = {
    'host': 'localhost',
    'user': 'your_username',
    'password': 'your_password',
    'database': 'sales_db',
    'port': 3306
}
```

## 使用方法

### 基本用法

生成默认数量的模拟数据：

```bash
python fakeData.py
```

### 自定义数据量

指定要生成的数据量：

```bash
python fakeData.py --users 1000 --products 500 --orders 5000
```

### 参数说明

- `--users` - 生成的用户数量（默认：1000）
- `--products` - 生成的产品数量（默认：200）
- `--orders` - 生成的订单数量（默认：10000）
- `--start_date` - 数据开始日期（默认：一年前）
- `--end_date` - 数据结束日期（默认：当前日期）
- `--clear` - 在生成新数据前清除现有数据（默认：False）

## 数据模型

生成的数据包含以下表：

1. **用户表(users)**
   - 用户ID、姓名、联系方式、地址等

2. **产品表(products)**
   - 产品ID、名称、类别、价格、成本等

3. **订单表(orders)**
   - 订单ID、用户ID、订单日期、状态等

4. **订单明细表(order_items)**
   - 订单ID、产品ID、数量、单价等

5. **库存表(inventory)**
   - 产品ID、库存数量、更新时间等

## 数据分布

数据生成过程模拟了真实的业务场景，包括：

- 产品销售的季节性波动
- 不同区域的销售差异
- 热门产品和冷门产品的销售分布
- 用户购买行为的多样性

## 扩展和自定义

可以通过修改`config/data_config.py`文件来自定义数据生成规则：

```python
# 产品类别与其对应的权重
PRODUCT_CATEGORIES = {
    '电子产品': 0.3,
    '服装': 0.25,
    '家居': 0.2,
    '食品': 0.15,
    '图书': 0.1
}

# 区域销售分布
REGION_DISTRIBUTION = {
    '华东': 0.35,
    '华北': 0.25,
    '华南': 0.2,
    '西部': 0.1,
    '东北': 0.1
}
```

## 注意事项

- 大规模数据生成可能需要较长时间，请耐心等待
- 确保MySQL数据库有足够的存储空间
- 对于大量数据，建议增加数据库的连接超时设置 