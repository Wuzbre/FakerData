# 购销数据仓库系统 - 数据生成模块

## 项目结构

```
biyesheji/
├── README.md           # 项目说明文档
├── requirements.txt    # Python依赖包列表
├── .env.example       # 环境变量配置模板
├── sql/
│   └── create_tables.sql  # 数据库表创建脚本
└── scripts/
    ├── config.py          # 配置文件
    └── data_generator.py  # 数据生成脚本
```

## 环境要求

- Python 3.8+
- MySQL 5.7+

## 安装步骤

1. 创建虚拟环境（推荐）：
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # 或
   .\venv\Scripts\activate  # Windows
   ```

2. 安装依赖包：
   ```bash
   pip install -r requirements.txt
   ```

3. 配置环境变量：
   ```bash
   cp .env.example .env
   # 编辑.env文件，填入实际的数据库连接信息
   ```

4. 创建数据库表：
   ```bash
   # 使用MySQL客户端执行sql/create_tables.sql脚本
   mysql -u your_username -p < sql/create_tables.sql
   ```

## 使用说明

1. 数据生成：
   ```bash
   python scripts/data_generator.py
   ```

2. 配置说明：
   - 在`scripts/config.py`中可以修改数据生成的配置参数
   - 主要配置项包括：
     - 用户数量
     - 商品类别数量
     - 供应商数量
     - 商品数量
     - 订单数量
     - 订单明细最大数量
     - 数据日期范围

## 生成数据说明

本模块会生成以下模拟数据：

1. 用户数据：包含用户基本信息
2. 商品类别：三级类别结构
3. 供应商信息：供应商基本信息
4. 商品信息：包含价格、库存等
5. 订单数据：包含订单主表和订单明细

## 注意事项

1. 生成数据前请确保数据库连接正确
2. 数据生成过程中请勿中断，以免数据不完整
3. 如需重新生成数据，请先清空相关表
4. 默认生成的用户密码均为"123456"（MD5加密） 