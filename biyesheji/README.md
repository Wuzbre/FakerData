# 购销数据仓库系统 - 数据生成模块

## 项目简介

本项目是购销数据仓库系统的数据生成模块，用于生成模拟的业务数据，包括用户、商品、供应商、订单等信息。这些数据可用于系统测试、性能评估和数据分析等场景。

## 功能特点

- 自动生成符合业务逻辑的模拟数据
- 支持可配置的数据量和参数
- 生成数据符合实际业务场景
- 支持批量数据生成
- 提供完整的数据一致性保证

## 项目结构

```
biyesheji/
├── README.md           # 项目说明文档
├── requirements.txt    # Python依赖包列表
├── .env.example       # 环境变量配置模板
├── .env               # 实际环境变量配置（需自行创建）
├── __init__.py        # Python包初始化文件
├── sql/
│   └── create_tables.sql  # 数据库表创建脚本
└── scripts/
    ├── config.py          # 配置文件
    └── data_generator.py  # 数据生成脚本
```

## 技术栈

- Python 3.8+
- MySQL 5.7+
- Faker 19.13.0 (数据模拟生成)
- Pandas 2.1.4 (数据处理)
- mysql-connector-python 8.2.0 (数据库连接)
- python-dotenv 1.0.0 (环境变量管理)
- tqdm 4.66.1 (进度显示)

## 环境要求

- 操作系统：Windows/Linux/MacOS
- Python 3.8 或更高版本
- MySQL 5.7 或更高版本
- 至少 4GB 可用内存
- 建议至少 10GB 可用磁盘空间（取决于生成数据量）

## 安装步骤

1. 克隆项目到本地：
   ```bash
   git clone <repository_url>
   cd biyesheji
   ```

2. 创建并激活虚拟环境：
   ```bash
   python -m venv venv
   
   # Windows
   .\venv\Scripts\activate
   
   # Linux/MacOS
   source venv/bin/activate
   ```

3. 安装依赖包：
   ```bash
   pip install -r requirements.txt
   ```

4. 配置环境变量：
   ```bash
   cp .env.example .env
   ```
   编辑 .env 文件，配置以下参数：
   - DB_HOST：数据库主机地址
   - DB_USER：数据库用户名
   - DB_PASSWORD：数据库密码
   - DB_PORT：数据库端口

5. 创建数据库和表：
   ```bash
   # 登录 MySQL
   mysql -u your_username -p
   
   # 创建数据库
   CREATE DATABASE IF NOT EXISTS your_database_name;
   
   # 执行建表脚本
   mysql -u your_username -p your_database_name < sql/create_tables.sql
   ```

## 配置说明

在 `scripts/config.py` 中可以配置以下参数：

1. 数据量配置：
   - USER_COUNT：用户数量
   - CATEGORY_COUNT：商品类别数量
   - SUPPLIER_COUNT：供应商数量
   - PRODUCT_COUNT：商品数量
   - ORDER_COUNT：订单数量
   - MAX_ORDER_DETAILS：单个订单最大商品数量

2. 日期范围配置：
   - START_DATE：数据开始日期
   - END_DATE：数据结束日期

3. 数据特征配置：
   - 价格范围
   - 库存范围
   - 折扣范围
   - 等级分布

## 使用说明

1. 生成全部数据：
   ```bash
   python scripts/data_generator.py
   ```

2. 生成指定类型数据：
   ```bash
   python scripts/data_generator.py --type users  # 仅生成用户数据
   python scripts/data_generator.py --type products  # 仅生成商品数据
   python scripts/data_generator.py --type orders  # 仅生成订单数据
   ```

3. 指定数据量：
   ```bash
   python scripts/data_generator.py --scale small  # 生成小规模数据
   python scripts/data_generator.py --scale medium  # 生成中等规模数据
   python scripts/data_generator.py --scale large  # 生成大规模数据
   ```

## 生成数据说明

本模块生成的数据包括：

1. 用户数据：
   - 基本信息：用户ID、姓名、性别、年龄等
   - 联系信息：电话、邮箱、地址
   - 账户信息：注册时间、账户状态等

2. 商品类别：
   - 三级类别结构
   - 类别编码和名称
   - 类别描述和状态

3. 供应商信息：
   - 基本信息：供应商ID、名称、联系人
   - 经营信息：经营范围、信用等级
   - 联系方式：地址、电话、邮箱

4. 商品信息：
   - 基本信息：商品ID、名称、描述
   - 价格信息：成本价、售价、折扣
   - 库存信息：库存量、警戒值
   - 其他属性：上架时间、状态等

5. 订单数据：
   - 订单主表：订单ID、用户ID、订单时间、总金额等
   - 订单明细：商品ID、数量、单价、折扣等

## 数据质量保证

- 所有生成的数据都符合业务规则和约束
- 确保数据之间的关联关系正确
- 生成的数据符合实际业务场景的分布特征
- 支持数据一致性检查

## 常见问题解决

1. 数据库连接失败：
   - 检查 .env 文件配置是否正确
   - 确认 MySQL 服务是否运行
   - 验证数据库用户权限

2. 内存不足：
   - 减小配置文件中的数据生成量
   - 使用分批次生成数据
   - 增加系统虚拟内存

3. 生成速度慢：
   - 优化数据库配置
   - 使用批量插入
   - 调整生成数据的批次大小

## 注意事项

1. 生成数据前请确保：
   - 数据库连接配置正确
   - 有足够的磁盘空间
   - 数据库用户具有足够权限

2. 数据生成过程：
   - 避免中断数据生成过程
   - 建议在非生产环境中使用
   - 大规模数据生成可能需要较长时间

3. 数据安全：
   - 生成的测试数据不要用于生产环境
   - 定期备份重要数据
   - 注意保护敏感信息

## 维护和支持

- 定期更新依赖包版本
- 根据业务需求调整数据生成规则
- 欢迎提交问题反馈和改进建议

## 版本历史

- v1.0.0 (2024-03-25)
  - 初始版本发布
  - 支持基础数据生成
  - 完整的文档支持 