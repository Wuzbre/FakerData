# Retail Data ETL Project

## 1. 项目概述
这是一个基于Spark的零售数据仓库ETL项目，采用经典的数据仓库分层架构（ODS、DWD、DWS、ADS），实现从MySQL数据源抽取数据，经过加工后加载到Apache Doris数据仓库。

### 1.1 技术栈
- 开发语言：Scala 2.12.15
- 计算引擎：Apache Spark 3.2.0
- 元数据存储：Apache Hive
- 目标数据库：Apache Doris
- 构建工具：Maven
- 调度工具：Shell Scripts

### 1.2 项目特点
- 完整的数据仓库分层架构
- 支持实时和离线数据处理
- 灵活的配置管理
- 完善的监控指标
- 高度模块化的代码结构

## 2. 项目结构

```
etl/
├── src/
│   └── main/
│       ├── scala/                    # Scala源代码
│       │   └── com/bigdata/etl/
│       │       ├── ads/             # ADS层：数据应用层
│       │       │   ├── AdsProcessor.scala              # ADS处理基类
│       │       │   ├── CategorySalesProcessor.scala    # 品类销售分析
│       │       │   ├── HotProductRankProcessor.scala   # 热销商品排行
│       │       │   ├── InventoryHealthProcessor.scala  # 库存健康分析
│       │       │   └── ...                            # 其他ADS处理器
│       │       ├── dimension/        # 维度处理
│       │       │   └── DateDimension.scala            # 日期维度处理
│       │       ├── doris/           # Doris相关
│       │       │   ├── DorisDataLoader.scala          # 数据加载器
│       │       │   └── DorisDataLoaderApp.scala       # 加载器应用程序
│       │       ├── fact/            # DWD层：事实表处理
│       │       │   ├── FactProcessor.scala            # 事实表处理基类
│       │       │   ├── OrderFactProcessor.scala       # 订单事实表
│       │       │   └── ...                           # 其他事实表处理器
│       │       └── summary/         # DWS层：汇总层
│       │           ├── SummaryProcessor.scala         # 汇总处理基类
│       │           ├── UserBehaviorSummary.scala      # 用户行为汇总
│       │           └── ...                           # 其他汇总处理器
│       └── resources/               # 配置文件
│           ├── application.conf      # 应用配置
│           ├── hive/                # Hive表定义
│           │   ├── create_tables.sql     # 基础表创建
│           │   ├── create_ads_tables.sql # ADS层表创建
│           │   └── dim_date.sql         # 日期维度表
│           └── doris/               # Doris表定义
│               └── create_tables.sql     # Doris表创建
├── bin/                            # 脚本文件
│   └── load_to_doris.sh            # Doris数据加载脚本
├── pom.xml                         # Maven配置
├── run_etl.sh                      # ETL执行脚本
└── README.md                       # 项目文档

## 3. 数据架构

### 3.1 分层设计
- ODS层：原始数据层，存储源系统数据
- DWD层：明细数据层，存储业务过程的事实表
- DWS层：汇总数据层，存储派生指标
- ADS层：应用数据层，存储业务指标

### 3.2 核心数据模型
1. 事实表
   - 订单事实表(dwd_fact_order)
   - 库存事实表(dwd_fact_inventory)
   - 价格事实表(dwd_fact_price)
   - 促销事实表(dwd_fact_promotion)

2. 维度表
   - 用户维度(dim_user)
   - 商品维度(dim_product)
   - 供应商维度(dim_supplier)
   - 日期维度(dim_date)

3. 汇总表
   - 用户行为汇总(dws_user_behavior)
   - 商品销售汇总(dws_product_sales)
   - 供应商表现汇总(dws_supplier_performance)
   - 促销效果汇总(dws_promotion_effect)

4. 应用指标
   - 用户价值分析(ads_user_value)
   - 热销商品排行(ads_hot_product_rank)
   - 库存健康分析(ads_inventory_health)
   - 运营整体统计(ads_operation_overall)

## 4. 模块说明

### 4.1 数据处理模块
1. **事实表处理(fact)**
   - `FactProcessor.scala`: 事实表处理基类
   - `OrderFactProcessor.scala`: 订单事实表处理
   - `InventoryFactProcessor.scala`: 库存事实表处理
   - `PriceFactProcessor.scala`: 价格事实表处理
   - `PromotionFactProcessor.scala`: 促销事实表处理

2. **汇总处理(summary)**
   - `SummaryProcessor.scala`: 汇总处理基类
   - `UserBehaviorSummary.scala`: 用户行为汇总
   - `ProductSalesSummary.scala`: 商品销售汇总
   - `SupplierPerformanceSummary.scala`: 供应商表现汇总

3. **应用指标处理(ads)**
   - `AdsProcessor.scala`: 应用指标处理基类
   - `UserValueProcessor.scala`: 用户价值分析
   - `CategorySalesProcessor.scala`: 品类销售分析
   - `InventoryHealthProcessor.scala`: 库存健康分析

### 4.2 数据加载模块
- `DorisDataLoader.scala`: Doris数据加载器
- `DorisDataLoaderApp.scala`: 加载应用程序
- `load_to_doris.sh`: 加载脚本

## 5. 运行说明

### 5.1 环境要求
- JDK 1.8+
- Scala 2.12.15
- Apache Spark 3.2.0
- Apache Hive
- Apache Doris
- Maven 3.x

### 5.2 配置说明
1. **application.conf**
```hocon
mysql {
  url = "jdbc:mysql://localhost:3306/retail_db"
  driver = "com.mysql.cj.jdbc.Driver"
  user = "root"
  password = "password"
}

hive {
  warehouse = "ods"
}

etl {
  batch_size = 10000
}
```

2. **Spark配置(run_etl.sh)**
```bash
SPARK_CONF="--master yarn \
  --deploy-mode cluster \
  --driver-memory 2g \
  --executor-memory 2g \
  --executor-cores 2 \
  --num-executors 2"
```

### 5.3 运行步骤
1. **编译打包**
```bash
mvn clean package
```

2. **执行ETL**
```bash
./run_etl.sh
```

3. **加载到Doris**
```bash
./bin/load_to_doris.sh 2024-01-01
```

## 6. 监控和维护

### 6.1 关键监控指标
- ETL作业执行状态
- 数据质量指标
- 系统资源使用
- 数据延迟情况

### 6.2 常见问题处理
1. **数据质量问题**
   - 检查源数据完整性
   - 验证转换逻辑
   - 核对数据一致性

2. **性能优化**
   - 调整Spark参数
   - 优化SQL语句
   - 合理设置并行度

3. **资源问题**
   - 监控内存使用
   - 检查磁盘空间
   - 优化资源配置

## 7. 开发指南

### 7.1 代码规范
- 遵循Scala编码规范
- 使用统一的命名约定
- 添加必要的注释说明

### 7.2 开发流程
1. 创建新的处理器类
2. 实现数据处理逻辑
3. 添加单元测试
4. 进行代码审查
5. 部署和验证

## 8. 联系方式
- 技术支持：[技术支持邮箱]
- 问题反馈：[问题反馈渠道]
- 项目负责人：[负责人联系方式]

## 9. 版本历史
- v1.0.0 (2024-01-01)
  - 初始版本发布
  - 实现基础ETL功能
  - 支持Doris数据加载 