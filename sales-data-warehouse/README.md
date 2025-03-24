# 销售数据仓库ETL系统

本项目是一个基于Spark SQL的销售数据仓库ETL系统，使用Scala语言开发，用于处理销售订单数据、采购订单数据及相关维度数据，构建ODS、DWD、DWS三层数据仓库架构。

## 系统架构

系统采用经典的三层数据仓库架构：

1. **ODS层(Operational Data Store)**：原始数据层，存储从MySQL等源系统抽取的原始数据
2. **DWD层(Data Warehouse Detail)**：明细数据层，存储经过清洗和转换的明细数据
3. **DWS层(Data Warehouse Summary)**：汇总数据层，存储经过聚合计算的统计指标数据

同时，系统实现了从Hive到Doris的数据同步，用于支持实时分析查询。

## 主要功能

1. **MySQL到ODS的数据抽取**：使用Spark JDBC抽取MySQL数据到Hive ODS层
2. **ODS到DWD的数据转换**：对ODS层数据进行清洗、转换，构建维度表和事实表
3. **DWD到DWS的数据聚合**：根据业务需求进行数据汇总，生成统计指标
4. **数据质量检查**：检查各层数据的完整性、一致性、准确性等
5. **Hive到Doris的数据同步**：将DWD/DWS层数据同步到Doris，用于快速分析查询

## 技术栈

- **编程语言**：Scala 2.12
- **数据处理**：Spark 3.3.1 (Spark SQL, Spark Core)
- **数据存储**：Hive (ODS/DWD/DWS), Doris (实时分析)
- **源数据库**：MySQL
- **构建工具**：Maven
- **配置管理**：Typesafe Config
- **日志管理**：Log4j 2
- **命令行参数解析**：scopt

## 项目结构

```
sales-data-warehouse/
├── src/main/scala/com/sales/
│   ├── SalesDataETL.scala                # 主程序入口
│   ├── etl/                              # ETL组件
│   │   ├── MysqlToOds.scala              # MySQL到ODS的抽取
│   │   ├── OdsToDwd.scala                # ODS到DWD的转换
│   │   └── DwdToDws.scala                # DWD到DWS的聚合
│   ├── quality/                          # 数据质量
│   │   └── DataQualityCheck.scala        # 数据质量检查组件
│   ├── sync/                             # 数据同步
│   │   └── HiveToDoris.scala             # Hive到Doris的同步组件
│   └── utils/                            # 工具类
│       ├── ConfigUtils.scala             # 配置工具
│       ├── DateUtils.scala               # 日期工具
│       └── SparkSessionUtils.scala       # Spark会话工具
├── src/main/resources/
│   ├── application.conf                  # 配置文件
│   └── log4j2.xml                        # 日志配置
├── pom.xml                               # Maven配置
├── run-etl.sh                            # ETL执行脚本
└── README.md                             # 项目说明
```

## 快速开始

### 1. 环境准备

- JDK 1.8+
- Scala 2.12
- Spark 3.3.1
- Hive
- Doris（可选，用于数据分析查询）
- MySQL（源数据库）

### 2. 配置修改

修改`src/main/resources/application.conf`文件，配置数据库连接信息和其他参数：

```hocon
mysql {
  url = "jdbc:mysql://localhost:3306/sales_db"
  user = "your_user"
  password = "your_password"
  driver = "com.mysql.cj.jdbc.Driver"
}

hive {
  warehouse.dir = "/user/hive/warehouse"
  metastore.uris = "thrift://localhost:9083"
  ods.database = "purchase_sales_ods"
  dwd.database = "purchase_sales_dwd"
  dws.database = "purchase_sales_dws"
  dq.database = "purchase_sales_dq"
}

doris {
  url = "jdbc:mysql://localhost:9030/sales_analysis"
  user = "root"
  password = "your_password"
  database = "sales_analysis"
}

# 其他配置...
```

### 3. 编译打包

```bash
mvn clean package
```

### 4. 执行ETL流程

使用提供的脚本执行ETL流程，默认处理昨天的数据：

```bash
./run-etl.sh
```

指定处理日期：

```bash
./run-etl.sh 20230101
```

### 5. 单独执行ETL阶段

执行MySQL到ODS的抽取：

```bash
./run-etl.sh 20230101 --mysql-to-ods
```

执行ODS到DWD的转换：

```bash
./run-etl.sh 20230101 --ods-to-dwd
```

执行DWD到DWS的聚合：

```bash
./run-etl.sh 20230101 --dwd-to-dws
```

执行数据质量检查：

```bash
./run-etl.sh 20230101 --dq-check
```

执行Hive到Doris的同步：

```bash
./run-etl.sh 20230101 --sync-to-doris
```

## 数据模型

### ODS层表结构

- `ods_users`：用户表
- `ods_employees`：员工表
- `ods_products`：产品表
- `ods_sales_orders`：销售订单表
- `ods_sales_order_items`：销售订单明细表
- `ods_purchase_orders`：采购订单表
- `ods_purchase_order_items`：采购订单明细表
- `ods_suppliers`：供应商表

### DWD层表结构

- `dwd_dim_user`：用户维度表
- `dwd_dim_employee`：员工维度表
- `dwd_dim_product`：产品维度表
- `dwd_dim_supplier`：供应商维度表
- `dwd_fact_sales_order`：销售订单事实表
- `dwd_fact_sales_order_item`：销售订单明细事实表
- `dwd_fact_purchase_order`：采购订单事实表
- `dwd_fact_purchase_order_item`：采购订单明细事实表

### DWS层表结构

- `dws_sales_day_agg`：销售日汇总表
- `dws_sales_month_agg`：销售月汇总表
- `dws_product_sales`：产品销售汇总表
- `dws_customer_behavior`：客户行为分析表
- `dws_supplier_purchase`：供应商采购分析表

## 数据质量管理

系统实现了全面的数据质量检查机制，包括：

1. **ODS层检查**：
   - 空值检查：检查关键字段的空值率
   - 数据量检查：检查各表的记录数是否正常
   - 主键重复检查：检查主键是否存在重复

2. **DWD层检查**：
   - 完整性检查：检查ODS到DWD的数据转换率
   - 一致性检查：检查关联表之间的数据一致性

3. **DWS层检查**：
   - 准确性检查：检查汇总结果的准确性

## 维护与支持

- 日志文件位于`log/`目录，包含详细的ETL执行记录和错误信息
- ETL报告保存在`log/etl_report_yyyyMMdd.log`文件中，包含数据记录数和质量检查结果
 