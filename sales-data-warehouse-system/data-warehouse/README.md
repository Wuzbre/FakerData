# 数据仓库模块

## 概述

数据仓库模块定义了整个购销数据仓库的结构和处理流程，包括Hive数据仓库的表结构定义、数据转换逻辑，以及与Doris查询引擎的集成。该模块是系统中数据存储和处理的核心，为业务分析提供基础数据支持。

## 功能特点

- 完整的数据仓库分层设计（ODS、DWD、DWS）
- 标准化的表结构和字段定义
- 基于HQL的数据转换和聚合逻辑
- 完善的分区策略和生命周期管理
- 集成Doris实现毫秒级查询响应
- 内置数据质量检查机制

## 目录结构

```
data-warehouse/
├── hive/                        # Hive相关脚本
│   ├── create_ods_tables.hql    # ODS层表创建
│   ├── add_ods_partitions.hql   # 分区管理
│   ├── ods_to_dwd.hql           # ODS到DWD转换
│   ├── dwd_to_dws.hql           # DWD到DWS聚合
│   └── data_quality_check.hql   # 数据质量检查
├── doris/                       # Doris相关脚本
│   └── hive_to_doris_sync.py    # 数据同步脚本
└── datax/                       # DataX数据集成
    ├── mysql_to_hive_users.json # DataX配置
    └── run_datax_jobs.py        # 执行脚本
```

## 数据仓库架构

### 数据分层

1. **ODS层 (Operational Data Store)**
   - 原始数据层，保持与源系统数据结构一致
   - 按天分区存储历史数据
   - 不做数据聚合和转换，保留原始数据细节

2. **DWD层 (Data Warehouse Detail)**
   - 明细数据层，对ODS数据进行清洗和转换
   - 统一字段命名和数据类型，处理空值和异常数据
   - 建立维度关系，提高数据质量

3. **DWS层 (Data Warehouse Service)**
   - 汇总数据层，按主题域进行轻度汇总
   - 面向业务分析场景，预计算常用指标
   - 提高查询效率，减少重复计算

4. **ADS层 (Application Data Service)**
   - 应用数据层，存储在Doris中
   - 面向具体应用的高度汇总数据
   - 提供毫秒级查询响应

### 主题域划分

数据仓库按以下主题域进行组织：

- 用户域（用户基本信息、用户行为特征）
- 产品域（产品信息、产品层级、价格历史）
- 销售域（订单、交易、退货）
- 库存域（库存水平、库存变动、补货）
- 渠道域（销售渠道、促销活动）

## 使用指南

### Hive脚本

#### 创建ODS表

```bash
hive -f hive/create_ods_tables.hql
```

该脚本创建ODS层的基础表结构，包括用户表、产品表、订单表等。

#### 添加分区

```bash
hive -f hive/add_ods_partitions.hql -hiveconf dt=20231231
```

为ODS表添加指定日期的分区。

#### ODS到DWD转换

```bash
hive -f hive/ods_to_dwd.hql -hiveconf dt=20231231
```

将ODS层数据转换到DWD层，进行数据清洗和规范化处理。

#### DWD到DWS聚合

```bash
hive -f hive/dwd_to_dws.hql -hiveconf dt=20231231
```

将DWD层数据聚合到DWS层，生成面向业务的汇总数据。

#### 数据质量检查

```bash
hive -f hive/data_quality_check.hql -hiveconf dt=20231231
```

执行数据质量检查，验证数据完整性和一致性。

### DataX数据抽取

#### 配置说明

DataX配置文件定义了从MySQL到Hive的数据抽取任务：

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "connection": [
              {
                "jdbcUrl": "jdbc:mysql://localhost:3306/sales_db",
                "querySql": ["SELECT * FROM users WHERE update_time >= ${start_time} AND update_time < ${end_time}"]
              }
            ],
            "username": "root",
            "password": "password"
          }
        },
        "writer": {
          "name": "hdfswriter",
          "parameter": {
            "defaultFS": "hdfs://localhost:9000",
            "fileType": "orc",
            "path": "/user/hive/warehouse/sales_ods.db/ods_users/dt=${dt}",
            "fileName": "users",
            "column": [
              {"name": "user_id", "type": "string"},
              {"name": "user_name", "type": "string"},
              // 其他字段...
            ],
            "writeMode": "append"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 3
      }
    }
  }
}
```

#### 执行DataX作业

```bash
python datax/run_datax_jobs.py --date 20231231
```

该脚本自动执行多个DataX作业，将MySQL数据抽取到Hive的ODS层。

### Doris数据同步

Doris集成脚本实现了Hive到Doris的数据同步：

```bash
python doris/hive_to_doris_sync.py --date 20231231 --table sales_summary
```

该脚本将Hive中的DWS层数据同步到Doris，支持增量或全量同步模式。

## 表结构说明

### ODS层表

ODS层表保持与源系统结构一致，主要包括：

- `ods_users` - 用户信息表
- `ods_products` - 产品信息表
- `ods_orders` - 订单主表
- `ods_order_items` - 订单明细表
- `ods_inventory` - 库存信息表

### DWD层表

DWD层表进行了字段规范化和数据清洗，主要包括：

- `dwd_dim_users` - 用户维度表
- `dwd_dim_products` - 产品维度表
- `dwd_fact_orders` - 订单事实表
- `dwd_fact_order_items` - 订单明细事实表
- `dwd_fact_inventory` - 库存事实表

### DWS层表

DWS层表按业务主题进行了聚合，主要包括：

- `dws_user_stats` - 用户统计汇总表
- `dws_product_sales` - 产品销售汇总表
- `dws_region_sales` - 区域销售汇总表
- `dws_category_sales` - 品类销售汇总表
- `dws_inventory_turnover` - 库存周转汇总表

## 数据生命周期管理

不同层级的数据有不同的保留策略：

- ODS层数据保留30天
- DWD层数据保留90天
- DWS层数据保留180天

通过以下脚本自动清理过期数据：

```sql
-- 清理ODS层过期分区
ALTER TABLE ods_users DROP IF EXISTS PARTITION (dt < date_sub(current_date, 30));
```

## 数据质量管理

数据质量检查包括以下方面：

1. **完整性检查**：检查关键字段的非空率
2. **准确性检查**：检查数据是否在有效范围内
3. **一致性检查**：检查跨表数据的一致性
4. **唯一性检查**：检查主键的唯一性
5. **及时性检查**：检查数据的更新时间

质量检查结果存储在专门的监控表中，可通过查询获取质量报告。

## 注意事项

- 修改表结构时需同步更新相关ETL脚本
- 分区字段的修改需谨慎，可能影响历史数据
- 大表Join操作应注意性能优化
- Doris同步频率应根据业务实时性需求调整 