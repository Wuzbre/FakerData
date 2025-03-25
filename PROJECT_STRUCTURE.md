# 基于Hive的购销数据仓库系统 - 项目结构说明

## 项目概述

本项目是一个完整的购销数据仓库解决方案，实现了从数据生成、抽取、转换、加载到数据可视化的全流程。系统采用了Hadoop生态的Hive作为核心数据仓库，Spark进行数据处理，Doris实现实时查询，结合Spring Boot后端和Vue.js前端提供完整的数据分析体验。

## 系统架构

```
+--------------------+    +--------------------+    +--------------------+
|                    |    |                    |    |                    |
|  数据源 (MySQL)     |--->|  数据仓库 (Hive)    |--->|  查询引擎 (Doris)   |
|                    |    |                    |    |                    |
+--------------------+    +--------------------+    +--------------------+
         ^                        ^                          |
         |                        |                          |
         |                        |                          v
+--------------------+    +--------------------+    +--------------------+
|                    |    |                    |    |                    |
|  数据生成 (Python)   |    |  数据处理 (Spark)   |    |  API服务 (SpringBoot)|
|                    |    |                    |    |                    |
+--------------------+    +--------------------+    +--------------------+
                                                             |
                                                             |
                                                             v
                                                   +--------------------+
                                                   |                    |
                                                   |  前端UI (Vue.js)    |
                                                   |                    |
                                                   +--------------------+
```

## 数据流程

1. **数据生成**: 使用Python和Faker库生成模拟的购销数据
2. **数据抽取**: 通过Spark SQL将MySQL中的数据抽取到Hive的ODS层
3. **数据转换**: 通过Spark SQL将ODS数据转换为DWD和DWS层数据
4. **数据同步**: 将Hive中的汇总数据同步到Doris，用于快速查询
5. **数据查询**: 通过Spring Boot开发的API服务查询Doris中的数据
6. **数据可视化**: 通过Vue.js开发的前端UI展示数据分析结果

## 项目结构

新的目录结构组织如下：

```
sales-data-warehouse-system/
├── README.md                          # 主README文件
├── data-generation/                   # 数据生成模块
│   ├── fakeData.py                    # 生成模拟购销数据
│   ├── requirements.txt               # Python依赖
│   └── config/                        # 数据生成配置
│       └── data_config.py             # 配置参数
│
├── etl-core/                          # ETL核心处理逻辑
│   ├── pom.xml                        # Maven配置
│   ├── src/                           # 源代码
│   │   └── main/
│   │       ├── scala/                 # Scala代码
│   │       │   └── com/sales/
│   │       │       ├── SalesDataETL.scala    # ETL入口类
│   │       │       ├── jobs/                 # 各ETL作业
│   │       │       ├── utils/                # 工具类
│   │       │       └── config/               # 配置加载
│   │       └── resources/              # 资源文件
│   │           ├── application.conf     # 应用配置
│   │           └── log4j2.xml           # 日志配置
│   ├── scripts/                       # 脚本
│   │   └── run_etl_pipeline.sh        # ETL执行脚本
│   ├── build.sh                       # 构建脚本
│   └── run-etl.sh                     # ETL执行入口
│
├── data-warehouse/                    # 数据仓库定义
│   ├── hive/                          # Hive脚本
│   │   ├── create_ods_tables.hql      # ODS层表创建
│   │   ├── add_ods_partitions.hql     # 分区添加
│   │   ├── ods_to_dwd.hql             # ODS到DWD转换
│   │   ├── dwd_to_dws.hql             # DWD到DWS聚合
│   │   └── data_quality_check.hql     # 数据质量检查
│   ├── doris/                         # Doris脚本
│   │   └── hive_to_doris_sync.py      # Hive到Doris同步
│   └── datax/                         # DataX配置
│       ├── mysql_to_hive_users.json   # MySQL到Hive的作业配置
│       └── run_datax_jobs.py          # DataX执行脚本
│
├── api-service/                       # 后端API服务
│   ├── pom.xml                        # Maven配置
│   └── src/                           # 源代码
│       └── main/
│           ├── java/                  # Java代码
│           │   └── com/sales/api/
│           │       ├── controller/    # API控制器
│           │       ├── service/       # 业务逻辑
│           │       ├── model/         # 数据模型
│           │       └── config/        # 配置类
│           └── resources/             # 资源文件
│               ├── application.yml    # 应用配置
│               └── logback.xml        # 日志配置
│
├── web-ui/                            # 前端Web界面
│   ├── package.json                   # npm配置
│   ├── public/                        # 静态资源
│   └── src/                           # 源代码
│       ├── views/                     # 视图组件
│       │   ├── sales/                 # 销售分析
│       │   │   ├── ProductSales.vue   # 产品销售分析
│       │   │   └── RegionSales.vue    # 区域销售分析
│       │   └── inventory/             # 库存分析
│       │       └── InventoryTurnover.vue  # 库存周转分析
│       ├── components/                # 通用组件
│       ├── api/                       # API调用
│       ├── router/                    # 路由配置
│       ├── store/                     # 状态管理
│       ├── assets/                    # 资源文件
│       └── App.vue                    # 根组件
│
└── docker/                            # Docker配置
    ├── Dockerfile                     # 应用容器定义
    └── docker-compose.yml             # 多容器编排配置
```

## 数据仓库分层

1. **ODS (Operational Data Store) 层**: 
   - 原始数据层，保持与源系统数据结构一致
   - 按天分区存储历史数据
   - 通过DataX从MySQL导入

2. **DWD (Data Warehouse Detail) 层**: 
   - 明细数据层，对ODS数据进行清洗和转换
   - 统一数据格式，处理空值和异常值
   - 通过Hive SQL从ODS转换而来

3. **DWS (Data Warehouse Service) 层**: 
   - 汇总数据层，面向业务主题的轻度汇总
   - 预计算常用统计指标
   - 通过Hive SQL从DWD聚合而来

4. **ADS (Application Data Service) 层**: 
   - 应用数据层，面向具体应用的高度汇总
   - 存储在Doris中，支持快速查询
   - 供前端界面展示使用

## 技术选型说明

1. **Hive**: 选择Hive作为数据仓库，利用其SQL兼容性和可扩展性
2. **Spark**: 用于大规模数据处理，提供高性能的ETL能力
3. **DataX**: 阿里开源的数据集成工具，用于MySQL到Hive的数据同步
4. **Doris**: 基于MPP架构的分析型数据库，提供毫秒级查询响应
5. **Spring Boot**: 用于构建RESTful API服务，连接前端和数据层
6. **Vue.js + Ant Design Vue**: 构建现代化、响应式的数据可视化界面

## 部署说明

本项目支持两种部署方式：

1. **独立部署**: 按模块分别部署在不同服务器上
2. **Docker部署**: 使用docker-compose进行容器化一键部署

详细部署步骤请参照各模块内的说明文档。 