# ETL核心处理模块

## 概述

ETL核心模块是整个数据仓库系统的核心组件，负责从MySQL数据源抽取数据，通过Spark进行转换处理，并将结果加载到Hive数据仓库中。该模块使用Scala语言开发，基于Spark框架实现高性能的数据处理能力。

## 功能特点

- 基于Spark的分布式数据处理，支持大规模数据ETL
- 完整的数据清洗、转换和加载流程
- 支持增量和全量数据处理策略
- 内置数据质量检查机制
- 可配置的数据生命周期管理
- 任务调度和错误重试机制

## 目录结构

```
etl-core/
├── pom.xml                # Maven构建配置
├── src/                   # 源代码
│   └── main/
│       ├── scala/         # Scala代码
│       │   └── com/sales/
│       │       ├── SalesDataETL.scala     # ETL入口类
│       │       ├── jobs/                  # 各ETL作业
│       │       ├── utils/                 # 工具类
│       │       └── config/                # 配置加载
│       └── resources/      # 资源文件
│           ├── application.conf  # 应用配置
│           └── log4j2.xml        # 日志配置
├── scripts/                # 脚本
│   └── run_etl_pipeline.sh # ETL执行脚本
├── build.sh                # 构建脚本
└── run-etl.sh              # ETL执行入口
```

## 环境要求

- JDK 1.8+
- Spark 3.0+
- Hadoop 3.0+
- Hive 3.0+
- Maven 3.6+

## 构建与部署

### 构建项目

使用提供的构建脚本进行编译打包：

```bash
./build.sh
```

该脚本会执行以下操作：
1. 检查JDK环境
2. 使用Maven编译项目
3. 打包成可执行JAR文件
4. 验证构建结果

成功构建后，会在`target`目录下生成`sales-data-warehouse-1.0-SNAPSHOT-jar-with-dependencies.jar`文件。

### 配置文件

主要配置文件`application.conf`包含以下重要配置：

```hocon
spark {
  master = "local[*]"  # Spark执行模式，生产环境可改为yarn或standalone
  app.name = "Sales Data ETL"
  sql.warehouse.dir = "/user/hive/warehouse"
}

mysql {
  jdbc.url = "jdbc:mysql://localhost:3306/sales_db"
  user = "root"
  password = "password"
}

hive {
  metastore.uris = "thrift://localhost:9083"
  db.ods = "sales_ods"
  db.dwd = "sales_dwd"
  db.dws = "sales_dws"
}

doris {
  jdbc.url = "jdbc:mysql://localhost:9030/sales_ads"
  user = "root"
  password = "password"
}

etl {
  date.format = "yyyyMMdd"
  default.partition.count = 50
  
  lifecycle {
    ods.retention.days = 30
    dwd.retention.days = 90
    dws.retention.days = 180
  }
  
  processing {
    batch.size = 10000
    parallel.tasks = 4
  }
  
  data.quality {
    check.enabled = true
    null.threshold = 0.1
    duplicate.threshold = 0.01
  }
}
```

根据具体环境修改相关配置参数。

## 运行方式

### 执行完整ETL流程

```bash
./run-etl.sh
```

默认处理前一天的数据。

### 指定处理日期

```bash
./run-etl.sh 20231231
```

### 单独执行特定阶段

```bash
./run-etl.sh --date 20231231 --stage mysql2ods,ods2dwd
```

可选的阶段参数：
- `mysql2ods`: MySQL到ODS层抽取
- `ods2dwd`: ODS到DWD层转换
- `dwd2dws`: DWD到DWS层聚合
- `quality_check`: 数据质量检查
- `hive2doris`: Hive到Doris同步

## 日志管理

ETL执行日志存储在`log`目录下，按日期命名：

```
log/etl_20231231.log
```

日志级别可通过`src/main/resources/log4j2.xml`配置文件调整。

## 数据质量检查

ETL过程包含内置的数据质量检查机制，检查项目包括：

1. 空值比例检查
2. 重复值检查
3. 数据完整性检查
4. 数据一致性检查
5. 数据范围和有效性检查

质量检查结果会在日志中记录，并可选择性中断ETL流程。

## 错误处理

ETL作业具有错误重试机制，默认配置：

```hocon
etl {
  retry {
    max.attempts = 3
    backoff.ms = 5000
  }
}
```

## 监控与告警

ETL作业执行状态和指标可通过以下方式监控：

1. 日志文件分析
2. Spark UI（默认端口4040）
3. 执行结果状态码

可以通过集成第三方监控系统（如Prometheus、Grafana）进行更全面的监控。

## 扩展与定制

要添加新的ETL作业，遵循以下步骤：

1. 在`com.sales.jobs`包下创建新的作业类
2. 实现通用接口`ETLJob`
3. 在`SalesDataETL.scala`中注册新作业
4. 更新配置文件和执行脚本

## 故障排除

常见问题及解决方法：

1. **Spark作业失败**
   - 检查日志中的具体错误信息
   - 验证Spark配置参数是否合适
   - 确认集群资源是否充足

2. **数据质量检查失败**
   - 检查源数据的完整性和正确性
   - 调整质量检查阈值
   - 修复源数据问题后重新执行

3. **Hive连接问题**
   - 确认Hive元数据服务是否正常运行
   - 验证配置中的连接参数是否正确
   - 检查权限设置 