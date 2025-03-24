# Docker部署模块

## 概述

Docker部署模块提供了基于Docker容器技术的一键部署解决方案，用于快速搭建购销数据仓库系统的完整运行环境。该模块使用Docker Compose编排多个容器服务，包括数据库、ETL处理、API服务和前端UI，实现系统各组件的协同工作。

## 功能特点

- 基于Docker的容器化部署方案
- 使用Docker Compose进行多容器编排
- 预配置的容器间通信和依赖关系
- 统一的环境变量配置管理
- 数据持久化存储解决方案
- 支持开发、测试和生产环境的灵活切换
- 内置健康检查和服务自动重启

## 目录结构

```
docker/
├── Dockerfile                # 应用主容器定义
├── docker-compose.yml        # 多容器编排配置
└── configs/                  # 配置文件
    ├── mysql/                # MySQL配置
    │   └── init.sql          # 数据库初始化脚本
    ├── hadoop/               # Hadoop配置
    │   └── core-site.xml     # 核心配置
    ├── hive/                 # Hive配置
    │   └── hive-site.xml     # Hive配置文件 
    ├── spark/                # Spark配置
    │   └── spark-defaults.conf # Spark默认配置
    └── doris/                # Doris配置
        └── fe.conf           # Doris前端配置
```

## 环境要求

- Docker 19.03.0+
- Docker Compose 1.27.0+
- 至少8GB可用内存
- 至少20GB磁盘空间
- 支持Linux、macOS或Windows操作系统

## 快速启动

### 基本用法

在项目根目录下执行以下命令启动整个系统：

```bash
cd docker
docker-compose up -d
```

使用以下命令查看服务状态：

```bash
docker-compose ps
```

使用以下命令查看服务日志：

```bash
docker-compose logs -f
```

### 停止服务

```bash
docker-compose down
```

如需保留数据卷：

```bash
docker-compose down --volumes
```

## 容器服务说明

Docker Compose配置文件定义了以下服务：

### 1. MySQL数据库

```yaml
mysql:
  image: mysql:5.7
  container_name: sales_mysql
  ports:
    - "3306:3306"
  volumes:
    - mysql_data:/var/lib/mysql
    - ./configs/mysql/init.sql:/docker-entrypoint-initdb.d/init.sql
  environment:
    MYSQL_ROOT_PASSWORD: password
    MYSQL_DATABASE: sales_db
  restart: always
```

作为系统的主要数据源，存储原始销售和库存数据。

### 2. Hadoop和Hive服务

```yaml
hadoop:
  image: harisekhon/hadoop:2.7
  container_name: sales_hadoop
  ports:
    - "9000:9000"
    - "50070:50070"
  volumes:
    - hadoop_data:/hadoop/dfs
    - ./configs/hadoop:/hadoop/etc/hadoop
  environment:
    HADOOP_HOME: /hadoop
  restart: always

hive:
  image: harisekhon/hive:2.3
  container_name: sales_hive
  depends_on:
    - hadoop
    - mysql
  ports:
    - "10000:10000"
    - "9083:9083"
  volumes:
    - ./configs/hive:/opt/hive/conf
  environment:
    HIVE_HOME: /opt/hive
    HADOOP_HOME: /hadoop
  restart: always
```

提供数据仓库的核心存储和处理功能。

### 3. Spark服务

```yaml
spark:
  image: bitnami/spark:3.1.2
  container_name: sales_spark
  depends_on:
    - hadoop
    - hive
  ports:
    - "4040:4040"
    - "7077:7077"
    - "8080:8080"
  volumes:
    - ./configs/spark:/opt/bitnami/spark/conf
    - ./etl-core:/app
  environment:
    SPARK_MODE: master
    SPARK_MASTER_URL: spark://spark:7077
    SPARK_WORKER_MEMORY: 2G
  restart: always

spark-worker:
  image: bitnami/spark:3.1.2
  container_name: sales_spark_worker
  depends_on:
    - spark
  environment:
    SPARK_MODE: worker
    SPARK_MASTER_URL: spark://spark:7077
    SPARK_WORKER_MEMORY: 2G
  restart: always
```

提供ETL处理的计算引擎。

### 4. Doris服务

```yaml
doris-fe:
  image: apache/doris:1.2.0-fe
  container_name: sales_doris_fe
  ports:
    - "8030:8030"
    - "9030:9030"
  volumes:
    - doris_fe_data:/opt/apache-doris/fe/doris-meta
    - ./configs/doris:/opt/apache-doris/fe/conf
  environment:
    FE_SERVERS: doris-fe:9010
    DORIS_CLUSTER_NAME: sales-cluster
  restart: always

doris-be:
  image: apache/doris:1.2.0-be
  container_name: sales_doris_be
  depends_on:
    - doris-fe
  ports:
    - "8040:8040"
  volumes:
    - doris_be_data:/opt/apache-doris/be/storage
  environment:
    FE_SERVERS: doris-fe:9010
    DORIS_CLUSTER_NAME: sales-cluster
  restart: always
```

提供快速查询分析引擎。

### 5. API服务

```yaml
api-service:
  build:
    context: ../api-service
    dockerfile: Dockerfile
  container_name: sales_api
  depends_on:
    - mysql
    - doris-fe
  ports:
    - "8080:8080"
  volumes:
    - ./configs/api:/app/config
  environment:
    SPRING_PROFILES_ACTIVE: prod
    DORIS_URL: jdbc:mysql://doris-fe:9030/sales_ads
    MYSQL_URL: jdbc:mysql://mysql:3306/sales_config
  restart: always
```

提供后端API服务。

### 6. Web UI服务

```yaml
web-ui:
  build:
    context: ../web-ui
    dockerfile: Dockerfile
  container_name: sales_web
  depends_on:
    - api-service
  ports:
    - "80:80"
  environment:
    API_URL: http://api-service:8080/api
  restart: always
```

提供前端Web界面。

## 数据卷

系统使用以下数据卷进行持久化存储：

```yaml
volumes:
  mysql_data:
  hadoop_data:
  doris_fe_data:
  doris_be_data:
```

这些卷会在容器重启后保留数据。

## 自定义配置

### 修改环境变量

可以通过创建`.env`文件在项目根目录下设置环境变量：

```
# 数据库配置
MYSQL_ROOT_PASSWORD=your_secure_password
MYSQL_DATABASE=sales_db

# Hadoop和Hive配置
HADOOP_HEAP_SIZE=1G
HIVE_HEAP_SIZE=1G

# Spark配置
SPARK_WORKER_MEMORY=4G
SPARK_DRIVER_MEMORY=2G
SPARK_EXECUTOR_MEMORY=2G

# Doris配置
DORIS_CLUSTER_NAME=sales-cluster

# API服务配置
SPRING_PROFILES_ACTIVE=prod
API_PORT=8080

# Web UI配置
WEB_PORT=80
```

### 修改配置文件

可以修改`configs`目录下的各组件配置文件，例如：

- MySQL: `configs/mysql/init.sql`
- Hadoop: `configs/hadoop/core-site.xml`
- Hive: `configs/hive/hive-site.xml`
- Spark: `configs/spark/spark-defaults.conf`
- Doris: `configs/doris/fe.conf`

## 部署示例

### 开发环境

适用于本地开发和测试：

```bash
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d
```

### 生产环境

适用于生产部署：

```bash
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

## 扩展与定制

### 添加新服务

如需添加新服务，遵循以下步骤：

1. 在`docker-compose.yml`中定义新服务
2. 设置适当的依赖关系和网络配置
3. 配置数据卷和环境变量

示例：

```yaml
new-service:
  image: your-image:tag
  container_name: sales_new_service
  depends_on:
    - mysql
  ports:
    - "8888:8888"
  volumes:
    - ./configs/new-service:/app/config
  environment:
    ENV_VAR1: value1
    ENV_VAR2: value2
  restart: always
```

### 自定义构建

如需自定义应用容器，可以修改`Dockerfile`：

```dockerfile
FROM openjdk:8-jdk-alpine

WORKDIR /app

COPY target/sales-data-warehouse-api-1.0.0.jar app.jar

ENV JAVA_OPTS="-Xmx512m -Xms256m"

EXPOSE 8080

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
```

## 性能调优

### 内存分配

根据系统资源情况，可以调整各服务的内存分配：

```yaml
environment:
  JAVA_OPTS: "-Xmx2g -Xms1g"  # API服务JVM内存配置
  SPARK_DRIVER_MEMORY: 2g     # Spark驱动程序内存
  SPARK_EXECUTOR_MEMORY: 2g   # Spark执行器内存
```

### 并行度设置

对于Spark和Hadoop服务，可以调整并行度：

```yaml
environment:
  SPARK_EXECUTOR_CORES: 2     # Spark执行器核心数
  SPARK_DEFAULT_PARALLELISM: 8 # Spark默认并行度
  HADOOP_NAMENODE_HEAP_SIZE: 1g # Hadoop NameNode堆大小
```

## 监控与日志

### 查看容器日志

```bash
# 查看特定服务的日志
docker-compose logs -f api-service

# 查看所有服务的日志
docker-compose logs -f
```

### 使用Portainer进行容器管理

可以添加Portainer服务进行可视化容器管理：

```yaml
portainer:
  image: portainer/portainer-ce
  container_name: sales_portainer
  ports:
    - "9000:9000"
  volumes:
    - /var/run/docker.sock:/var/run/docker.sock
    - portainer_data:/data
  restart: always
```

## 故障排除

### 常见问题

1. **容器启动失败**
   - 检查日志: `docker-compose logs -f <service-name>`
   - 确认依赖服务是否已启动
   - 验证端口是否被占用

2. **服务连接问题**
   - 检查网络配置
   - 确认服务名称解析正确
   - 验证端口映射配置

3. **数据持久化问题**
   - 确认数据卷配置正确
   - 检查容器内文件路径
   - 确认权限设置正确

### 重置环境

如需完全重置环境：

```bash
# 停止所有容器并删除数据卷
docker-compose down -v

# 删除所有相关镜像
docker image prune -af --filter "label=com.docker.compose.project=sales-data-warehouse"

# 重新构建并启动
docker-compose up -d --build
``` 