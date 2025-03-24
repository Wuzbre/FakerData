#!/bin/bash

# 基于Hive的购销数据仓库系统项目结构重组脚本

# 设置颜色输出
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# 打印信息函数
info() {
  echo -e "${GREEN}[INFO]${NC} $1"
}

warn() {
  echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
  echo -e "${RED}[ERROR]${NC} $1"
}

# 创建主项目目录
info "创建项目主目录结构..."
mkdir -p sales-data-warehouse-system
mkdir -p sales-data-warehouse-system/data-generation
mkdir -p sales-data-warehouse-system/etl-core/src/main/scala
mkdir -p sales-data-warehouse-system/etl-core/src/main/resources
mkdir -p sales-data-warehouse-system/etl-core/scripts
mkdir -p sales-data-warehouse-system/data-warehouse/hive
mkdir -p sales-data-warehouse-system/data-warehouse/doris
mkdir -p sales-data-warehouse-system/data-warehouse/datax
mkdir -p sales-data-warehouse-system/api-service
mkdir -p sales-data-warehouse-system/web-ui
mkdir -p sales-data-warehouse-system/docker

# 拷贝和移动文件
info "拷贝数据生成相关文件..."
cp fakeData.py sales-data-warehouse-system/data-generation/
cp requirements.txt sales-data-warehouse-system/data-generation/
cp -r config/ sales-data-warehouse-system/data-generation/

info "拷贝ETL核心文件..."
cp sales-data-warehouse/pom.xml sales-data-warehouse-system/etl-core/
cp sales-data-warehouse/run-etl.sh sales-data-warehouse-system/etl-core/
cp sales-data-warehouse/build.sh sales-data-warehouse-system/etl-core/
cp -r sales-data-warehouse/src/main/resources/* sales-data-warehouse-system/etl-core/src/main/resources/
cp -r sales-data-warehouse/src/main/scala/* sales-data-warehouse-system/etl-core/src/main/scala/
cp scripts/run_etl_pipeline.sh sales-data-warehouse-system/etl-core/scripts/

info "拷贝数据仓库文件..."
cp -r sales-data-warehouse/hive/* sales-data-warehouse-system/data-warehouse/hive/
cp -r sales-data-warehouse/doris/* sales-data-warehouse-system/data-warehouse/doris/
cp -r sales-data-warehouse/datax/* sales-data-warehouse-system/data-warehouse/datax/

info "拷贝API服务文件..."
cp -r sales-data-warehouse-api/* sales-data-warehouse-system/api-service/

info "拷贝前端UI文件..."
cp -r sales-data-warehouse-web/* sales-data-warehouse-system/web-ui/

info "拷贝Docker文件..."
cp Dockerfile sales-data-warehouse-system/docker/
cp docker-compose.yml sales-data-warehouse-system/docker/

# 创建主README文件
info "创建主README文件..."
cat > sales-data-warehouse-system/README.md << EOF
# 基于Hive的购销数据仓库系统

本项目是一个基于Hive、Spark和Doris构建的购销数据仓库系统，包含数据生成、ETL处理、API服务和前端展示等完整功能。

## 项目结构

- **data-generation/**: 模拟数据生成模块
- **etl-core/**: ETL核心处理逻辑
- **data-warehouse/**: 数据仓库结构定义和脚本
- **api-service/**: 后端API服务
- **web-ui/**: 前端Web界面
- **docker/**: Docker相关配置

## 安装与使用

请参考各模块下的README文件获取详细的使用说明。

## 技术栈

- 数据生成: Python, Faker
- 数据处理: Spark, Hive, DataX
- 数据查询: Doris
- 后端API: Spring Boot
- 前端UI: Vue.js, Ant Design Vue

## 贡献者

[在此处添加贡献者信息]

## 许可证

[在此处添加许可证信息]
EOF

info "项目结构重组完成！新的项目根目录是: sales-data-warehouse-system/"
info "您可以检查新结构并删除不需要的旧文件。" 