#!/bin/bash

# 销售数据仓库项目构建脚本

# 定义彩色输出函数
info() {
  echo -e "\033[34m[INFO]\033[0m $1"
}

warning() {
  echo -e "\033[33m[WARN]\033[0m $1"
}

error() {
  echo -e "\033[31m[ERROR]\033[0m $1"
}

success() {
  echo -e "\033[32m[SUCCESS]\033[0m $1"
}

# 检查Java环境
if type -p java >/dev/null; then
  _java=java
elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]]; then
  _java="$JAVA_HOME/bin/java"
else
  error "找不到Java环境，请安装JDK 8或更高版本"
  exit 1
fi

java_version=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
info "检测到Java版本: $java_version"

# 清理和构建项目
info "开始构建销售数据仓库项目..."

# 判断是否使用Maven Wrapper或本地Maven
if [ -f "./mvnw" ]; then
  MVN="./mvnw"
else
  MVN="mvn"
fi

# 执行Maven构建
info "执行Maven构建..."
$MVN clean package -DskipTests

# 检查构建结果
if [ $? -eq 0 ]; then
  success "项目构建成功！"
  
  # 检查jar包是否存在
  JAR_FILE="target/sales-data-warehouse-1.0-SNAPSHOT-jar-with-dependencies.jar"
  if [ -f "$JAR_FILE" ]; then
    success "生成的JAR包: $JAR_FILE ($(du -h "$JAR_FILE" | cut -f1))"
    info "可以使用以下命令运行ETL流程："
    echo -e "\033[36m./run-etl.sh [处理日期(可选,格式:yyyyMMdd)] [阶段参数(可选)]\033[0m"
  else
    warning "找不到生成的JAR包，请检查pom.xml配置"
  fi
else
  error "项目构建失败，请检查上述错误信息"
  exit 1
fi

exit 0 