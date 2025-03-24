#!/bin/bash

# 基于Hive的购销数据仓库系统项目清理脚本

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

# 确认函数
confirm() {
  read -p "$1 [y/N] " response
  case "$response" in
    [yY][eE][sS]|[yY]) 
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

# 提示用户
warn "此脚本将删除项目中的无用文件。确保你已经运行了 reorganize_project.sh 并验证新结构正确。"
warn "强烈建议在执行此脚本前备份你的项目！"

if ! confirm "是否继续清理操作？"; then
  info "操作已取消。"
  exit 0
fi

# 删除无用文件和目录
info "开始清理无用文件..."

# 临时文件和构建产物
if [ -d "target" ]; then
  info "删除 target/ 目录 (构建产物)..."
  rm -rf target/
fi

if [ -d ".idea" ]; then
  info "删除 .idea/ 目录 (IDE配置)..."
  rm -rf .idea/
fi

if [ -d ".vscode" ]; then
  info "删除 .vscode/ 目录 (IDE配置)..."
  rm -rf .vscode/
fi

# 示例文件
if [ -d "examples" ]; then
  if confirm "是否删除 examples/ 目录？这可能包含示例代码。"; then
    info "删除 examples/ 目录..."
    rm -rf examples/
  else
    info "保留 examples/ 目录。"
  fi
fi

# 临时文档
if [ -f "messages.md" ]; then
  if confirm "是否删除 messages.md？这可能是临时文档。"; then
    info "删除 messages.md..."
    rm -f messages.md
  else
    info "保留 messages.md。"
  fi
fi

if [ -f "spark.md" ]; then
  if confirm "是否删除 spark.md？这可能是临时文档。"; then
    info "删除 spark.md..."
    rm -f spark.md
  else
    info "保留 spark.md。"
  fi
fi

# 重复的文件
if [ -f "run-etl.sh" ]; then
  if confirm "是否删除根目录的 run-etl.sh？该文件已被复制到新结构中。"; then
    info "删除根目录的 run-etl.sh..."
    rm -f run-etl.sh
  else
    info "保留根目录的 run-etl.sh。"
  fi
fi

if [ -f "pom.xml" ]; then
  if confirm "是否删除根目录的 pom.xml？该文件已被复制到新结构中。"; then
    info "删除根目录的 pom.xml..."
    rm -f pom.xml
  else
    info "保留根目录的 pom.xml。"
  fi
fi

# 检查是否保留原始目录结构
warn "现在已经创建了新的项目结构 'sales-data-warehouse-system'，并且可能已删除了一些无用文件。"
warn "但原始的目录结构仍然存在。"

if confirm "是否完全删除旧的目录结构？这将删除：sales-data-warehouse/, sales-data-warehouse-api/, sales-data-warehouse-web/, scripts/, config/"; then
  info "删除旧目录结构..."
  
  if [ -d "sales-data-warehouse" ]; then
    rm -rf sales-data-warehouse/
  fi
  
  if [ -d "sales-data-warehouse-api" ]; then
    rm -rf sales-data-warehouse-api/
  fi
  
  if [ -d "sales-data-warehouse-web" ]; then
    rm -rf sales-data-warehouse-web/
  fi
  
  if [ -d "scripts" ]; then
    rm -rf scripts/
  fi
  
  if [ -d "config" ]; then
    rm -rf config/
  fi
  
  info "旧目录结构已删除。"
else
  info "保留旧目录结构。"
fi

# 删除数据生成文件
if confirm "是否删除根目录的 fakeData.py？该文件已被复制到新结构中。"; then
  info "删除根目录的 fakeData.py..."
  rm -f fakeData.py
else
  info "保留根目录的 fakeData.py。"
fi

if confirm "是否删除根目录的 requirements.txt？该文件已被复制到新结构中。"; then
  info "删除根目录的 requirements.txt..."
  rm -f requirements.txt
else
  info "保留根目录的 requirements.txt。"
fi

# Docker相关文件
if confirm "是否删除根目录的 Dockerfile？该文件已被复制到新结构中。"; then
  info "删除根目录的 Dockerfile..."
  rm -f Dockerfile
else
  info "保留根目录的 Dockerfile。"
fi

if confirm "是否删除根目录的 docker-compose.yml？该文件已被复制到新结构中。"; then
  info "删除根目录的 docker-compose.yml..."
  rm -f docker-compose.yml
else
  info "保留根目录的 docker-compose.yml。"
fi

info "清理操作完成！"
info "新的项目结构位于: sales-data-warehouse-system/"
info "请检查整理结果，确保所有重要文件都已正确保留。" 