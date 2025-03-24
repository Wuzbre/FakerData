# 销售数据仓库前端项目

本项目是销售数据仓库系统的前端部分，基于Vue.js和Ant Design Vue构建，提供了数据可视化和分析功能。

## 项目特点

- 基于Vue.js框架构建的现代化前端应用
- 使用Ant Design Vue组件库，提供美观、一致的用户界面
- 集成ECharts图表库，实现丰富的数据可视化效果
- 响应式设计，适配不同设备屏幕

## 功能模块

- **仪表盘**: 提供销售数据的整体概况和关键指标
- **销售分析**: 
  - 销售趋势分析
  - 产品销售分析
  - 区域销售分析
- **客户分析**:
  - RFM客户价值分析
- **库存分析**:
  - 库存周转分析

## 项目构建与运行

### 环境要求

- Node.js (>= 10.x)
- npm (>= 6.x) 或 yarn (>= 1.x)

### 安装依赖

```bash
# 使用npm
npm install

# 或使用yarn
yarn install
```

### 开发模式运行

```bash
# 使用npm
npm run serve

# 或使用yarn
yarn serve
```

### 构建生产版本

```bash
# 使用npm
npm run build

# 或使用yarn
yarn build
```

### 配置后端API地址

在`.env.development`和`.env.production`文件中配置后端API地址:

```
VUE_APP_API_URL=http://your-api-server/api
```

## 项目结构

```
sales-data-warehouse-web/
├── public/                 # 静态资源
├── src/
│   ├── api/                # API请求封装
│   ├── assets/             # 项目资源文件
│   ├── components/         # 公共组件
│   ├── router/             # 路由配置
│   ├── store/              # Vuex状态管理
│   ├── utils/              # 工具函数
│   ├── views/              # 页面组件
│   │   ├── dashboard/      # 仪表盘
│   │   ├── sales/          # 销售分析
│   │   ├── customer/       # 客户分析
│   │   ├── inventory/      # 库存分析
│   │   └── layout/         # 布局组件
│   ├── App.vue             # 根组件
│   └── main.js             # 入口文件
├── .env.development        # 开发环境配置
├── .env.production         # 生产环境配置
├── package.json            # 项目依赖配置
└── README.md               # 项目说明
```

## 开发约定

- 组件名称使用PascalCase命名
- API请求集中在api目录管理
- 路由配置在router目录
- 页面模块在views目录按功能分组

## 技术栈

- Vue.js (2.x)
- Vue Router
- Vuex
- Ant Design Vue
- Axios
- ECharts
- Moment.js

## 部署说明

1. 执行`npm run build`构建项目
2. 将`dist`目录下生成的文件部署到Web服务器
3. 配置服务器将所有路由请求转发到index.html 