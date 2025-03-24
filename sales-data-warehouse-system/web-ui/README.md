# 前端Web UI模块

## 概述

前端Web UI模块是购销数据仓库系统的可视化界面，基于Vue.js和Ant Design Vue构建，为用户提供直观、交互式的数据分析体验。该模块通过调用后端API服务获取数据，并使用现代化的图表和组件展示销售、库存等核心业务指标，支持多维度的数据分析和探索。

## 功能特点

- 现代化、响应式的用户界面设计
- 丰富的数据可视化图表和仪表盘
- 多维度数据筛选和分析能力
- 灵活的时间范围选择和比较
- 支持数据导出和报表生成
- 完善的用户权限控制
- 良好的移动设备适配性

## 目录结构

```
web-ui/
├── package.json             # npm配置文件
├── public/                  # 静态资源
│   ├── favicon.ico          # 网站图标
│   └── index.html           # HTML模板
├── src/                     # 源代码
│   ├── assets/              # 资源文件
│   │   ├── logo.png         # 项目logo
│   │   ├── icons/           # 图标资源
│   │   └── styles/          # 样式文件
│   ├── components/          # 通用组件
│   │   ├── charts/          # 图表组件
│   │   │   ├── LineChart.vue  # 折线图
│   │   │   ├── BarChart.vue   # 柱状图
│   │   │   └── PieChart.vue   # 饼图
│   │   ├── layout/          # 布局组件
│   │   │   ├── Header.vue     # 头部组件
│   │   │   ├── Sidebar.vue    # 侧边栏
│   │   │   └── Footer.vue     # 底部组件
│   │   └── common/          # 通用组件
│   │       ├── DataTable.vue  # 数据表格
│   │       └── FilterBar.vue  # 筛选栏
│   ├── views/               # 页面视图
│   │   ├── dashboard/       # 仪表盘
│   │   │   └── Index.vue    # 首页仪表盘
│   │   ├── sales/           # 销售分析
│   │   │   ├── Overview.vue   # 销售概览
│   │   │   ├── ProductSales.vue # 产品销售
│   │   │   └── RegionSales.vue  # 区域销售
│   │   └── inventory/       # 库存分析
│   │       ├── Overview.vue   # 库存概览
│   │       └── Turnover.vue   # 库存周转
│   ├── router/              # 路由配置
│   │   └── index.js         # 路由定义
│   ├── store/               # 状态管理
│   │   ├── index.js         # Store入口
│   │   ├── modules/         # 模块
│   │   │   ├── user.js      # 用户模块
│   │   │   └── app.js       # 应用模块
│   │   └── getters.js       # Getter
│   ├── api/                 # API调用
│   │   ├── request.js       # 请求封装
│   │   ├── sales.js         # 销售API
│   │   └── inventory.js     # 库存API
│   ├── utils/               # 工具函数
│   │   ├── auth.js          # 认证工具
│   │   ├── date.js          # 日期工具
│   │   └── export.js        # 导出工具
│   ├── config/              # 配置文件
│   │   └── settings.js      # 应用配置
│   ├── App.vue              # 根组件
│   └── main.js              # 入口文件
├── .env                     # 环境变量
├── .env.development         # 开发环境变量
└── .env.production          # 生产环境变量
```

## 环境要求

- Node.js 14.0+
- npm 6.0+ 或 yarn 1.20+
- 现代浏览器 (Chrome, Firefox, Safari, Edge)

## 安装与运行

### 安装依赖

```bash
cd web-ui
npm install
# 或者
yarn install
```

### 开发环境运行

```bash
npm run serve
# 或者
yarn serve
```

默认情况下，开发服务器将启动在 http://localhost:8080

### 生产环境构建

```bash
npm run build
# 或者
yarn build
```

构建后的文件将生成在`dist`目录中，可直接部署到静态文件服务器。

## 配置说明

### 环境变量配置

项目使用`.env`文件系列进行环境配置：

**.env.development** (开发环境)
```
NODE_ENV=development
VUE_APP_BASE_API=http://localhost:8080/api
VUE_APP_MOCK_ENABLE=true
```

**.env.production** (生产环境)
```
NODE_ENV=production
VUE_APP_BASE_API=/api
VUE_APP_MOCK_ENABLE=false
```

### 应用配置

在`src/config/settings.js`中可配置应用的全局设置：

```javascript
export default {
  // 应用名称
  title: '购销数据仓库系统',
  
  // 侧边栏主题 'light' | 'dark'
  sideTheme: 'dark',
  
  // 是否显示设置
  showSettings: true,
  
  // 是否显示标签视图
  tagsView: true,
  
  // 是否固定头部
  fixedHeader: true,
  
  // 是否显示侧边栏Logo
  sidebarLogo: true,
  
  // 导航模式 'side' | 'top'
  layout: 'side',
  
  // 主题色
  themeColor: '#1890ff',
  
  // 内容区域宽度 'fluid' | 'fixed'
  contentWidth: 'fixed',
  
  // 默认语言
  language: 'zh-CN',
  
  // 时间显示格式
  dateFormat: 'YYYY-MM-DD',
  
  // 每页默认记录数
  defaultPageSize: 10
};
```

## 页面说明

### 仪表盘

仪表盘页面提供系统关键指标的概览，包含：
- 销售总额和订单数量统计
- 销售额趋势图
- 产品类别销售分布
- 热销产品排行
- 库存状态概览

### 销售分析

#### 销售概览

提供销售数据的总体视图，包含：
- 销售额和订单数时间趋势
- 销售周期性分析
- 销售额年度/季度/月度比较

#### 产品销售分析

分析产品维度的销售数据，包含：
- 产品销售排行榜
- 产品类别销售占比
- 产品销售趋势分析
- 产品单价分析

#### 区域销售分析

分析地域维度的销售数据，包含：
- 区域销售热力图
- 区域销售排行
- 区域销售增长率分析

### 库存分析

#### 库存概览

提供库存状态的总体视图，包含：
- 库存总量和价值统计
- 库存结构分析
- 低库存和过量库存预警

#### 库存周转分析

分析库存周转情况，包含：
- 库存周转率趋势
- 库存周转率产品/类别比较
- 库存滞留天数分析

## 用户指南

### 登录与权限

系统支持以下角色：
- 管理员：可访问所有功能
- 销售经理：可访问销售相关报表
- 库存管理员：可访问库存相关报表
- 普通用户：只能查看基础仪表盘

登录流程：
1. 访问系统登录页
2. 输入用户名和密码
3. 系统验证身份并分配相应权限

### 数据筛选

所有分析页面都支持多维度数据筛选：
- 时间范围选择
- 产品类别筛选
- 区域筛选
- 销售渠道筛选

筛选器位于页面顶部，可以组合使用多个筛选条件。

### 图表交互

系统中的所有图表都支持丰富的交互功能：
- 鼠标悬停显示详细数据
- 点击图表元素进行下钻分析
- 缩放和平移时间序列图表
- 显示/隐藏图表系列

### 数据导出

系统支持以下格式的数据导出：
- Excel (.xlsx)
- CSV (.csv)
- PDF 报表

导出按钮位于各数据表格和图表的右上角。

## 开发指南

### 添加新页面

1. 在`src/views`中创建新的Vue组件
2. 在`src/router/index.js`中添加路由配置
3. 在侧边栏配置中添加菜单项

示例路由配置：
```javascript
{
  path: '/customer',
  component: Layout,
  redirect: '/customer/analysis',
  name: 'Customer',
  meta: { title: '客户分析', icon: 'user' },
  children: [
    {
      path: 'analysis',
      name: 'CustomerAnalysis',
      component: () => import('@/views/customer/Analysis'),
      meta: { title: '客户分析', icon: 'chart' }
    }
  ]
}
```

### 添加新API

1. 在`src/api`中创建新的API模块文件
2. 使用`request`工具函数封装API调用

示例：
```javascript
import request from '@/utils/request'

export function getCustomerList(params) {
  return request({
    url: '/customer/list',
    method: 'get',
    params
  })
}

export function getCustomerDetails(id) {
  return request({
    url: `/customer/${id}`,
    method: 'get'
  })
}
```

### 添加新图表

1. 在`src/components/charts`中创建新的图表组件
2. 使用echarts或其他图表库实现可视化

示例：
```vue
<template>
  <div :class="className" :style="{height:height,width:width}" />
</template>

<script>
import * as echarts from 'echarts'

export default {
  props: {
    className: {
      type: String,
      default: 'chart'
    },
    width: {
      type: String,
      default: '100%'
    },
    height: {
      type: String,
      default: '350px'
    },
    chartData: {
      type: Object,
      required: true
    }
  },
  data() {
    return {
      chart: null
    }
  },
  watch: {
    chartData: {
      deep: true,
      handler(val) {
        this.setOptions(val)
      }
    }
  },
  mounted() {
    this.initChart()
  },
  beforeDestroy() {
    if (!this.chart) {
      return
    }
    this.chart.dispose()
    this.chart = null
  },
  methods: {
    initChart() {
      this.chart = echarts.init(this.$el)
      this.setOptions(this.chartData)
    },
    setOptions({ categories, series }) {
      this.chart.setOption({
        tooltip: {
          trigger: 'axis'
        },
        legend: {
          data: series.map(item => item.name)
        },
        xAxis: {
          type: 'category',
          data: categories
        },
        yAxis: {
          type: 'value'
        },
        series: series.map(item => ({
          name: item.name,
          type: 'bar',
          data: item.data
        }))
      })
    }
  }
}
</script>
```

## 性能优化

前端项目已实施以下性能优化措施：

1. **路由懒加载**：仅在需要时加载页面组件
2. **组件按需导入**：Ant Design Vue组件按需引入
3. **图片优化**：使用WebP格式和适当压缩
4. **Tree Shaking**：移除未使用的代码
5. **缓存策略**：合理使用浏览器缓存
6. **Gzip压缩**：减少传输体积

## 浏览器兼容性

- Chrome (最新版)
- Firefox (最新版)
- Safari (最新版)
- Edge (最新版)
- IE11需要额外的polyfill支持

## 常见问题

1. **开发环境API跨域问题**
   
   在`vue.config.js`中配置代理：
   
   ```javascript
   devServer: {
     proxy: {
       '/api': {
         target: 'http://localhost:8080',
         changeOrigin: true,
         pathRewrite: {
           '^/api': ''
         }
       }
     }
   }
   ```

2. **图表不显示问题**
   
   确保容器有明确的宽高，并在窗口大小变化时调用`resize`方法：
   
   ```javascript
   mounted() {
     this.chart = echarts.init(this.$el)
     window.addEventListener('resize', this.resize)
   },
   beforeDestroy() {
     window.removeEventListener('resize', this.resize)
     this.chart.dispose()
     this.chart = null
   },
   methods: {
     resize() {
       this.chart && this.chart.resize()
     }
   }
   ```

3. **构建后资源路径问题**
   
   在`vue.config.js`中设置publicPath：
   
   ```javascript
   module.exports = {
     publicPath: process.env.NODE_ENV === 'production' ? '/sales-dashboard/' : '/'
   }
   ``` 