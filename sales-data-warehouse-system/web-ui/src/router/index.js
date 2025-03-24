import Vue from 'vue'
import VueRouter from 'vue-router'
import Layout from '@/views/layout/Layout.vue'

Vue.use(VueRouter)

// 路由配置
const routes = [
  {
    path: '/login',
    name: 'Login',
    component: () => import('@/views/login/Login.vue'),
    meta: { title: '登录', auth: false }
  },
  {
    path: '/',
    component: Layout,
    redirect: '/dashboard',
    children: [
      {
        path: 'dashboard',
        name: 'Dashboard',
        component: () => import('@/views/dashboard/Dashboard.vue'),
        meta: { title: '仪表盘', icon: 'dashboard', auth: true }
      },
      {
        path: 'sales',
        name: 'Sales',
        component: { render: h => h('router-view') },
        meta: { title: '销售分析', icon: 'line-chart', auth: true },
        children: [
          {
            path: 'trend',
            name: 'SalesTrend',
            component: () => import('@/views/sales/SalesTrend.vue'),
            meta: { title: '销售趋势', auth: true }
          },
          {
            path: 'products',
            name: 'ProductSales',
            component: () => import('@/views/sales/ProductSales.vue'),
            meta: { title: '产品销售', auth: true }
          },
          {
            path: 'region',
            name: 'RegionSales',
            component: () => import('@/views/sales/RegionSales.vue'),
            meta: { title: '地域销售', auth: true }
          }
        ]
      },
      {
        path: 'customer',
        name: 'Customer',
        component: { render: h => h('router-view') },
        meta: { title: '客户分析', icon: 'user', auth: true },
        children: [
          {
            path: 'rfm',
            name: 'CustomerRFM',
            component: () => import('@/views/customer/CustomerRFM.vue'),
            meta: { title: 'RFM分析', auth: true }
          }
        ]
      },
      {
        path: 'purchase',
        name: 'Purchase',
        component: { render: h => h('router-view') },
        meta: { title: '采购分析', icon: 'shopping-cart', auth: true },
        children: [
          {
            path: 'supplier',
            name: 'SupplierAnalysis',
            component: () => import('@/views/purchase/SupplierAnalysis.vue'),
            meta: { title: '供应商分析', auth: true }
          },
          {
            path: 'cost',
            name: 'PurchaseCost',
            component: () => import('@/views/purchase/PurchaseCost.vue'),
            meta: { title: '采购成本', auth: true }
          }
        ]
      },
      {
        path: 'inventory',
        name: 'Inventory',
        component: { render: h => h('router-view') },
        meta: { title: '库存分析', icon: 'database', auth: true },
        children: [
          {
            path: 'turnover',
            name: 'InventoryTurnover',
            component: () => import('@/views/inventory/InventoryTurnover.vue'),
            meta: { title: '库存周转', auth: true }
          }
        ]
      }
    ]
  },
  {
    path: '/404',
    component: () => import('@/views/error/404.vue'),
    meta: { title: '404', auth: false }
  },
  {
    path: '*',
    redirect: '/404'
  }
]

const router = new VueRouter({
  routes
})

// 全局前置守卫
router.beforeEach((to, from, next) => {
  // 设置页面标题
  document.title = to.meta.title ? `${to.meta.title} - 销售数据仓库系统` : '销售数据仓库系统'
  
  // 检查是否需要登录认证
  if (to.meta.auth) {
    const token = localStorage.getItem('token')
    if (!token) {
      next({ path: '/login', query: { redirect: to.fullPath } })
      return
    }
  }
  
  next()
})

export default router 