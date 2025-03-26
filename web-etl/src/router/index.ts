import { createRouter, createWebHistory } from 'vue-router'
import { useUserStore } from '@/stores/user'
import type { RouteRecordRaw } from 'vue-router'

const routes: RouteRecordRaw[] = [
  {
    path: '/login',
    name: 'Login',
    component: () => import('@/views/login/index.vue'),
    meta: {
      title: '登录',
      requiresAuth: false
    }
  },
  {
    path: '/',
    name: 'Layout',
    component: () => import('@/layout/index.vue'),
    meta: {
      requiresAuth: true
    },
    children: [
      {
        path: '',
        name: 'Home',
        component: () => import('@/views/home/index.vue'),
        meta: {
          title: '首页',
          requiresAuth: true
        }
      },
      {
        path: 'dws',
        name: 'DWS',
        component: () => import('@/views/dws/index.vue'),
        meta: {
          title: 'DWS层数据管理',
          requiresAuth: true
        },
        children: [
          {
            path: '',
            name: 'DWSList',
            component: () => import('@/views/dws/list/index.vue'),
            meta: {
              title: 'DWS表管理',
              requiresAuth: true
            }
          },
          {
            path: 'preview/:tableName',
            name: 'DWSPreview',
            component: () => import('@/views/dws/preview/index.vue'),
            meta: {
              title: '数据预览',
              requiresAuth: true
            }
          },
          {
            path: 'quality',
            name: 'DWSQuality',
            component: () => import('@/views/dws/quality/index.vue'),
            meta: {
              title: '数据质量',
              requiresAuth: true
            }
          },
          {
            path: 'tasks',
            name: 'DWSTasks',
            component: () => import('@/views/dws/tasks/index.vue'),
            meta: {
              title: '任务管理',
              requiresAuth: true
            }
          }
        ]
      }
    ]
  }
]

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes
})

router.beforeEach((to, from, next) => {
  const userStore = useUserStore()
  const requiresAuth = to.matched.some(record => record.meta.requiresAuth)
  
  // 设置页面标题
  document.title = to.meta.title ? `${to.meta.title} - 数据中台管理系统` : '数据中台管理系统'
  
  if (requiresAuth && !userStore.isLogin) {
    next({
      path: '/login',
      query: { redirect: to.fullPath }
    })
  } else if (to.path === '/login' && userStore.isLogin) {
    next('/')
  } else {
    next()
  }
})

export default router
