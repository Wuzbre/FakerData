import axios from 'axios'
import { message } from 'ant-design-vue'
import router from '@/router'
import NProgress from 'nprogress'
import 'nprogress/nprogress.css'

// NProgress 配置
NProgress.configure({ showSpinner: false })

// 创建axios实例
const request = axios.create({
  baseURL: process.env.VUE_APP_API_URL || '/api',
  timeout: 30000
})

// 请求拦截器
request.interceptors.request.use(
  config => {
    NProgress.start()
    
    // 添加token到请求头
    const token = localStorage.getItem('token')
    if (token) {
      config.headers['Authorization'] = `Bearer ${token}`
    }
    
    return config
  },
  error => {
    NProgress.done()
    return Promise.reject(error)
  }
)

// 响应拦截器
request.interceptors.response.use(
  response => {
    NProgress.done()
    
    const res = response.data
    
    // 根据API约定的返回格式判断请求是否成功
    if (res.code !== 200) {
      message.error(res.message || '请求失败')
      
      // 401: 未登录或Token过期
      if (res.code === 401) {
        // 清除本地token
        localStorage.removeItem('token')
        router.push(`/login?redirect=${router.currentRoute.fullPath}`)
      }
      
      return Promise.reject(new Error(res.message || '请求失败'))
    }
    
    return res
  },
  error => {
    NProgress.done()
    
    // 处理HTTP错误状态码
    if (error.response) {
      const { status, data } = error.response
      
      switch (status) {
        case 401:
          message.error('未授权，请重新登录')
          localStorage.removeItem('token')
          router.push(`/login?redirect=${router.currentRoute.fullPath}`)
          break
        case 403:
          message.error('没有权限访问该资源')
          break
        case 404:
          message.error('请求的资源不存在')
          break
        case 500:
          message.error('服务器错误')
          break
        default:
          message.error(data.message || `请求失败: ${status}`)
      }
    } else {
      message.error('网络异常，请检查您的网络连接')
    }
    
    return Promise.reject(error)
  }
)

export default request 