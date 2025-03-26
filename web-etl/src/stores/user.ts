import { defineStore } from 'pinia'
import { login as loginApi, type LoginParams, type LoginResult } from '@/api/auth'

interface UserState {
  userId: number | null
  username: string
  nickname: string
  token: string
  roles: string[]
  permissions: string[]
}

export const useUserStore = defineStore('user', {
  state: (): UserState => ({
    userId: null,
    username: '',
    nickname: '',
    token: '',
    roles: [],
    permissions: []
  }),
  
  getters: {
    isLogin: (state) => !!state.token,
    hasRole: (state) => (role: string) => state.roles.includes(role),
    hasPermission: (state) => (permission: string) => state.permissions.includes(permission)
  },
  
  actions: {
    async login(loginParams: LoginParams) {
      try {
        const result = await loginApi(loginParams)
        this.setUserInfo(result)
        return result
      } catch (error) {
        return Promise.reject(error)
      }
    },
    
    setUserInfo(userInfo: LoginResult) {
      this.userId = userInfo.userId
      this.username = userInfo.username
      this.nickname = userInfo.nickname
      this.token = userInfo.token
      this.roles = userInfo.roles
      this.permissions = userInfo.permissions
    },
    
    logout() {
      this.userId = null
      this.username = ''
      this.nickname = ''
      this.token = ''
      this.roles = []
      this.permissions = []
    }
  },
  
  persist: {
    enabled: true,
    strategies: [
      {
        storage: localStorage,
        paths: ['token']
      }
    ]
  }
}) 