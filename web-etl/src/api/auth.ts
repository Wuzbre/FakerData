import request from '@/utils/request'

export interface LoginParams {
  username: string
  password: string
}

export interface LoginResult {
  userId: number
  username: string
  nickname: string
  token: string
  roles: string[]
  permissions: string[]
}

export function login(data: LoginParams) {
  return request<LoginResult>({
    url: '/auth/login',
    method: 'post',
    data
  })
} 