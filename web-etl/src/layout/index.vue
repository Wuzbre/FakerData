<template>
  <a-layout class="layout">
    <a-layout-header class="header">
      <div class="logo">数据中台管理系统</div>
      <div class="header-right">
        <a-dropdown>
          <a class="user-dropdown" @click.prevent>
            <UserOutlined />
            <span class="username">{{ userStore.nickname }}</span>
          </a>
          <template #overlay>
            <a-menu>
              <a-menu-item @click="handleLogout">
                <LogoutOutlined />
                <span>退出登录</span>
              </a-menu-item>
            </a-menu>
          </template>
        </a-dropdown>
      </div>
    </a-layout-header>
    
    <a-layout>
      <a-layout-sider width="200" class="sider">
        <a-menu
          v-model:selectedKeys="selectedKeys"
          v-model:openKeys="openKeys"
          mode="inline"
          theme="dark"
        >
          <a-menu-item key="home">
            <template #icon>
              <HomeOutlined />
            </template>
            <router-link to="/">首页</router-link>
          </a-menu-item>
          
          <a-sub-menu key="data">
            <template #icon>
              <DatabaseOutlined />
            </template>
            <template #title>数据管理</template>
            <a-menu-item key="dws">
              <router-link to="/dws">DWS层</router-link>
            </a-menu-item>
            <a-menu-item key="ads">
              <router-link to="/ads">ADS层</router-link>
            </a-menu-item>
          </a-sub-menu>
        </a-menu>
      </a-layout-sider>
      
      <a-layout-content class="content">
        <router-view></router-view>
      </a-layout-content>
    </a-layout>
  </a-layout>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { useUserStore } from '@/stores/user'
import { UserOutlined, LogoutOutlined, HomeOutlined, DatabaseOutlined } from '@ant-design/icons-vue'
import { Modal } from 'ant-design-vue'

const router = useRouter()
const userStore = useUserStore()

const selectedKeys = ref<string[]>(['home'])
const openKeys = ref<string[]>(['data'])

const handleLogout = () => {
  Modal.confirm({
    title: '确认退出',
    content: '是否确认退出登录？',
    onOk: () => {
      userStore.logout()
      router.push('/login')
    }
  })
}
</script>

<style scoped>
.layout {
  min-height: 100vh;
}

.header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0 24px;
  background: #fff;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
}

.logo {
  font-size: 18px;
  font-weight: bold;
  color: #1890ff;
}

.header-right {
  display: flex;
  align-items: center;
}

.user-dropdown {
  display: flex;
  align-items: center;
  color: rgba(0, 0, 0, 0.85);
  cursor: pointer;
}

.username {
  margin-left: 8px;
}

.sider {
  background: #001529;
}

.content {
  padding: 24px;
  background: #fff;
  margin: 24px;
  min-height: 280px;
}
</style> 