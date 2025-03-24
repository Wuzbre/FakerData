<template>
  <a-layout class="app-layout">
    <a-layout-sider v-model="collapsed" :trigger="null" collapsible>
      <div class="logo">
        <img src="@/assets/logo.png" alt="Logo">
        <h1 v-if="!collapsed">销售数据仓库</h1>
      </div>
      <a-menu
        theme="dark"
        mode="inline"
        :selectedKeys="[activeMenu]"
        :openKeys="openKeys"
        @openChange="onOpenChange"
      >
        <a-menu-item key="/dashboard">
          <router-link to="/dashboard">
            <a-icon type="dashboard" />
            <span>仪表盘</span>
          </router-link>
        </a-menu-item>
        
        <a-sub-menu key="sales">
          <span slot="title">
            <a-icon type="line-chart" />
            <span>销售分析</span>
          </span>
          <a-menu-item key="/sales/trend">
            <router-link to="/sales/trend">
              <span>销售趋势</span>
            </router-link>
          </a-menu-item>
          <a-menu-item key="/sales/products">
            <router-link to="/sales/products">
              <span>产品销售</span>
            </router-link>
          </a-menu-item>
          <a-menu-item key="/sales/region">
            <router-link to="/sales/region">
              <span>地域销售</span>
            </router-link>
          </a-menu-item>
        </a-sub-menu>
        
        <a-sub-menu key="customer">
          <span slot="title">
            <a-icon type="user" />
            <span>客户分析</span>
          </span>
          <a-menu-item key="/customer/rfm">
            <router-link to="/customer/rfm">
              <span>RFM分析</span>
            </router-link>
          </a-menu-item>
        </a-sub-menu>
        
        <a-sub-menu key="purchase">
          <span slot="title">
            <a-icon type="shopping-cart" />
            <span>采购分析</span>
          </span>
          <a-menu-item key="/purchase/supplier">
            <router-link to="/purchase/supplier">
              <span>供应商分析</span>
            </router-link>
          </a-menu-item>
          <a-menu-item key="/purchase/cost">
            <router-link to="/purchase/cost">
              <span>采购成本</span>
            </router-link>
          </a-menu-item>
        </a-sub-menu>
        
        <a-sub-menu key="inventory">
          <span slot="title">
            <a-icon type="database" />
            <span>库存分析</span>
          </span>
          <a-menu-item key="/inventory/turnover">
            <router-link to="/inventory/turnover">
              <span>库存周转</span>
            </router-link>
          </a-menu-item>
        </a-sub-menu>
      </a-menu>
    </a-layout-sider>
    
    <a-layout>
      <a-layout-header style="background: #fff; padding: 0">
        <a-icon
          class="trigger"
          :type="collapsed ? 'menu-unfold' : 'menu-fold'"
          @click="collapsed = !collapsed"
        />
        
        <div class="header-right">
          <a-dropdown>
            <a class="user-dropdown" @click="e => e.preventDefault()">
              <a-avatar icon="user" /> 
              <span class="username">管理员</span>
              <a-icon type="down" />
            </a>
            <a-menu slot="overlay">
              <a-menu-item>
                <a-icon type="user" />个人中心
              </a-menu-item>
              <a-menu-item>
                <a-icon type="setting" />系统设置
              </a-menu-item>
              <a-menu-divider />
              <a-menu-item @click="handleLogout">
                <a-icon type="logout" />退出登录
              </a-menu-item>
            </a-menu>
          </a-dropdown>
        </div>
      </a-layout-header>
      
      <a-layout-content class="layout-content">
        <router-view />
      </a-layout-content>
      
      <a-layout-footer style="text-align: center">
        销售数据仓库系统 &copy; {{ new Date().getFullYear() }}
      </a-layout-footer>
    </a-layout>
  </a-layout>
</template>

<script>
export default {
  name: 'Layout',
  data() {
    return {
      collapsed: false,
      openKeys: [],
      rootSubmenuKeys: ['sales', 'customer', 'purchase', 'inventory']
    }
  },
  computed: {
    activeMenu() {
      return this.$route.path
    }
  },
  watch: {
    $route: {
      handler(route) {
        // 根据当前路由自动展开对应的菜单
        const matched = route.path.match(/^\/([^/]+)/)
        if (matched) {
          this.openKeys = [matched[1]]
        }
      },
      immediate: true
    }
  },
  methods: {
    onOpenChange(openKeys) {
      const latestOpenKey = openKeys.find(key => this.openKeys.indexOf(key) === -1)
      if (this.rootSubmenuKeys.indexOf(latestOpenKey) === -1) {
        this.openKeys = openKeys
      } else {
        this.openKeys = latestOpenKey ? [latestOpenKey] : []
      }
    },
    handleLogout() {
      localStorage.removeItem('token')
      this.$router.push('/login')
    }
  }
}
</script>

<style scoped>
.app-layout {
  min-height: 100vh;
}

.logo {
  height: 64px;
  line-height: 64px;
  padding-left: 18px;
  background: #002140;
  overflow: hidden;
  display: flex;
  align-items: center;
}

.logo img {
  width: 32px;
  height: 32px;
  margin-right: 12px;
}

.logo h1 {
  color: white;
  font-size: 18px;
  margin: 0;
  font-weight: 600;
}

.trigger {
  font-size: 18px;
  line-height: 64px;
  padding: 0 24px;
  cursor: pointer;
  transition: color 0.3s;
}

.trigger:hover {
  color: #1890ff;
}

.header-right {
  float: right;
  margin-right: 24px;
}

.user-dropdown {
  display: inline-block;
  height: 64px;
  padding: 0 12px;
  cursor: pointer;
  transition: all 0.3s;
}

.user-dropdown:hover {
  background: rgba(0, 0, 0, 0.025);
}

.username {
  margin: 0 8px;
}

.layout-content {
  margin: 24px 16px;
  padding: 24px;
  background: #fff;
  min-height: 280px;
  overflow: auto;
}
</style> 