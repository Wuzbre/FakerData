<template>
  <div class="dws-container">
    <a-tabs
      v-model:activeKey="activeKey"
      type="card"
      @change="handleTabChange"
    >
      <a-tab-pane key="list" tab="表管理">
        <router-view v-if="activeKey === 'list'"></router-view>
      </a-tab-pane>
      
      <a-tab-pane key="quality" tab="数据质量">
        <router-view v-if="activeKey === 'quality'></router-view>
      </a-tab-pane>
      
      <a-tab-pane key="tasks" tab="任务管理">
        <router-view v-if="activeKey === 'tasks'></router-view>
      </a-tab-pane>
    </a-tabs>
  </div>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue'
import { useRouter, useRoute } from 'vue-router'

const router = useRouter()
const route = useRoute()

const activeKey = ref('list')

// 根据路由路径设置当前激活的标签页
watch(
  () => route.path,
  (path) => {
    if (path.includes('/quality')) {
      activeKey.value = 'quality'
    } else if (path.includes('/tasks')) {
      activeKey.value = 'tasks'
    } else {
      activeKey.value = 'list'
    }
  },
  { immediate: true }
)

// 标签页切换时更新路由
const handleTabChange = (key: string) => {
  switch (key) {
    case 'quality':
      router.push('/dws/quality')
      break
    case 'tasks':
      router.push('/dws/tasks')
      break
    default:
      router.push('/dws')
  }
}
</script>

<style scoped>
.dws-container {
  background: #fff;
  padding: 24px;
  border-radius: 2px;
}

:deep(.ant-tabs-nav) {
  margin-bottom: 24px;
}
</style> 