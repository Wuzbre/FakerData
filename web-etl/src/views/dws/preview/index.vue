<template>
  <div class="dws-preview">
    <div class="preview-header">
      <a-page-header
        :title="tableName"
        :sub-title="tableInfo.description"
        @back="handleBack"
      >
        <template #extra>
          <a-space>
            <a-button @click="handleRefresh">
              <template #icon><ReloadOutlined /></template>
              刷新
            </a-button>
            <a-button type="primary" @click="handleExport">
              <template #icon><DownloadOutlined /></template>
              导出
            </a-button>
          </a-space>
        </template>
      </a-page-header>
    </div>

    <a-card title="表结构" class="preview-card">
      <a-table
        :columns="schemaColumns"
        :data-source="tableInfo.schema"
        :pagination="false"
        size="small"
      >
        <template #bodyCell="{ column, record }">
          <template v-if="column.key === 'isPrimaryKey'">
            <a-tag v-if="record.isPrimaryKey" color="blue">是</a-tag>
            <span v-else>否</span>
          </template>
        </template>
      </a-table>
    </a-card>

    <a-card title="数据预览" class="preview-card">
      <template #extra>
        <a-input-search
          v-model:value="searchText"
          placeholder="请输入关键字搜索"
          style="width: 300px"
          @search="onSearch"
        />
      </template>
      <a-table
        :columns="previewColumns"
        :data-source="previewData"
        :loading="loading"
        :pagination="pagination"
        @change="handleTableChange"
      />
    </a-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { message } from 'ant-design-vue'
import { ReloadOutlined, DownloadOutlined } from '@ant-design/icons-vue'
import type { TablePaginationConfig } from 'ant-design-vue'
import {
  type TableInfo,
  type PreviewDataParams,
  type PreviewDataResult,
  getTableInfo,
  getPreviewData,
  exportTableData
} from '@/api/dws'

const route = useRoute()
const router = useRouter()
const tableName = route.params.tableName as string
const searchText = ref('')
const loading = ref(false)

const tableInfo = ref<TableInfo>({
  description: '',
  createTime: '',
  lastUpdateTime: '',
  owner: '',
  schema: []
})

const previewData = ref<Record<string, any>[]>([])
const previewColumns = ref<{ title: string; dataIndex: string }[]>([])

const schemaColumns = [
  {
    title: '字段名',
    dataIndex: 'fieldName',
    key: 'fieldName',
    width: '25%'
  },
  {
    title: '字段类型',
    dataIndex: 'fieldType',
    key: 'fieldType',
    width: '20%'
  },
  {
    title: '描述',
    dataIndex: 'description',
    key: 'description',
    width: '45%'
  },
  {
    title: '主键',
    dataIndex: 'isPrimaryKey',
    key: 'isPrimaryKey',
    width: '10%'
  }
]

const pagination = ref<TablePaginationConfig>({
  total: 0,
  current: 1,
  pageSize: 10,
  showSizeChanger: true,
  showQuickJumper: true
})

const fetchTableInfo = async () => {
  try {
    const info = await getTableInfo(tableName)
    tableInfo.value = info
  } catch (error: any) {
    message.error('获取表信息失败')
    console.error('获取表信息失败:', error)
  }
}

const fetchPreviewData = async () => {
  loading.value = true
  try {
    const params: PreviewDataParams = {
      tableName,
      keyword: searchText.value,
      pageSize: pagination.value.pageSize!,
      current: pagination.value.current!
    }
    const result = await getPreviewData(params)
    previewData.value = result.data
    previewColumns.value = result.columns
    pagination.value.total = result.total
  } catch (error: any) {
    message.error('获取预览数据失败')
    console.error('获取预览数据失败:', error)
  } finally {
    loading.value = false
  }
}

const handleBack = () => {
  router.back()
}

const handleRefresh = () => {
  fetchTableInfo()
  fetchPreviewData()
}

const onSearch = () => {
  pagination.value.current = 1
  fetchPreviewData()
}

const handleTableChange = (pag: TablePaginationConfig) => {
  pagination.value = pag
  fetchPreviewData()
}

const handleExport = async () => {
  try {
    const blob = await exportTableData(tableName)
    const url = window.URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = `${tableName}.csv`
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    window.URL.revokeObjectURL(url)
    message.success('导出成功')
  } catch (error: any) {
    message.error('导出失败')
    console.error('导出失败:', error)
  }
}

onMounted(() => {
  fetchTableInfo()
  fetchPreviewData()
})
</script>

<style scoped>
.dws-preview {
  .preview-header {
    margin-bottom: 24px;
    background: #fff;
    padding: 16px;
  }
  
  .preview-card {
    margin-bottom: 24px;
  }
}
</style> 