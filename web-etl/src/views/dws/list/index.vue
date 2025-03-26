<template>
  <div class="dws-list">
    <div class="list-header">
      <a-space>
        <a-input-search
          v-model:value="searchText"
          placeholder="请输入表名搜索"
          style="width: 300px"
          @search="onSearch"
        />
        <a-button type="primary" @click="handleRefresh">
          <template #icon><ReloadOutlined /></template>
          刷新
        </a-button>
      </a-space>
    </div>

    <a-table
      :columns="columns"
      :data-source="tableData"
      :loading="loading"
      :pagination="pagination"
      @change="handleTableChange"
    >
      <template #bodyCell="{ column, record }">
        <template v-if="column.key === 'status'">
          <a-tag :color="getStatusColor(record.status)">
            {{ record.status }}
          </a-tag>
        </template>
        <template v-else-if="column.key === 'action'">
          <a-space>
            <a-button type="link" @click="handlePreview(record)">
              <template #icon><EyeOutlined /></template>
              预览
            </a-button>
            <a-button type="link" @click="handleQuality(record)">
              <template #icon><BarChartOutlined /></template>
              质量报告
            </a-button>
          </a-space>
        </template>
      </template>
    </a-table>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { message } from 'ant-design-vue'
import { ReloadOutlined, EyeOutlined, BarChartOutlined } from '@ant-design/icons-vue'
import type { TablePaginationConfig } from 'ant-design-vue'
import {
  type TableItem,
  type TableListParams,
  getTableList
} from '@/api/dws'

const router = useRouter()
const searchText = ref('')
const loading = ref(false)
const tableData = ref<TableItem[]>([])

const columns = [
  {
    title: '表名',
    dataIndex: 'tableName',
    key: 'tableName',
    width: '25%'
  },
  {
    title: '描述',
    dataIndex: 'description',
    key: 'description',
    width: '30%'
  },
  {
    title: '总行数',
    dataIndex: 'totalRows',
    key: 'totalRows',
    width: '15%'
  },
  {
    title: '最后更新时间',
    dataIndex: 'lastUpdateTime',
    key: 'lastUpdateTime',
    width: '15%'
  },
  {
    title: '状态',
    dataIndex: 'status',
    key: 'status',
    width: '10%'
  },
  {
    title: '操作',
    key: 'action',
    width: '15%'
  }
]

const pagination = ref<TablePaginationConfig>({
  total: 0,
  current: 1,
  pageSize: 10,
  showSizeChanger: true,
  showQuickJumper: true
})

const getStatusColor = (status: string) => {
  switch (status) {
    case 'normal':
      return 'success'
    case 'warning':
      return 'warning'
    case 'error':
      return 'error'
    default:
      return 'default'
  }
}

const fetchTableList = async () => {
  loading.value = true
  try {
    const params: TableListParams = {
      keyword: searchText.value,
      pageSize: pagination.value.pageSize!,
      current: pagination.value.current!
    }
    const { total, list } = await getTableList(params)
    tableData.value = list
    pagination.value.total = total
  } catch (error: any) {
    console.error('获取表列表失败:', error)
  } finally {
    loading.value = false
  }
}

const onSearch = () => {
  pagination.value.current = 1
  fetchTableList()
}

const handleRefresh = () => {
  fetchTableList()
}

const handleTableChange = (pag: TablePaginationConfig) => {
  pagination.value = pag
  fetchTableList()
}

const handlePreview = (record: TableItem) => {
  router.push(`/dws/preview/${record.tableName}`)
}

const handleQuality = (record: TableItem) => {
  router.push(`/dws/quality/${record.tableName}`)
}

onMounted(() => {
  fetchTableList()
})
</script>

<style scoped>
.dws-list {
  .list-header {
    margin-bottom: 16px;
  }
}
</style> 