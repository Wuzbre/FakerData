<template>
  <div class="dws-tasks">
    <div class="tasks-header">
      <a-space>
        <a-input-search
          v-model:value="searchText"
          placeholder="请输入任务名称搜索"
          style="width: 300px"
          @search="onSearch"
        />
        <a-select
          v-model:value="status"
          style="width: 120px"
          placeholder="任务状态"
          @change="onStatusChange"
        >
          <a-select-option value="">全部状态</a-select-option>
          <a-select-option value="running">运行中</a-select-option>
          <a-select-option value="success">成功</a-select-option>
          <a-select-option value="failed">失败</a-select-option>
          <a-select-option value="pending">等待中</a-select-option>
        </a-select>
        <a-button type="primary" @click="handleCreate">
          <template #icon><PlusOutlined /></template>
          新建任务
        </a-button>
      </a-space>
    </div>

    <a-table
      :columns="columns"
      :data-source="taskData"
      :loading="loading"
      :pagination="pagination"
      @change="handleTableChange"
      row-key="id"
    >
      <template #bodyCell="{ column, record }">
        <template v-if="column.key === 'status'">
          <a-tag :color="getStatusColor(record.status)">
            {{ getStatusText(record.status) }}
          </a-tag>
        </template>
        <template v-else-if="column.key === 'action'">
          <a-space>
            <a-button
              type="link"
              :disabled="!canRerun(record as TaskItem)"
              @click="handleRerun(record as TaskItem)"
            >
              <template #icon><RedoOutlined /></template>
              重跑
            </a-button>
            <a-button
              type="link"
              :disabled="!canStop(record as TaskItem)"
              @click="handleStop(record as TaskItem)"
            >
              <template #icon><StopOutlined /></template>
              停止
            </a-button>
            <a-button type="link" @click="handleViewLog(record as TaskItem)">
              <template #icon><FileTextOutlined /></template>
              日志
            </a-button>
          </a-space>
        </template>
      </template>
    </a-table>

    <a-modal
      v-model:visible="logVisible"
      title="任务日志"
      width="800px"
      :footer="null"
      :mask-closable="false"
      :keyboard="false"
      @cancel="handleLogCancel"
    >
      <a-spin :spinning="logLoading">
        <pre class="task-log">{{ taskLog }}</pre>
      </a-spin>
    </a-modal>

    <a-modal
      v-model:visible="createVisible"
      title="新建任务"
      :confirm-loading="createLoading"
      :mask-closable="false"
      :keyboard="false"
      @ok="handleCreateSubmit"
      @cancel="handleCreateCancel"
    >
      <a-form
        ref="createFormRef"
        :model="createForm"
        :rules="rules"
        :label-col="{ span: 6 }"
        :wrapper-col="{ span: 16 }"
      >
        <a-form-item label="任务名称" name="name">
          <a-input
            v-model:value="createForm.name"
            placeholder="请输入任务名称"
            :maxLength="50"
            show-count
            @pressEnter="handleCreateSubmit"
          />
        </a-form-item>
        <a-form-item label="目标表" name="targetTable">
          <a-select
            v-model:value="createForm.targetTable"
            placeholder="请选择目标表"
            show-search
            :filter-option="filterOption"
          >
            <a-select-option v-for="table in tables" :key="table" :value="table">
              {{ table }}
            </a-select-option>
          </a-select>
        </a-form-item>
        <a-form-item label="调度周期" name="schedule">
          <a-select
            v-model:value="createForm.schedule"
            placeholder="请选择调度周期"
          >
            <a-select-option value="daily">每日</a-select-option>
            <a-select-option value="hourly">每小时</a-select-option>
          </a-select>
        </a-form-item>
        <a-form-item label="优先级" name="priority">
          <a-select
            v-model:value="createForm.priority"
            placeholder="请选择优先级"
          >
            <a-select-option value="high">高</a-select-option>
            <a-select-option value="medium">中</a-select-option>
            <a-select-option value="low">低</a-select-option>
          </a-select>
        </a-form-item>
      </a-form>
    </a-modal>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { message, Modal } from 'ant-design-vue'
import {
  PlusOutlined,
  RedoOutlined,
  StopOutlined,
  FileTextOutlined
} from '@ant-design/icons-vue'
import type { TablePaginationConfig } from 'ant-design-vue'
import type { FormInstance } from 'ant-design-vue'
import {
  type TaskItem,
  type TaskListParams,
  type CreateTaskParams,
  getTaskList,
  createTask,
  rerunTask,
  stopTask,
  getTaskLog
} from '@/api/dws'
import type {
  TaskStatus,
  ScheduleType,
  PriorityType,
  FormRules,
  DwsColumn
} from '@/types/dws'

const searchText = ref('')
const status = ref<TaskStatus | ''>('')
const loading = ref(false)
const logVisible = ref(false)
const logLoading = ref(false)
const taskLog = ref('')
const createVisible = ref(false)
const createLoading = ref(false)
const createForm = ref<CreateTaskParams>({
  name: '',
  targetTable: '',
  schedule: 'daily',
  priority: 'medium'
})
const createFormRef = ref<FormInstance>()

const tables = ref(['dws_user_behavior_d', 'dws_product_sales_d'])
const taskData = ref<TaskItem[]>([])

const rules: FormRules = {
  name: [
    { required: true, message: '请输入任务名称', trigger: 'blur' },
    { min: 2, max: 50, message: '任务名称长度必须在 2-50 个字符之间', trigger: 'blur' }
  ],
  targetTable: [
    { required: true, message: '请选择目标表', trigger: 'change' }
  ],
  schedule: [
    { required: true, message: '请选择调度周期', trigger: 'change' }
  ],
  priority: [
    { required: true, message: '请选择优先级', trigger: 'change' }
  ]
}

const columns: DwsColumn<TaskItem>[] = [
  {
    title: '任务名称',
    dataIndex: 'name',
    key: 'name',
    width: '20%'
  },
  {
    title: '目标表',
    dataIndex: 'targetTable',
    key: 'targetTable',
    width: '15%'
  },
  {
    title: '状态',
    dataIndex: 'status',
    key: 'status',
    width: '10%'
  },
  {
    title: '开始时间',
    dataIndex: 'startTime',
    key: 'startTime',
    width: '15%'
  },
  {
    title: '结束时间',
    dataIndex: 'endTime',
    key: 'endTime',
    width: '15%'
  },
  {
    title: '耗时',
    dataIndex: 'duration',
    key: 'duration',
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

const getStatusColor = (status: TaskStatus) => {
  switch (status) {
    case 'running':
      return 'processing'
    case 'success':
      return 'success'
    case 'failed':
      return 'error'
    case 'pending':
      return 'warning'
    default:
      return 'default'
  }
}

const getStatusText = (status: TaskStatus) => {
  switch (status) {
    case 'running':
      return '运行中'
    case 'success':
      return '成功'
    case 'failed':
      return '失败'
    case 'pending':
      return '等待中'
    default:
      return '未知'
  }
}

const canRerun = (record: TaskItem) => {
  return ['success', 'failed'].includes(record.status)
}

const canStop = (record: TaskItem) => {
  return ['running', 'pending'].includes(record.status)
}

const filterOption = (input: string, option: any) => {
  return option.value.toLowerCase().indexOf(input.toLowerCase()) >= 0
}

const fetchTasks = async () => {
  loading.value = true
  try {
    const params: TaskListParams = {
      keyword: searchText.value,
      status: status.value,
      pageSize: pagination.value.pageSize!,
      current: pagination.value.current!
    }
    const { total, list } = await getTaskList(params)
    taskData.value = list
    pagination.value.total = total
  } catch (error: any) {
    message.error('获取任务列表失败')
    console.error('获取任务列表失败:', error)
  } finally {
    loading.value = false
  }
}

const onSearch = () => {
  pagination.value.current = 1
  fetchTasks()
}

const onStatusChange = () => {
  pagination.value.current = 1
  fetchTasks()
}

const handleTableChange = (pag: TablePaginationConfig) => {
  pagination.value = pag
  fetchTasks()
}

const handleCreate = () => {
  createVisible.value = true
}

const handleCreateCancel = () => {
  Modal.confirm({
    title: '确认取消',
    content: '确定要取消创建任务吗？已填写的内容将会丢失。',
    onOk: () => {
      createVisible.value = false
      createForm.value = {
        name: '',
        targetTable: '',
        schedule: 'daily',
        priority: 'medium'
      }
      createFormRef.value?.resetFields()
    }
  })
}

const handleCreateSubmit = async () => {
  try {
    await createFormRef.value?.validate()
    createLoading.value = true
    await createTask(createForm.value)
    message.success('创建任务成功')
    createVisible.value = false
    createForm.value = {
      name: '',
      targetTable: '',
      schedule: 'daily',
      priority: 'medium'
    }
    createFormRef.value?.resetFields()
    fetchTasks()
  } catch (error: any) {
    if (error.isAxiosError) {
      message.error('创建任务失败：' + (error.response?.data?.message || '网络错误'))
    }
    console.error('创建任务失败:', error)
  } finally {
    createLoading.value = false
  }
}

const handleRerun = async (record: TaskItem) => {
  try {
    Modal.confirm({
      title: '确认重跑',
      content: `确定要重跑任务"${record.name}"吗？`,
      onOk: async () => {
        await rerunTask(record.id)
        message.success('重跑任务成功')
        fetchTasks()
      }
    })
  } catch (error: any) {
    message.error('重跑任务失败：' + (error.response?.data?.message || '网络错误'))
    console.error('重跑任务失败:', error)
  }
}

const handleStop = async (record: TaskItem) => {
  try {
    Modal.confirm({
      title: '确认停止',
      content: `确定要停止任务"${record.name}"吗？`,
      onOk: async () => {
        await stopTask(record.id)
        message.success('停止任务成功')
        fetchTasks()
      }
    })
  } catch (error: any) {
    message.error('停止任务失败：' + (error.response?.data?.message || '网络错误'))
    console.error('停止任务失败:', error)
  }
}

const handleLogCancel = () => {
  if (logLoading.value) {
    return
  }
  logVisible.value = false
  taskLog.value = ''
}

const handleViewLog = async (record: TaskItem) => {
  logVisible.value = true
  logLoading.value = true
  try {
    const log = await getTaskLog(record.id)
    taskLog.value = log || '暂无日志'
  } catch (error: any) {
    message.error('获取任务日志失败：' + (error.response?.data?.message || '网络错误'))
    console.error('获取任务日志失败:', error)
    taskLog.value = '获取日志失败'
  } finally {
    logLoading.value = false
  }
}

onMounted(() => {
  fetchTasks()
})
</script>

<style scoped>
.dws-tasks {
  .tasks-header {
    margin-bottom: 16px;
  }
  
  .task-log {
    max-height: 500px;
    overflow-y: auto;
    padding: 16px;
    background: #f5f5f5;
    border-radius: 2px;
    font-family: monospace;
    white-space: pre-wrap;
    word-wrap: break-word;
  }
}
</style>