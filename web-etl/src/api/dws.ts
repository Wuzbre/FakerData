import request from '@/utils/request'

// 表管理相关接口
export interface TableItem {
  tableName: string
  description: string
  totalRows: number
  lastUpdateTime: string
  status: string
}

export interface TableListParams {
  keyword?: string
  pageSize: number
  current: number
}

export interface TableListResult {
  total: number
  list: TableItem[]
}

export function getTableList(params: TableListParams) {
  return request<TableListResult>({
    url: '/api/dws/tables',
    method: 'get',
    params
  })
}

export interface TableSchema {
  fieldName: string
  fieldType: string
  description: string
  isPrimaryKey: boolean
}

export interface TableInfo {
  description: string
  createTime: string
  lastUpdateTime: string
  owner: string
  schema: TableSchema[]
}

export function getTableInfo(tableName: string) {
  return request<TableInfo>({
    url: `/api/dws/tables/${tableName}/info`,
    method: 'get'
  })
}

export interface PreviewDataParams {
  tableName: string
  keyword?: string
  pageSize: number
  current: number
}

export interface PreviewDataResult {
  total: number
  columns: { title: string; dataIndex: string }[]
  data: Record<string, any>[]
}

export function getPreviewData(params: PreviewDataParams) {
  return request<PreviewDataResult>({
    url: `/api/dws/tables/${params.tableName}/preview`,
    method: 'get',
    params: {
      keyword: params.keyword,
      pageSize: params.pageSize,
      current: params.current
    }
  })
}

// 数据质量相关接口
export interface QualityStats {
  totalRows: number
  rowGrowth: number
  completeness: number
  completenessGrowth: number
  accuracy: number
  accuracyGrowth: number
  timeliness: number
  timelinessGrowth: number
}

export interface QualityTrend {
  date: string
  totalRows: number
  completeness: number
  accuracy: number
  timeliness: number
}

export interface FieldQuality {
  fieldName: string
  nullRate: number
  uniqueRate: number
  status: string
  issues: string[]
}

export interface QualityParams {
  tableName: string
  startDate: string
  endDate: string
}

export interface QualityResult {
  stats: QualityStats
  trends: QualityTrend[]
  fieldQualities: FieldQuality[]
}

export function getQualityData(params: QualityParams) {
  return request<QualityResult>({
    url: '/api/dws/quality',
    method: 'get',
    params
  })
}

// 任务管理相关接口
export interface TaskItem {
  id: number
  name: string
  targetTable: string
  status: string
  schedule: string
  priority: string
  startTime: string
  endTime: string
  duration: string
}

export interface TaskListParams {
  keyword?: string
  status?: string
  pageSize: number
  current: number
}

export interface TaskListResult {
  total: number
  list: TaskItem[]
}

export function getTaskList(params: TaskListParams) {
  return request<TaskListResult>({
    url: '/api/dws/tasks',
    method: 'get',
    params
  })
}

export interface CreateTaskParams {
  name: string
  targetTable: string
  schedule: string
  priority: string
}

export function createTask(data: CreateTaskParams) {
  return request<void>({
    url: '/api/dws/tasks',
    method: 'post',
    data
  })
}

export function rerunTask(taskId: number) {
  return request<void>({
    url: `/api/dws/tasks/${taskId}/rerun`,
    method: 'post'
  })
}

export function stopTask(taskId: number) {
  return request<void>({
    url: `/api/dws/tasks/${taskId}/stop`,
    method: 'post'
  })
}

export function getTaskLog(taskId: number) {
  return request<string>({
    url: `/api/dws/tasks/${taskId}/log`,
    method: 'get'
  })
}

// 数据导出接口
export function exportTableData(tableName: string) {
  return request({
    url: `/api/dws/tables/${tableName}/export`,
    method: 'get',
    responseType: 'blob'
  })
} 