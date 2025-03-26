import type { Rule } from 'ant-design-vue/es/form'
import type { TableColumnType } from 'ant-design-vue/es/table'

// 表格列类型
export type DwsColumn<T> = TableColumnType<T> & {
  dataIndex: keyof T
}

// 表单规则类型
export type FormRules = Record<string, Rule[]>

// 表单项类型
export type FormItemProps = {
  span: number
}

// 状态类型
export type TableStatus = 'normal' | 'warning' | 'error'
export type TaskStatus = 'running' | 'success' | 'failed' | 'pending'
export type QualityStatus = 'good' | 'warning' | 'error'

// 调度周期类型
export type ScheduleType = 'daily' | 'hourly'

// 优先级类型
export type PriorityType = 'high' | 'medium' | 'low' 