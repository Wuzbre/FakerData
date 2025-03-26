<template>
  <div class="dws-quality">
    <a-page-header :title="`${tableName} - 数据质量报告`" @back="handleBack">
      <template #extra>
        <a-space>
          <a-range-picker
            v-model:value="dateRange"
            :disabled-date="disabledDate"
            @change="handleDateChange"
          />
          <a-button @click="handleRefresh">
            <template #icon><ReloadOutlined /></template>
            刷新
          </a-button>
        </a-space>
      </template>
    </a-page-header>

    <a-row :gutter="16" class="stats-row">
      <a-col :span="6">
        <a-card>
          <statistic
            title="数据量"
            :value="qualityData.stats.totalRows"
            :precision="0"
          >
            <template #suffix>
              <trend
                :value="qualityData.stats.rowGrowth"
                type="value"
                style="margin-left: 8px"
              />
            </template>
          </statistic>
        </a-card>
      </a-col>
      <a-col :span="6">
        <a-card>
          <statistic
            title="完整性"
            :value="qualityData.stats.completeness"
            :precision="2"
            suffix="%"
          >
            <template #suffix>
              <trend
                :value="qualityData.stats.completenessGrowth"
                style="margin-left: 8px"
              />
            </template>
          </statistic>
        </a-card>
      </a-col>
      <a-col :span="6">
        <a-card>
          <statistic
            title="准确性"
            :value="qualityData.stats.accuracy"
            :precision="2"
            suffix="%"
          >
            <template #suffix>
              <trend
                :value="qualityData.stats.accuracyGrowth"
                style="margin-left: 8px"
              />
            </template>
          </statistic>
        </a-card>
      </a-col>
      <a-col :span="6">
        <a-card>
          <statistic
            title="及时性"
            :value="qualityData.stats.timeliness"
            :precision="2"
            suffix="%"
          >
            <template #suffix>
              <trend
                :value="qualityData.stats.timelinessGrowth"
                style="margin-left: 8px"
              />
            </template>
          </statistic>
        </a-card>
      </a-col>
    </a-row>

    <a-row :gutter="16" class="chart-row">
      <a-col :span="12">
        <a-card title="数据量趋势">
          <div ref="volumeChartRef" class="chart" />
        </a-card>
      </a-col>
      <a-col :span="12">
        <a-card title="质量指标趋势">
          <div ref="qualityChartRef" class="chart" />
        </a-card>
      </a-col>
    </a-row>

    <a-card title="字段质量详情" class="field-quality">
      <a-table
        :columns="columns"
        :data-source="qualityData.fieldQualities"
        :pagination="false"
      >
        <template #bodyCell="{ column, record }">
          <template v-if="column.key === 'status'">
            <a-tag :color="getStatusColor(record.status)">
              {{ record.status }}
            </a-tag>
          </template>
          <template v-else-if="column.key === 'issues'">
            <a-space>
              <a-tag v-for="issue in record.issues" :key="issue" color="red">
                {{ issue }}
              </a-tag>
            </a-space>
          </template>
        </template>
      </a-table>
    </a-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { message } from 'ant-design-vue'
import { ReloadOutlined } from '@ant-design/icons-vue'
import * as echarts from 'echarts'
import dayjs from 'dayjs'
import type { Dayjs } from 'dayjs'
import {
  type QualityParams,
  type QualityResult,
  getQualityData
} from '@/api/dws'

const route = useRoute()
const router = useRouter()
const tableName = route.params.tableName as string

const dateRange = ref<[Dayjs, Dayjs]>([
  dayjs().subtract(30, 'days'),
  dayjs()
])

const qualityData = ref<QualityResult>({
  stats: {
    totalRows: 0,
    rowGrowth: 0,
    completeness: 0,
    completenessGrowth: 0,
    accuracy: 0,
    accuracyGrowth: 0,
    timeliness: 0,
    timelinessGrowth: 0
  },
  trends: [],
  fieldQualities: []
})

const columns = [
  {
    title: '字段名',
    dataIndex: 'fieldName',
    key: 'fieldName',
    width: '20%'
  },
  {
    title: '空值率',
    dataIndex: 'nullRate',
    key: 'nullRate',
    width: '15%',
    customRender: ({ text }: { text: number }) => `${(text * 100).toFixed(2)}%`
  },
  {
    title: '唯一值率',
    dataIndex: 'uniqueRate',
    key: 'uniqueRate',
    width: '15%',
    customRender: ({ text }: { text: number }) => `${(text * 100).toFixed(2)}%`
  },
  {
    title: '状态',
    dataIndex: 'status',
    key: 'status',
    width: '15%'
  },
  {
    title: '问题',
    dataIndex: 'issues',
    key: 'issues',
    width: '35%'
  }
]

const volumeChartRef = ref<HTMLElement>()
const qualityChartRef = ref<HTMLElement>()
let volumeChart: echarts.ECharts | null = null
let qualityChart: echarts.ECharts | null = null

const getStatusColor = (status: string) => {
  switch (status) {
    case 'good':
      return 'success'
    case 'warning':
      return 'warning'
    case 'error':
      return 'error'
    default:
      return 'default'
  }
}

const disabledDate = (current: Dayjs) => {
  return current && current > dayjs().endOf('day')
}

const fetchQualityData = async () => {
  try {
    const params: QualityParams = {
      tableName,
      startDate: dateRange.value[0].format('YYYY-MM-DD'),
      endDate: dateRange.value[1].format('YYYY-MM-DD')
    }
    const data = await getQualityData(params)
    qualityData.value = data
    updateCharts()
  } catch (error: any) {
    message.error('获取质量数据失败')
    console.error('获取质量数据失败:', error)
  }
}

const updateCharts = () => {
  const { trends } = qualityData.value
  const dates = trends.map(item => item.date)
  const volumes = trends.map(item => item.totalRows)
  const completeness = trends.map(item => item.completeness)
  const accuracy = trends.map(item => item.accuracy)
  const timeliness = trends.map(item => item.timeliness)

  // 数据量趋势图
  if (volumeChart) {
    volumeChart.setOption({
      tooltip: {
        trigger: 'axis'
      },
      xAxis: {
        type: 'category',
        data: dates
      },
      yAxis: {
        type: 'value',
        name: '数据量'
      },
      series: [
        {
          name: '数据量',
          type: 'line',
          data: volumes,
          smooth: true,
          areaStyle: {}
        }
      ]
    })
  }

  // 质量指标趋势图
  if (qualityChart) {
    qualityChart.setOption({
      tooltip: {
        trigger: 'axis'
      },
      legend: {
        data: ['完整性', '准确性', '及时性']
      },
      xAxis: {
        type: 'category',
        data: dates
      },
      yAxis: {
        type: 'value',
        name: '百分比',
        min: 0,
        max: 100
      },
      series: [
        {
          name: '完整性',
          type: 'line',
          data: completeness,
          smooth: true
        },
        {
          name: '准确性',
          type: 'line',
          data: accuracy,
          smooth: true
        },
        {
          name: '及时性',
          type: 'line',
          data: timeliness,
          smooth: true
        }
      ]
    })
  }
}

const handleBack = () => {
  router.back()
}

const handleRefresh = () => {
  fetchQualityData()
}

const handleDateChange = () => {
  fetchQualityData()
}

const initCharts = () => {
  if (volumeChartRef.value) {
    volumeChart = echarts.init(volumeChartRef.value)
  }
  if (qualityChartRef.value) {
    qualityChart = echarts.init(qualityChartRef.value)
  }
  updateCharts()
}

watch(
  () => route.params.tableName,
  (newTableName) => {
    if (newTableName) {
      fetchQualityData()
    }
  }
)

onMounted(() => {
  initCharts()
  fetchQualityData()
})
</script>

<style scoped>
.dws-quality {
  .stats-row {
    margin: 16px 0;
  }

  .chart-row {
    margin-bottom: 16px;
  }

  .chart {
    height: 300px;
  }

  .field-quality {
    margin-bottom: 16px;
  }
}
</style>