<template>
  <div class="home">
    <a-row :gutter="24">
      <a-col :span="6">
        <a-card>
          <template #title>
            <TeamOutlined />
            <span>活跃用户数</span>
          </template>
          <div class="card-content">
            <h3>{{ stats.activeUsers }}</h3>
            <p>较昨日 {{ stats.userGrowth > 0 ? '+' : '' }}{{ stats.userGrowth }}%</p>
          </div>
        </a-card>
      </a-col>
      
      <a-col :span="6">
        <a-card>
          <template #title>
            <ShoppingOutlined />
            <span>订单总数</span>
          </template>
          <div class="card-content">
            <h3>{{ stats.totalOrders }}</h3>
            <p>较昨日 {{ stats.orderGrowth > 0 ? '+' : '' }}{{ stats.orderGrowth }}%</p>
          </div>
        </a-card>
      </a-col>
      
      <a-col :span="6">
        <a-card>
          <template #title>
            <DollarOutlined />
            <span>销售额</span>
          </template>
          <div class="card-content">
            <h3>¥{{ formatNumber(stats.totalSales) }}</h3>
            <p>较昨日 {{ stats.salesGrowth > 0 ? '+' : '' }}{{ stats.salesGrowth }}%</p>
          </div>
        </a-card>
      </a-col>
      
      <a-col :span="6">
        <a-card>
          <template #title>
            <InboxOutlined />
            <span>库存商品数</span>
          </template>
          <div class="card-content">
            <h3>{{ stats.totalProducts }}</h3>
            <p>较昨日 {{ stats.productGrowth > 0 ? '+' : '' }}{{ stats.productGrowth }}%</p>
          </div>
        </a-card>
      </a-col>
    </a-row>
    
    <a-row :gutter="24" class="charts-row">
      <a-col :span="12">
        <a-card title="销售趋势">
          <div ref="salesChart" class="chart"></div>
        </a-card>
      </a-col>
      
      <a-col :span="12">
        <a-card title="用户增长">
          <div ref="userChart" class="chart"></div>
        </a-card>
      </a-col>
    </a-row>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { TeamOutlined, ShoppingOutlined, DollarOutlined, InboxOutlined } from '@ant-design/icons-vue'
import * as echarts from 'echarts'

const stats = ref({
  activeUsers: 1234,
  userGrowth: 5.2,
  totalOrders: 856,
  orderGrowth: 3.8,
  totalSales: 128500,
  salesGrowth: 7.6,
  totalProducts: 324,
  productGrowth: -2.1
})

const salesChart = ref()
const userChart = ref()

const formatNumber = (num: number) => {
  return num.toLocaleString('zh-CN')
}

const initSalesChart = () => {
  const chart = echarts.init(salesChart.value)
  chart.setOption({
    tooltip: {
      trigger: 'axis'
    },
    xAxis: {
      type: 'category',
      data: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    },
    yAxis: {
      type: 'value'
    },
    series: [{
      data: [820, 932, 901, 934, 1290, 1330, 1320],
      type: 'line',
      smooth: true
    }]
  })
}

const initUserChart = () => {
  const chart = echarts.init(userChart.value)
  chart.setOption({
    tooltip: {
      trigger: 'axis'
    },
    xAxis: {
      type: 'category',
      data: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    },
    yAxis: {
      type: 'value'
    },
    series: [{
      data: [120, 132, 101, 134, 90, 230, 210],
      type: 'bar'
    }]
  })
}

onMounted(() => {
  initSalesChart()
  initUserChart()
  
  window.addEventListener('resize', () => {
    const salesChartInstance = echarts.getInstanceByDom(salesChart.value)
    const userChartInstance = echarts.getInstanceByDom(userChart.value)
    salesChartInstance?.resize()
    userChartInstance?.resize()
  })
})
</script>

<style scoped>
.home {
  .charts-row {
    margin-top: 24px;
  }
  
  .card-content {
    text-align: center;
    
    h3 {
      font-size: 24px;
      margin: 0;
    }
    
    p {
      margin: 8px 0 0;
      color: rgba(0, 0, 0, 0.45);
    }
  }
  
  .chart {
    height: 300px;
  }
}
</style> 