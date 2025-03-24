<template>
  <div class="dashboard">
    <a-row :gutter="24">
      <a-col :span="24">
        <a-card class="dashboard-header" title="销售数据仓库 - 业务概览">
          <a-date-picker
            v-model="queryDate"
            :format="dateFormat"
            @change="handleDateChange"
            placeholder="选择日期"
            style="float: right; margin-top: -5px;"
          />
        </a-card>
      </a-col>
    </a-row>
    
    <a-row :gutter="24" style="margin-top: 24px">
      <a-col :xs="24" :sm="12" :md="12" :lg="6" :xl="6">
        <a-card>
          <template slot="title">
            总销售额
            <a-tooltip title="当前周期内的销售总额" slot="action">
              <a-icon type="info-circle" />
            </a-tooltip>
          </template>
          <div class="card-content">
            <div class="card-statistic">
              <span class="card-value">¥{{ formatNumber(dashboardData.totalSalesAmount || 0) }}</span>
              <span class="card-trend" :class="{'trend-up': dashboardData.salesYoyGrowth > 0, 'trend-down': dashboardData.salesYoyGrowth < 0}">
                <a-icon :type="dashboardData.salesYoyGrowth > 0 ? 'arrow-up' : 'arrow-down'" />
                {{ Math.abs((dashboardData.salesYoyGrowth || 0) * 100).toFixed(2) }}%
              </span>
            </div>
            <div class="card-footer">
              同比
            </div>
          </div>
        </a-card>
      </a-col>
      
      <a-col :xs="24" :sm="12" :md="12" :lg="6" :xl="6">
        <a-card>
          <template slot="title">
            订单数量
            <a-tooltip title="当前周期内的订单总数" slot="action">
              <a-icon type="info-circle" />
            </a-tooltip>
          </template>
          <div class="card-content">
            <div class="card-statistic">
              <span class="card-value">{{ formatNumber(dashboardData.totalSalesOrders || 0) }}</span>
            </div>
            <div class="card-footer">
              平均客单价: ¥{{ formatNumber(dashboardData.avgOrderValue || 0) }}
            </div>
          </div>
        </a-card>
      </a-col>
      
      <a-col :xs="24" :sm="12" :md="12" :lg="6" :xl="6">
        <a-card>
          <template slot="title">
            客户数量
            <a-tooltip title="当前周期内的活跃客户数" slot="action">
              <a-icon type="info-circle" />
            </a-tooltip>
          </template>
          <div class="card-content">
            <div class="card-statistic">
              <span class="card-value">{{ formatNumber(dashboardData.totalCustomers || 0) }}</span>
            </div>
            <div class="card-footer">
              人均消费: ¥{{ formatNumber(dashboardData.totalSalesAmount / dashboardData.totalCustomers || 0) }}
            </div>
          </div>
        </a-card>
      </a-col>
      
      <a-col :xs="24" :sm="12" :md="12" :lg="6" :xl="6">
        <a-card>
          <template slot="title">
            库存量
            <a-tooltip title="当前库存总量" slot="action">
              <a-icon type="info-circle" />
            </a-tooltip>
          </template>
          <div class="card-content">
            <div class="card-statistic">
              <span class="card-value">{{ formatNumber(dashboardData.totalInventory || 0) }}</span>
            </div>
            <div class="card-footer">
              商品种类: {{ formatNumber(dashboardData.totalProducts || 0) }}
            </div>
          </div>
        </a-card>
      </a-col>
    </a-row>
    
    <a-row :gutter="24" style="margin-top: 24px">
      <a-col :span="16">
        <a-card title="销售趋势" :loading="loading">
          <v-chart :option="salesTrendOption" autoresize />
        </a-card>
      </a-col>
      
      <a-col :span="8">
        <a-card title="热销产品" :loading="loading">
          <v-chart :option="topProductsOption" autoresize />
        </a-card>
      </a-col>
    </a-row>
    
    <a-row :gutter="24" style="margin-top: 24px">
      <a-col :span="12">
        <a-card title="销售地域分布" :loading="loading">
          <v-chart :option="regionSalesOption" autoresize />
        </a-card>
      </a-col>
      
      <a-col :span="12">
        <a-card title="采购与销售对比" :loading="loading">
          <v-chart :option="purchaseSalesOption" autoresize />
        </a-card>
      </a-col>
    </a-row>
  </div>
</template>

<script>
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { BarChart, LineChart, PieChart } from 'echarts/charts'
import {
  GridComponent,
  TooltipComponent,
  TitleComponent,
  LegendComponent
} from 'echarts/components'
import VChart from 'vue-echarts'
import { getSalesDashboard } from '@/api/sales'
import moment from 'moment'

use([
  CanvasRenderer,
  BarChart,
  LineChart,
  PieChart,
  GridComponent,
  TooltipComponent,
  TitleComponent,
  LegendComponent
])

export default {
  name: 'Dashboard',
  components: {
    VChart
  },
  data() {
    return {
      loading: false,
      dateFormat: 'YYYY-MM-DD',
      queryDate: moment(),
      dashboardData: {},
      // 销售趋势图表配置
      salesTrendOption: {
        tooltip: {
          trigger: 'axis'
        },
        legend: {
          data: ['销售额', '订单量']
        },
        xAxis: {
          type: 'category',
          data: []
        },
        yAxis: [
          {
            type: 'value',
            name: '销售额',
            axisLabel: {
              formatter: '{value} 元'
            }
          },
          {
            type: 'value',
            name: '订单量',
            axisLabel: {
              formatter: '{value} 件'
            }
          }
        ],
        series: [
          {
            name: '销售额',
            type: 'line',
            data: []
          },
          {
            name: '订单量',
            type: 'bar',
            yAxisIndex: 1,
            data: []
          }
        ]
      },
      // 热销产品图表配置
      topProductsOption: {
        tooltip: {
          trigger: 'item',
          formatter: '{a} <br/>{b}: {c} ({d}%)'
        },
        legend: {
          orient: 'vertical',
          right: 10,
          top: 'center',
          data: []
        },
        series: [
          {
            name: '销售额',
            type: 'pie',
            radius: ['50%', '70%'],
            avoidLabelOverlap: false,
            label: {
              show: false,
              position: 'center'
            },
            emphasis: {
              label: {
                show: true,
                fontSize: '18',
                fontWeight: 'bold'
              }
            },
            labelLine: {
              show: false
            },
            data: []
          }
        ]
      },
      // 销售地域分布图表配置
      regionSalesOption: {
        tooltip: {
          trigger: 'item',
          formatter: '{a} <br/>{b}: {c} ({d}%)'
        },
        legend: {
          orient: 'vertical',
          left: 10,
          top: 'center',
          data: []
        },
        series: [
          {
            name: '销售额',
            type: 'pie',
            radius: '55%',
            center: ['60%', '50%'],
            data: [],
            emphasis: {
              itemStyle: {
                shadowBlur: 10,
                shadowOffsetX: 0,
                shadowColor: 'rgba(0, 0, 0, 0.5)'
              }
            }
          }
        ]
      },
      // 采购与销售对比图表配置
      purchaseSalesOption: {
        tooltip: {
          trigger: 'axis',
          axisPointer: {
            type: 'shadow'
          }
        },
        legend: {
          data: ['采购额', '销售额', '毛利润']
        },
        grid: {
          left: '3%',
          right: '4%',
          bottom: '3%',
          containLabel: true
        },
        xAxis: {
          type: 'value'
        },
        yAxis: {
          type: 'category',
          data: []
        },
        series: [
          {
            name: '采购额',
            type: 'bar',
            stack: 'total',
            label: {
              show: true
            },
            emphasis: {
              focus: 'series'
            },
            data: []
          },
          {
            name: '销售额',
            type: 'bar',
            stack: 'total',
            label: {
              show: true
            },
            emphasis: {
              focus: 'series'
            },
            data: []
          },
          {
            name: '毛利润',
            type: 'bar',
            stack: 'total',
            label: {
              show: true
            },
            emphasis: {
              focus: 'series'
            },
            data: []
          }
        ]
      }
    }
  },
  created() {
    this.fetchDashboardData()
  },
  methods: {
    handleDateChange(date) {
      this.fetchDashboardData()
    },
    fetchDashboardData() {
      this.loading = true
      
      getSalesDashboard({
        date: this.queryDate ? moment(this.queryDate).format(this.dateFormat) : ''
      })
        .then(res => {
          if (res.success) {
            this.dashboardData = res.data
            
            // 更新图表数据
            this.updateChartData()
          }
        })
        .catch(err => {
          console.error('获取仪表盘数据失败', err)
        })
        .finally(() => {
          this.loading = false
        })
    },
    updateChartData() {
      // 这里通常会根据API返回的数据更新各个图表的数据
      // 由于目前我们没有真实的数据，暂时使用模拟数据
      
      // 销售趋势模拟数据
      const dates = ['1月', '2月', '3月', '4月', '5月', '6月']
      const salesData = [120000, 132000, 101000, 134000, 190000, 230000]
      const orderData = [320, 302, 301, 334, 390, 330]
      
      this.salesTrendOption.xAxis.data = dates
      this.salesTrendOption.series[0].data = salesData
      this.salesTrendOption.series[1].data = orderData
      
      // 热销产品模拟数据
      const productNames = ['笔记本电脑', '手机', '平板电脑', '耳机', '电视']
      const productSales = [
        { value: 334000, name: '笔记本电脑' },
        { value: 290000, name: '手机' },
        { value: 235000, name: '平板电脑' },
        { value: 135000, name: '耳机' },
        { value: 148000, name: '电视' }
      ]
      
      this.topProductsOption.legend.data = productNames
      this.topProductsOption.series[0].data = productSales
      
      // 销售地域分布模拟数据
      const regions = ['华东', '华南', '华北', '华中', '西南', '东北', '西北']
      const regionSales = [
        { value: 420000, name: '华东' },
        { value: 360000, name: '华南' },
        { value: 310000, name: '华北' },
        { value: 280000, name: '华中' },
        { value: 190000, name: '西南' },
        { value: 150000, name: '东北' },
        { value: 120000, name: '西北' }
      ]
      
      this.regionSalesOption.legend.data = regions
      this.regionSalesOption.series[0].data = regionSales
      
      // 采购与销售对比模拟数据
      const categoryNames = ['电子产品', '家用电器', '办公用品', '服装', '食品']
      const purchaseData = [200000, 180000, 90000, 70000, 50000]
      const salesData2 = [300000, 230000, 120000, 90000, 80000]
      const profitData = [100000, 50000, 30000, 20000, 30000]
      
      this.purchaseSalesOption.yAxis.data = categoryNames
      this.purchaseSalesOption.series[0].data = purchaseData
      this.purchaseSalesOption.series[1].data = salesData2
      this.purchaseSalesOption.series[2].data = profitData
    },
    formatNumber(num) {
      return num.toString().replace(/(\d)(?=(?:\d{3})+$)/g, '$1,')
    }
  }
}
</script>

<style scoped>
.dashboard {
  padding: 0;
}

.dashboard-header {
  background-color: #fff;
  margin-bottom: 24px;
}

.card-content {
  text-align: center;
}

.card-statistic {
  display: flex;
  align-items: baseline;
  justify-content: center;
  margin-bottom: 16px;
}

.card-value {
  font-size: 30px;
  font-weight: 500;
  margin-right: 16px;
}

.card-trend {
  font-size: 16px;
}

.trend-up {
  color: #f5222d;
}

.trend-down {
  color: #52c41a;
}

.card-footer {
  color: rgba(0, 0, 0, 0.45);
  font-size: 14px;
}

.echarts {
  min-height: 300px;
}
</style> 