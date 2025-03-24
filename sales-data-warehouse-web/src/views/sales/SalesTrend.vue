<template>
  <div class="sales-trend">
    <a-card :bordered="false">
      <div class="table-page-search-wrapper">
        <a-form layout="inline">
          <a-row :gutter="48">
            <a-col :md="8" :sm="24">
              <a-form-item label="时间范围">
                <a-range-picker 
                  v-model="dateRange"
                  style="width: 100%"
                  :format="dateFormat"
                  @change="handleDateRangeChange"
                />
              </a-form-item>
            </a-col>
            <a-col :md="8" :sm="24">
              <a-form-item label="时间粒度">
                <a-select
                  v-model="queryParams.timeGranularity"
                  placeholder="请选择时间粒度"
                  style="width: 100%"
                >
                  <a-select-option value="day">日</a-select-option>
                  <a-select-option value="week">周</a-select-option>
                  <a-select-option value="month">月</a-select-option>
                </a-select>
              </a-form-item>
            </a-col>
            <a-col :md="8" :sm="24">
              <span class="table-page-search-submitButtons">
                <a-button type="primary" @click="fetchData">查询</a-button>
                <a-button style="margin-left: 8px" @click="resetQuery">重置</a-button>
              </span>
            </a-col>
          </a-row>
        </a-form>
      </div>

      <a-row :gutter="24">
        <a-col :span="6">
          <a-card class="metric-card">
            <a-statistic
              title="总销售额"
              :value="statistics.totalSales"
              :precision="2"
              suffix="元"
              style="text-align: center"
            >
              <template #prefix>
                <icon-font type="icon-money" />
              </template>
            </a-statistic>
            <div class="metric-footer" v-if="statistics.salesGrowth !== undefined">
              <span :class="statistics.salesGrowth >= 0 ? 'up' : 'down'">
                <icon-font :type="statistics.salesGrowth >= 0 ? 'icon-rise' : 'icon-fall'" />
                {{ Math.abs(statistics.salesGrowth) }}%
              </span>
              <span class="metric-desc">同比上期</span>
            </div>
          </a-card>
        </a-col>
        <a-col :span="6">
          <a-card class="metric-card">
            <a-statistic
              title="订单总数"
              :value="statistics.totalOrders"
              style="text-align: center"
            >
              <template #prefix>
                <icon-font type="icon-file" />
              </template>
            </a-statistic>
            <div class="metric-footer" v-if="statistics.orderGrowth !== undefined">
              <span :class="statistics.orderGrowth >= 0 ? 'up' : 'down'">
                <icon-font :type="statistics.orderGrowth >= 0 ? 'icon-rise' : 'icon-fall'" />
                {{ Math.abs(statistics.orderGrowth) }}%
              </span>
              <span class="metric-desc">同比上期</span>
            </div>
          </a-card>
        </a-col>
        <a-col :span="6">
          <a-card class="metric-card">
            <a-statistic
              title="客单价"
              :value="statistics.avgOrderValue"
              :precision="2"
              suffix="元"
              style="text-align: center"
            >
              <template #prefix>
                <icon-font type="icon-shopping" />
              </template>
            </a-statistic>
            <div class="metric-footer" v-if="statistics.aovGrowth !== undefined">
              <span :class="statistics.aovGrowth >= 0 ? 'up' : 'down'">
                <icon-font :type="statistics.aovGrowth >= 0 ? 'icon-rise' : 'icon-fall'" />
                {{ Math.abs(statistics.aovGrowth) }}%
              </span>
              <span class="metric-desc">同比上期</span>
            </div>
          </a-card>
        </a-col>
        <a-col :span="6">
          <a-card class="metric-card">
            <a-statistic
              title="转化率"
              :value="statistics.conversionRate"
              :precision="2"
              suffix="%"
              style="text-align: center"
            >
              <template #prefix>
                <icon-font type="icon-percentage" />
              </template>
            </a-statistic>
            <div class="metric-footer" v-if="statistics.conversionGrowth !== undefined">
              <span :class="statistics.conversionGrowth >= 0 ? 'up' : 'down'">
                <icon-font :type="statistics.conversionGrowth >= 0 ? 'icon-rise' : 'icon-fall'" />
                {{ Math.abs(statistics.conversionGrowth) }}%
              </span>
              <span class="metric-desc">同比上期</span>
            </div>
          </a-card>
        </a-col>
      </a-row>

      <a-card style="margin-top: 24px" :bordered="false">
        <a-tabs default-active-key="1" @change="handleTabChange">
          <a-tab-pane key="1" tab="销售趋势">
            <div class="chart-wrapper">
              <a-spin :spinning="loading">
                <div class="chart-header">
                  <h4 class="chart-title">销售趋势分析</h4>
                  <div class="chart-extra">
                    <a-radio-group v-model="chartView" button-style="solid" @change="handleViewChange">
                      <a-radio-button value="sales">销售额</a-radio-button>
                      <a-radio-button value="orders">订单量</a-radio-button>
                      <a-radio-button value="aov">客单价</a-radio-button>
                    </a-radio-group>
                  </div>
                </div>
                <v-chart class="main-chart" :option="salesTrendChart" autoresize />
              </a-spin>
            </div>
          </a-tab-pane>
          <a-tab-pane key="2" tab="销售分布">
            <a-row :gutter="24">
              <a-col :span="12">
                <div class="chart-wrapper">
                  <a-spin :spinning="loading">
                    <h4 class="chart-title">销售渠道分布</h4>
                    <v-chart class="chart" :option="channelDistributionChart" autoresize />
                  </a-spin>
                </div>
              </a-col>
              <a-col :span="12">
                <div class="chart-wrapper">
                  <a-spin :spinning="loading">
                    <h4 class="chart-title">销售区域分布</h4>
                    <v-chart class="chart" :option="regionDistributionChart" autoresize />
                  </a-spin>
                </div>
              </a-col>
            </a-row>
          </a-tab-pane>
        </a-tabs>
      </a-card>

      <a-row :gutter="24" style="margin-top: 24px">
        <a-col :span="12">
          <a-card class="metric-card">
            <div class="metric-title">销售额周同比/环比</div>
            <div class="sales-extra">
              <a-radio-group default-value="all" button-style="solid" @change="handleRangeChange">
                <a-radio-button value="all">全部</a-radio-button>
                <a-radio-button value="month">本月</a-radio-button>
                <a-radio-button value="week">本周</a-radio-button>
                <a-radio-button value="day">今日</a-radio-button>
              </a-radio-group>
            </div>
            <div class="metric-chart">
              <a-spin :spinning="loading">
                <v-chart class="chart" :option="salesComparisonChart" autoresize />
              </a-spin>
            </div>
          </a-card>
        </a-col>
        <a-col :span="12">
          <a-card class="metric-card">
            <div class="metric-title">热销时段分析</div>
            <div class="metric-chart">
              <a-spin :spinning="loading">
                <v-chart class="chart" :option="hourlyDistributionChart" autoresize />
              </a-spin>
            </div>
          </a-card>
        </a-col>
      </a-row>

      <a-card style="margin-top: 24px" :bordered="false" title="销售数据明细">
        <a-table
          :loading="loading"
          :columns="columns"
          :data-source="tableData"
          :pagination="pagination"
          @change="handleTableChange"
        >
          <template slot="sales" slot-scope="text">
            ¥{{ formatNumber(text) }}
          </template>
          <template slot="avgOrderValue" slot-scope="text">
            ¥{{ formatNumber(text) }}
          </template>
          <template slot="conversionRate" slot-scope="text">
            {{ text }}%
          </template>
        </a-table>
      </a-card>
    </a-card>
  </div>
</template>

<script>
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { PieChart, BarChart, LineChart } from 'echarts/charts'
import {
  GridComponent,
  TooltipComponent,
  TitleComponent,
  LegendComponent,
  MarkLineComponent
} from 'echarts/components'
import VChart from 'vue-echarts'
import { getSalesTrend } from '@/api/sales'
import { IconFont } from '@/core/icons'
import moment from 'moment'

use([
  CanvasRenderer,
  PieChart,
  BarChart,
  LineChart,
  GridComponent,
  TooltipComponent,
  TitleComponent,
  LegendComponent,
  MarkLineComponent
])

export default {
  name: 'SalesTrend',
  components: {
    VChart,
    IconFont
  },
  data() {
    return {
      loading: false,
      dateFormat: 'YYYY-MM-DD',
      dateRange: [moment().subtract(30, 'days'), moment()],
      activeTab: '1',
      chartView: 'sales', // 默认查看销售额
      chartRange: 'all', // 默认时间范围
      queryParams: {
        startDate: null,
        endDate: null,
        timeGranularity: 'day'
      },
      statistics: {
        totalSales: 0,
        totalOrders: 0,
        avgOrderValue: 0,
        conversionRate: 0,
        salesGrowth: 0,
        orderGrowth: 0,
        aovGrowth: 0,
        conversionGrowth: 0
      },
      // 销售趋势图
      salesTrendChart: {
        tooltip: {
          trigger: 'axis',
          axisPointer: {
            type: 'cross',
            label: {
              backgroundColor: '#6a7985'
            }
          }
        },
        legend: {
          data: ['销售额', '同比']
        },
        grid: {
          left: '3%',
          right: '4%',
          bottom: '3%',
          containLabel: true
        },
        xAxis: {
          type: 'category',
          boundaryGap: false,
          data: []
        },
        yAxis: {
          type: 'value'
        },
        series: [
          {
            name: '销售额',
            type: 'line',
            smooth: true,
            data: [],
            itemStyle: {
              color: '#1890ff'
            },
            areaStyle: {
              color: {
                type: 'linear',
                x: 0,
                y: 0,
                x2: 0,
                y2: 1,
                colorStops: [
                  { offset: 0, color: 'rgba(24,144,255,0.3)' },
                  { offset: 1, color: 'rgba(24,144,255,0.1)' }
                ]
              }
            },
            markLine: {
              data: [
                { type: 'average', name: '平均值' }
              ]
            }
          },
          {
            name: '同比',
            type: 'line',
            smooth: true,
            data: [],
            itemStyle: {
              color: '#faad14'
            }
          }
        ]
      },
      // 销售额周同比/环比图
      salesComparisonChart: {
        tooltip: {
          trigger: 'axis',
          axisPointer: {
            type: 'shadow'
          }
        },
        legend: {
          data: ['本期销售额', '上期销售额', '环比增长率']
        },
        grid: {
          left: '3%',
          right: '4%',
          bottom: '3%',
          containLabel: true
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
              formatter: '¥{value}'
            }
          },
          {
            type: 'value',
            name: '增长率',
            position: 'right',
            axisLabel: {
              formatter: '{value}%'
            }
          }
        ],
        series: [
          {
            name: '本期销售额',
            type: 'bar',
            data: [],
            barGap: '0%',
            itemStyle: {
              color: '#1890ff'
            }
          },
          {
            name: '上期销售额',
            type: 'bar',
            data: [],
            barGap: '0%',
            itemStyle: {
              color: '#13c2c2'
            }
          },
          {
            name: '环比增长率',
            type: 'line',
            yAxisIndex: 1,
            data: [],
            itemStyle: {
              color: '#faad14'
            }
          }
        ]
      },
      // 销售渠道分布图
      channelDistributionChart: {
        tooltip: {
          trigger: 'item',
          formatter: '{a} <br/>{b}: {c} ({d}%)'
        },
        legend: {
          type: 'scroll',
          orient: 'vertical',
          right: 10,
          top: 20,
          bottom: 20,
          data: ['线上商城', '实体门店', '第三方平台', '电话订单', '其他']
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
            data: [
              { value: 335, name: '线上商城' },
              { value: 310, name: '实体门店' },
              { value: 234, name: '第三方平台' },
              { value: 135, name: '电话订单' },
              { value: 40, name: '其他' }
            ]
          }
        ]
      },
      // 销售区域分布图
      regionDistributionChart: {
        tooltip: {
          trigger: 'item',
          formatter: '{a} <br/>{b}: {c} ({d}%)'
        },
        legend: {
          type: 'scroll',
          orient: 'vertical',
          right: 10,
          top: 20,
          bottom: 20,
          data: ['华东', '华南', '华北', '华中', '西南', '东北', '西北']
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
            data: [
              { value: 335, name: '华东' },
              { value: 310, name: '华南' },
              { value: 234, name: '华北' },
              { value: 135, name: '华中' },
              { value: 102, name: '西南' },
              { value: 87, name: '东北' },
              { value: 48, name: '西北' }
            ]
          }
        ]
      },
      // 热销时段分布图
      hourlyDistributionChart: {
        tooltip: {
          trigger: 'axis',
          axisPointer: {
            type: 'shadow'
          }
        },
        grid: {
          left: '3%',
          right: '4%',
          bottom: '3%',
          containLabel: true
        },
        xAxis: {
          type: 'category',
          data: [
            '00:00', '01:00', '02:00', '03:00', '04:00', '05:00',
            '06:00', '07:00', '08:00', '09:00', '10:00', '11:00',
            '12:00', '13:00', '14:00', '15:00', '16:00', '17:00',
            '18:00', '19:00', '20:00', '21:00', '22:00', '23:00'
          ]
        },
        yAxis: {
          type: 'value',
          name: '销售额'
        },
        series: [
          {
            name: '销售额',
            type: 'bar',
            data: [],
            itemStyle: {
              color: function(params) {
                // 颜色从深蓝到浅蓝
                return `rgba(24, 144, 255, ${0.3 + params.data / 1500 * 0.7})`;
              }
            }
          }
        ]
      },
      columns: [
        {
          title: '日期',
          dataIndex: 'date',
          key: 'date'
        },
        {
          title: '销售额',
          dataIndex: 'sales',
          key: 'sales',
          sorter: (a, b) => a.sales - b.sales,
          scopedSlots: { customRender: 'sales' }
        },
        {
          title: '订单数',
          dataIndex: 'orders',
          key: 'orders',
          sorter: (a, b) => a.orders - b.orders
        },
        {
          title: '客单价',
          dataIndex: 'avgOrderValue',
          key: 'avgOrderValue',
          sorter: (a, b) => a.avgOrderValue - b.avgOrderValue,
          scopedSlots: { customRender: 'avgOrderValue' }
        },
        {
          title: '转化率',
          dataIndex: 'conversionRate',
          key: 'conversionRate',
          sorter: (a, b) => a.conversionRate - b.conversionRate,
          scopedSlots: { customRender: 'conversionRate' }
        },
        {
          title: '同比增长',
          dataIndex: 'yoyGrowth',
          key: 'yoyGrowth',
          customRender: (text) => {
            if (text === null || text === undefined) return '-';
            return <span style={{ color: text >= 0 ? '#52c41a' : '#f5222d' }}>
              {text >= 0 ? `+${text}%` : `${text}%`}
            </span>
          }
        },
        {
          title: '环比增长',
          dataIndex: 'momGrowth',
          key: 'momGrowth',
          customRender: (text) => {
            if (text === null || text === undefined) return '-';
            return <span style={{ color: text >= 0 ? '#52c41a' : '#f5222d' }}>
              {text >= 0 ? `+${text}%` : `${text}%`}
            </span>
          }
        }
      ],
      tableData: [],
      pagination: {
        current: 1,
        pageSize: 10,
        total: 0,
        showSizeChanger: true,
        showQuickJumper: true,
        showTotal: total => `共 ${total} 条记录`
      }
    }
  },
  created() {
    this.initQueryParams();
    this.fetchData();
  },
  methods: {
    initQueryParams() {
      if (this.dateRange && this.dateRange.length === 2) {
        this.queryParams.startDate = this.dateRange[0].format(this.dateFormat);
        this.queryParams.endDate = this.dateRange[1].format(this.dateFormat);
      }
    },
    fetchData() {
      this.loading = true;
      getSalesTrend(this.queryParams)
        .then(res => {
          if (res.success) {
            this.processData(res.data);
          }
        })
        .catch(err => {
          console.error('获取销售趋势数据失败', err);
          // 加载模拟数据用于演示
          this.loadMockData();
        })
        .finally(() => {
          this.loading = false;
        });
    },
    processData(data) {
      // 如果API返回了实际数据，在这里处理
      if (data && data.salesData) {
        this.tableData = data.salesData;
        this.pagination.total = this.tableData.length;
        
        // 更新统计数据
        this.updateStatistics(data.statistics);
        
        // 更新图表数据
        this.updateChartData(data);
      } else {
        this.loadMockData();
      }
    },
    updateStatistics(statistics) {
      if (statistics) {
        this.statistics = statistics;
      }
    },
    updateChartData(data) {
      // 如果API返回了实际数据，在这里更新图表
      if (data.salesTrend) {
        this.updateSalesTrendChart(data.salesTrend);
      }
      
      if (data.salesComparison) {
        this.updateSalesComparisonChart(data.salesComparison);
      }
      
      if (data.channelDistribution) {
        this.updateChannelDistributionChart(data.channelDistribution);
      }
      
      if (data.regionDistribution) {
        this.updateRegionDistributionChart(data.regionDistribution);
      }
      
      if (data.hourlyDistribution) {
        this.updateHourlyDistributionChart(data.hourlyDistribution);
      }
    },
    loadMockData() {
      // 生成销售趋势数据
      const days = [];
      const salesData = [];
      const salesDataLastYear = [];
      const ordersData = [];
      const aovData = [];
      const tableData = [];
      
      const startDate = moment(this.queryParams.startDate);
      const endDate = moment(this.queryParams.endDate);
      const diff = endDate.diff(startDate, 'days');
      
      // 生成时间序列数据
      for (let i = 0; i <= diff; i++) {
        const date = moment(startDate).add(i, 'days');
        const dateStr = date.format('YYYY-MM-DD');
        days.push(date.format('MM-DD'));
        
        // 销售额在10000-50000之间波动，周末略高
        const dayOfWeek = date.day();
        const isWeekend = dayOfWeek === 0 || dayOfWeek === 6;
        const baseSales = isWeekend ? 40000 : 25000;
        const randomFactor = Math.random() * 0.4 + 0.8; // 0.8-1.2之间的随机因子
        const sales = Math.floor(baseSales * randomFactor);
        salesData.push(sales);
        
        // 去年同期销售额，假设同比增长10-20%
        const lastYearFactor = 0.8 + Math.random() * 0.1; // 0.8-0.9之间的随机因子
        const lastYearSales = Math.floor(sales * lastYearFactor);
        salesDataLastYear.push(lastYearSales);
        
        // 订单数在100-500之间波动
        const baseOrders = isWeekend ? 400 : 250;
        const orders = Math.floor(baseOrders * randomFactor);
        ordersData.push(orders);
        
        // 客单价
        const aov = Math.floor(sales / orders);
        aovData.push(aov);
        
        // 计算同比和环比增长率
        const yoyGrowth = Math.round(((sales - lastYearSales) / lastYearSales) * 100);
        let momGrowth = null;
        if (i > 0) {
          const yesterdaySales = salesData[i - 1];
          momGrowth = Math.round(((sales - yesterdaySales) / yesterdaySales) * 100);
        }
        
        // 表格数据
        tableData.push({
          date: dateStr,
          sales,
          orders,
          avgOrderValue: aov,
          conversionRate: Math.round(Math.random() * 5 + 2), // 2%-7%之间的转化率
          yoyGrowth,
          momGrowth,
          key: dateStr
        });
      }
      
      // 更新销售趋势图
      this.updateSalesTrendChartData(days, salesData, salesDataLastYear, ordersData, aovData);
      
      // 更新销售额周同比/环比图
      this.updateSalesComparisonChartData();
      
      // 更新热销时段分布图
      this.updateHourlyDistributionChartData();
      
      // 更新表格数据
      this.tableData = tableData;
      this.pagination.total = tableData.length;
      
      // 更新统计数据
      const totalSales = tableData.reduce((sum, item) => sum + item.sales, 0);
      const totalOrders = tableData.reduce((sum, item) => sum + item.orders, 0);
      const avgOrderValue = Math.round(totalSales / totalOrders);
      
      this.statistics = {
        totalSales,
        totalOrders,
        avgOrderValue,
        conversionRate: 3.5,
        salesGrowth: 15.2,
        orderGrowth: 12.8,
        aovGrowth: 2.4,
        conversionGrowth: -0.5
      };
    },
    updateSalesTrendChartData(days, salesData, salesDataLastYear, ordersData, aovData) {
      // 根据当前选择的视图更新图表
      this.salesTrendChart.xAxis.data = days;
      
      if (this.chartView === 'sales') {
        this.salesTrendChart.legend.data = ['销售额', '同比'];
        this.salesTrendChart.series[0].name = '销售额';
        this.salesTrendChart.series[0].data = salesData;
        this.salesTrendChart.series[1].name = '同比';
        this.salesTrendChart.series[1].data = salesDataLastYear;
      } else if (this.chartView === 'orders') {
        this.salesTrendChart.legend.data = ['订单量'];
        this.salesTrendChart.series[0].name = '订单量';
        this.salesTrendChart.series[0].data = ordersData;
        this.salesTrendChart.series[1].name = '';
        this.salesTrendChart.series[1].data = [];
      } else if (this.chartView === 'aov') {
        this.salesTrendChart.legend.data = ['客单价'];
        this.salesTrendChart.series[0].name = '客单价';
        this.salesTrendChart.series[0].data = aovData;
        this.salesTrendChart.series[1].name = '';
        this.salesTrendChart.series[1].data = [];
      }
    },
    updateSalesComparisonChartData() {
      // 根据所选时间范围生成比较数据
      const xAxisData = ['周一', '周二', '周三', '周四', '周五', '周六', '周日'];
      const currentPeriod = [];
      const lastPeriod = [];
      const growthRate = [];
      
      // 生成本期和上期的数据
      for (let i = 0; i < xAxisData.length; i++) {
        const current = Math.floor(Math.random() * 20000 + 15000);
        const last = Math.floor(current * (0.7 + Math.random() * 0.2));
        const growth = Math.round(((current - last) / last) * 100);
        
        currentPeriod.push(current);
        lastPeriod.push(last);
        growthRate.push(growth);
      }
      
      this.salesComparisonChart.xAxis.data = xAxisData;
      this.salesComparisonChart.series[0].data = currentPeriod;
      this.salesComparisonChart.series[1].data = lastPeriod;
      this.salesComparisonChart.series[2].data = growthRate;
    },
    updateHourlyDistributionChartData() {
      // 生成热销时段数据
      const hourlyData = [];
      
      // 模拟一天中各时段的销售额模式
      // 早上8-11点，中午12-13点，下午17-19点，晚上20-22点是高峰
      for (let hour = 0; hour < 24; hour++) {
        let baseSales;
        if (hour >= 8 && hour <= 11) {
          baseSales = 600 + Math.random() * 400;
        } else if (hour >= 12 && hour <= 13) {
          baseSales = 800 + Math.random() * 400;
        } else if (hour >= 17 && hour <= 19) {
          baseSales = 1000 + Math.random() * 500;
        } else if (hour >= 20 && hour <= 22) {
          baseSales = 900 + Math.random() * 400;
        } else {
          baseSales = 100 + Math.random() * 200;
        }
        
        hourlyData.push(Math.floor(baseSales));
      }
      
      this.hourlyDistributionChart.series[0].data = hourlyData;
    },
    handleDateRangeChange(dates, dateStrings) {
      this.queryParams.startDate = dateStrings[0];
      this.queryParams.endDate = dateStrings[1];
    },
    handleTabChange(activeKey) {
      this.activeTab = activeKey;
    },
    handleViewChange(e) {
      this.chartView = e.target.value;
      // 重新加载数据
      this.loadMockData();
    },
    handleRangeChange(e) {
      this.chartRange = e.target.value;
      // 可以根据所选范围更新周同比/环比图
      this.updateSalesComparisonChartData();
    },
    resetQuery() {
      this.dateRange = [moment().subtract(30, 'days'), moment()];
      this.queryParams = {
        startDate: this.dateRange[0].format(this.dateFormat),
        endDate: this.dateRange[1].format(this.dateFormat),
        timeGranularity: 'day'
      };
      this.fetchData();
    },
    handleTableChange(pagination, filters, sorter) {
      this.pagination.current = pagination.current;
      this.pagination.pageSize = pagination.pageSize;
    },
    formatNumber(num) {
      return num.toString().replace(/(\d)(?=(?:\d{3})+$)/g, '$1,');
    }
  }
}
</script>

<style scoped>
.sales-trend {
  background-color: #fff;
}

.table-page-search-wrapper {
  margin-bottom: 24px;
}

.metric-card {
  margin-bottom: 24px;
}

.metric-footer {
  margin-top: 8px;
  text-align: center;
}

.metric-footer .up {
  color: #52c41a;
}

.metric-footer .down {
  color: #f5222d;
}

.metric-desc {
  margin-left: 8px;
  color: rgba(0, 0, 0, 0.45);
}

.chart-wrapper {
  padding: 16px;
  border-radius: 2px;
}

.chart-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}

.chart-title {
  margin: 0;
  color: rgba(0, 0, 0, 0.85);
  font-weight: 500;
  font-size: 16px;
}

.main-chart {
  height: 400px;
  width: 100%;
}

.chart {
  height: 320px;
  width: 100%;
}

.metric-title {
  font-size: 16px;
  font-weight: 500;
  color: rgba(0, 0, 0, 0.85);
  margin-bottom: 16px;
  text-align: center;
}

.sales-extra {
  margin-bottom: 24px;
  text-align: right;
}

.table-page-search-submitButtons {
  display: block;
  margin-bottom: 24px;
  white-space: nowrap;
}

@media (min-width: 576px) {
  .table-page-search-submitButtons {
    margin-top: 24px;
    white-space: nowrap;
  }
}
</style>