<template>
  <div class="product-sales">
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
              <a-form-item label="产品类别">
                <a-select
                  v-model="queryParams.categoryId"
                  placeholder="请选择产品类别"
                  style="width: 100%"
                  allowClear
                >
                  <a-select-option v-for="item in categoryOptions" :key="item.value">{{ item.label }}</a-select-option>
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
          <a-statistic title="总销售额" :value="statistics.totalSales" :precision="2">
            <template #prefix>
              <icon-font type="icon-money" />
            </template>
            <template #suffix>
              <span>元</span>
            </template>
          </a-statistic>
        </a-col>
        <a-col :span="6">
          <a-statistic title="销售总量" :value="statistics.totalQuantity">
            <template #prefix>
              <icon-font type="icon-package" />
            </template>
            <template #suffix>
              <span>件</span>
            </template>
          </a-statistic>
        </a-col>
        <a-col :span="6">
          <a-statistic title="平均单价" :value="statistics.avgPrice" :precision="2">
            <template #prefix>
              <icon-font type="icon-tag" />
            </template>
            <template #suffix>
              <span>元/件</span>
            </template>
          </a-statistic>
        </a-col>
        <a-col :span="6">
          <a-statistic title="畅销品种数" :value="statistics.hotProductCount">
            <template #prefix>
              <icon-font type="icon-fire" />
            </template>
            <template #suffix>
              <span>种</span>
            </template>
          </a-statistic>
        </a-col>
      </a-row>

      <a-divider style="margin: 24px 0" />

      <a-row :gutter="24">
        <a-col :span="16">
          <a-card class="metric-card">
            <div class="metric-title">产品销售趋势</div>
            <div class="metric-chart">
              <a-spin :spinning="loading">
                <v-chart class="chart" :option="salesTrendChart" autoresize />
              </a-spin>
            </div>
          </a-card>
        </a-col>
        <a-col :span="8">
          <a-card class="metric-card">
            <div class="metric-title">产品类别销售占比</div>
            <div class="metric-chart">
              <a-spin :spinning="loading">
                <v-chart class="chart" :option="categorySalesChart" autoresize />
              </a-spin>
            </div>
          </a-card>
        </a-col>
      </a-row>

      <a-row :gutter="24" style="margin-top: 24px">
        <a-col :span="12">
          <a-card class="metric-card">
            <div class="metric-title">热销产品TOP10</div>
            <div class="metric-chart">
              <a-spin :spinning="loading">
                <v-chart class="chart" :option="topProductsChart" autoresize />
              </a-spin>
            </div>
          </a-card>
        </a-col>
        <a-col :span="12">
          <a-card class="metric-card">
            <div class="metric-title">产品销量与利润分布</div>
            <div class="metric-chart">
              <a-spin :spinning="loading">
                <v-chart class="chart" :option="profitDistributionChart" autoresize />
              </a-spin>
            </div>
          </a-card>
        </a-col>
      </a-row>

      <a-card style="margin-top: 24px" :bordered="false" title="产品销售明细">
        <a-table
          :loading="loading"
          :columns="columns"
          :data-source="tableData"
          :pagination="pagination"
          @change="handleTableChange"
        >
          <template slot="productImg" slot-scope="text, record">
            <img :src="record.productImg || '/assets/default-product.png'" alt="产品图片" style="width: 50px; height: 50px;" />
          </template>
          <template slot="sales" slot-scope="text">
            ¥{{ formatNumber(text) }}
          </template>
          <template slot="price" slot-scope="text">
            ¥{{ formatNumber(text) }}
          </template>
          <template slot="profit" slot-scope="text">
            ¥{{ formatNumber(text) }}
          </template>
          <template slot="profitRate" slot-scope="text">
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
import { PieChart, BarChart, LineChart, ScatterChart } from 'echarts/charts'
import {
  GridComponent,
  TooltipComponent,
  TitleComponent,
  LegendComponent,
  MarkPointComponent,
  MarkLineComponent
} from 'echarts/components'
import VChart from 'vue-echarts'
import { getProductSales } from '@/api/sales'
import { IconFont } from '@/core/icons'
import moment from 'moment'

use([
  CanvasRenderer,
  PieChart,
  BarChart,
  LineChart,
  ScatterChart,
  GridComponent,
  TooltipComponent,
  TitleComponent,
  LegendComponent,
  MarkPointComponent,
  MarkLineComponent
])

export default {
  name: 'ProductSales',
  components: {
    VChart,
    IconFont
  },
  data() {
    return {
      loading: false,
      dateFormat: 'YYYY-MM-DD',
      dateRange: [moment().subtract(30, 'days'), moment()],
      categoryOptions: [
        { label: '全部类别', value: '' },
        { label: '电子产品', value: '1' },
        { label: '服装', value: '2' },
        { label: '家居', value: '3' },
        { label: '食品', value: '4' },
        { label: '美妆', value: '5' },
        { label: '运动户外', value: '6' }
      ],
      queryParams: {
        startDate: null,
        endDate: null,
        categoryId: ''
      },
      statistics: {
        totalSales: 0,
        totalQuantity: 0,
        avgPrice: 0,
        hotProductCount: 0
      },
      // 产品销售趋势图
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
          data: ['销售额', '销售量']
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
        yAxis: [
          {
            type: 'value',
            name: '销售额(元)',
            position: 'left'
          },
          {
            type: 'value',
            name: '销售量(件)',
            position: 'right'
          }
        ],
        series: [
          {
            name: '销售额',
            type: 'line',
            smooth: true,
            yAxisIndex: 0,
            data: [],
            itemStyle: {
              color: '#3aa1ff'
            },
            areaStyle: {
              color: {
                type: 'linear',
                x: 0,
                y: 0,
                x2: 0,
                y2: 1,
                colorStops: [
                  { offset: 0, color: 'rgba(58,161,255,0.3)' },
                  { offset: 1, color: 'rgba(58,161,255,0.1)' }
                ]
              }
            }
          },
          {
            name: '销售量',
            type: 'line',
            smooth: true,
            yAxisIndex: 1,
            data: [],
            itemStyle: {
              color: '#36cfc9'
            }
          }
        ]
      },
      // 产品类别销售占比
      categorySalesChart: {
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
      // 热销产品TOP10
      topProductsChart: {
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
          type: 'value',
          name: '销售量(件)'
        },
        yAxis: {
          type: 'category',
          data: [],
          axisLabel: {
            formatter: function(value) {
              if (value.length > 10) {
                return value.substring(0, 10) + '...';
              }
              return value;
            }
          }
        },
        series: [
          {
            name: '销售量',
            type: 'bar',
            data: [],
            itemStyle: {
              color: function(params) {
                // 颜色从深到浅
                const colorList = [
                  '#f5222d', '#fa541c', '#fa8c16', '#faad14', '#fadb14',
                  '#a0d911', '#52c41a', '#13c2c2', '#1890ff', '#722ed1'
                ];
                return colorList[params.dataIndex % colorList.length];
              }
            }
          }
        ]
      },
      // 产品销量与利润分布
      profitDistributionChart: {
        tooltip: {
          trigger: 'item',
          formatter: function(params) {
            return `${params.data.name}<br/>
                    销售量: ${params.data.value[0]}件<br/>
                    利润率: ${params.data.value[1]}%<br/>
                    销售额: ¥${params.data.value[2].toLocaleString()}`
          }
        },
        xAxis: {
          type: 'value',
          name: '销售量(件)',
          splitLine: {
            lineStyle: {
              type: 'dashed'
            }
          }
        },
        yAxis: {
          type: 'value',
          name: '利润率(%)',
          splitLine: {
            lineStyle: {
              type: 'dashed'
            }
          }
        },
        series: [
          {
            name: '产品分布',
            type: 'scatter',
            data: [],
            symbolSize: function(data) {
              return Math.sqrt(data[2]) / 50;
            }
          }
        ]
      },
      columns: [
        {
          title: '产品图片',
          dataIndex: 'productImg',
          key: 'productImg',
          scopedSlots: { customRender: 'productImg' }
        },
        {
          title: '产品ID',
          dataIndex: 'productId',
          key: 'productId'
        },
        {
          title: '产品名称',
          dataIndex: 'productName',
          key: 'productName'
        },
        {
          title: '产品类别',
          dataIndex: 'categoryName',
          key: 'categoryName',
          filters: [
            { text: '电子产品', value: '电子产品' },
            { text: '服装', value: '服装' },
            { text: '家居', value: '家居' },
            { text: '食品', value: '食品' },
            { text: '美妆', value: '美妆' },
            { text: '运动户外', value: '运动户外' }
          ],
          onFilter: (value, record) => record.categoryName === value
        },
        {
          title: '销售单价',
          dataIndex: 'price',
          key: 'price',
          sorter: (a, b) => a.price - b.price,
          scopedSlots: { customRender: 'price' }
        },
        {
          title: '销售数量',
          dataIndex: 'quantity',
          key: 'quantity',
          sorter: (a, b) => a.quantity - b.quantity
        },
        {
          title: '销售金额',
          dataIndex: 'sales',
          key: 'sales',
          sorter: (a, b) => a.sales - b.sales,
          scopedSlots: { customRender: 'sales' }
        },
        {
          title: '利润',
          dataIndex: 'profit',
          key: 'profit',
          sorter: (a, b) => a.profit - b.profit,
          scopedSlots: { customRender: 'profit' }
        },
        {
          title: '利润率',
          dataIndex: 'profitRate',
          key: 'profitRate',
          sorter: (a, b) => a.profitRate - b.profitRate,
          scopedSlots: { customRender: 'profitRate' }
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
      getProductSales(this.queryParams)
        .then(res => {
          if (res.success) {
            this.processData(res.data);
          }
        })
        .catch(err => {
          console.error('获取产品销售数据失败', err);
          // 加载模拟数据用于演示
          this.loadMockData();
        })
        .finally(() => {
          this.loading = false;
        });
    },
    processData(data) {
      // 如果API返回了实际数据，在这里处理
      if (data && data.productList) {
        this.tableData = data.productList;
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
      
      if (data.categorySales) {
        this.updateCategorySalesChart(data.categorySales);
      }
      
      if (data.topProducts) {
        this.updateTopProductsChart(data.topProducts);
      }
      
      if (data.productList) {
        this.updateProfitDistributionChart(data.productList);
      }
    },
    loadMockData() {
      // 生成日期范围内的日期数组
      const days = [];
      const salesData = [];
      const quantityData = [];
      
      const startDate = moment(this.queryParams.startDate);
      const endDate = moment(this.queryParams.endDate);
      const diff = endDate.diff(startDate, 'days');
      
      // 生成时间序列数据
      for (let i = 0; i <= diff; i++) {
        const date = moment(startDate).add(i, 'days');
        days.push(date.format('MM-DD'));
        
        // 销售额在10000-50000之间波动，周末略高
        const dayOfWeek = date.day();
        const isWeekend = dayOfWeek === 0 || dayOfWeek === 6;
        const baseSales = isWeekend ? 40000 : 25000;
        const sales = Math.floor(baseSales + Math.random() * 10000);
        salesData.push(sales);
        
        // 销售量在100-500之间波动
        const baseQuantity = isWeekend ? 400 : 250;
        const quantity = Math.floor(baseQuantity + Math.random() * 100);
        quantityData.push(quantity);
      }
      
      // 更新销售趋势图
      this.salesTrendChart.xAxis.data = days;
      this.salesTrendChart.series[0].data = salesData;
      this.salesTrendChart.series[1].data = quantityData;
      
      // 生成产品类别销售数据
      const categories = ['电子产品', '服装', '家居', '食品', '美妆', '运动户外'];
      const categorySales = categories.map(category => ({
        name: category,
        value: Math.floor(Math.random() * 50000 + 30000)
      }));
      
      // 更新产品类别销售占比图
      this.categorySalesChart.legend.data = categories;
      this.categorySalesChart.series[0].data = categorySales;
      
      // 生成模拟产品数据
      const productNames = [
        'iPhone 13 Pro', 'MacBook Air', 'Samsung Galaxy S21', 'AirPods Pro',
        '休闲运动裤', '时尚连衣裙', '男士衬衫', '儿童套装',
        '北欧风沙发', '简约茶几', '多功能书桌', '人体工学椅',
        '有机蔬菜礼盒', '坚果礼盒', '零食大礼包', '进口牛排',
        '精华面霜', '防晒霜', '口红套装', '洁面乳',
        '瑜伽垫', '跑步机', '健身哑铃', '户外帐篷'
      ];
      
      const mockProducts = [];
      productNames.forEach((name, index) => {
        const categoryIndex = Math.floor(index / 4);
        const category = categories[categoryIndex];
        
        const quantity = Math.floor(Math.random() * 1000 + 100);
        const price = Math.floor(Math.random() * 1000 + 100);
        const sales = quantity * price;
        const cost = price * 0.6; // 假设成本为售价的60%
        const profit = sales - (cost * quantity);
        const profitRate = Math.round((profit / sales) * 100);
        
        mockProducts.push({
          productId: `P${10000 + index}`,
          productName: name,
          categoryId: (categoryIndex + 1).toString(),
          categoryName: category,
          price,
          quantity,
          sales,
          profit,
          profitRate,
          productImg: `/assets/products/${index % 8 + 1}.jpg`, // 假设有8张产品图循环使用
          key: `P${10000 + index}`
        });
      });
      
      // 更新表格数据
      this.tableData = mockProducts;
      this.pagination.total = mockProducts.length;
      
      // 更新热销产品TOP10图表
      const topProducts = [...mockProducts].sort((a, b) => b.quantity - a.quantity).slice(0, 10);
      this.topProductsChart.yAxis.data = topProducts.map(p => p.productName).reverse();
      this.topProductsChart.series[0].data = topProducts.map(p => p.quantity).reverse();
      
      // 更新产品销量与利润分布图
      this.profitDistributionChart.series[0].data = mockProducts.map(p => ({
        name: p.productName,
        value: [p.quantity, p.profitRate, p.sales]
      }));
      
      // 更新统计数据
      const totalSales = mockProducts.reduce((sum, p) => sum + p.sales, 0);
      const totalQuantity = mockProducts.reduce((sum, p) => sum + p.quantity, 0);
      this.statistics = {
        totalSales,
        totalQuantity,
        avgPrice: Math.round((totalSales / totalQuantity) * 100) / 100,
        hotProductCount: topProducts.length
      };
    },
    handleDateRangeChange(dates, dateStrings) {
      this.queryParams.startDate = dateStrings[0];
      this.queryParams.endDate = dateStrings[1];
    },
    resetQuery() {
      this.dateRange = [moment().subtract(30, 'days'), moment()];
      this.queryParams = {
        startDate: this.dateRange[0].format(this.dateFormat),
        endDate: this.dateRange[1].format(this.dateFormat),
        categoryId: ''
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
.product-sales {
  background-color: #fff;
}

.table-page-search-wrapper {
  margin-bottom: 24px;
}

.metric-card {
  margin-bottom: 24px;
}

.metric-title {
  font-size: 16px;
  font-weight: 500;
  color: rgba(0, 0, 0, 0.85);
  margin-bottom: 16px;
  text-align: center;
}

.metric-chart {
  height: 320px;
}

.chart {
  height: 100%;
  width: 100%;
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
