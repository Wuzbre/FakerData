<template>
  <div class="inventory-turnover">
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
          <a-card class="metric-card">
            <a-statistic
              title="平均库存周转率"
              :value="statistics.avgTurnoverRate"
              :precision="2"
              suffix="次/年"
              style="text-align: center"
            >
              <template #prefix>
                <icon-font type="icon-swap" />
              </template>
            </a-statistic>
            <div class="metric-footer">
              <span :class="statistics.turnoverRateChange >= 0 ? 'up' : 'down'">
                <icon-font :type="statistics.turnoverRateChange >= 0 ? 'icon-rise' : 'icon-fall'" />
                {{ Math.abs(statistics.turnoverRateChange) }}%
              </span>
              <span class="metric-desc">同比上期</span>
            </div>
          </a-card>
        </a-col>
        <a-col :span="6">
          <a-card class="metric-card">
            <a-statistic
              title="平均库存周转天数"
              :value="statistics.avgTurnoverDays"
              suffix="天"
              style="text-align: center"
            >
              <template #prefix>
                <icon-font type="icon-calendar" />
              </template>
            </a-statistic>
            <div class="metric-footer">
              <span :class="statistics.turnoverDaysChange <= 0 ? 'up' : 'down'">
                <icon-font :type="statistics.turnoverDaysChange <= 0 ? 'icon-rise' : 'icon-fall'" />
                {{ Math.abs(statistics.turnoverDaysChange) }}%
              </span>
              <span class="metric-desc">同比上期</span>
            </div>
          </a-card>
        </a-col>
        <a-col :span="6">
          <a-card class="metric-card">
            <a-statistic
              title="库存总金额"
              :value="statistics.totalInventoryValue"
              :precision="2"
              suffix="元"
              style="text-align: center"
            >
              <template #prefix>
                <icon-font type="icon-money" />
              </template>
            </a-statistic>
            <div class="metric-footer">
              <span :class="statistics.inventoryValueChange <= 0 ? 'up' : 'down'">
                <icon-font :type="statistics.inventoryValueChange <= 0 ? 'icon-rise' : 'icon-fall'" />
                {{ Math.abs(statistics.inventoryValueChange) }}%
              </span>
              <span class="metric-desc">环比变化</span>
            </div>
          </a-card>
        </a-col>
        <a-col :span="6">
          <a-card class="metric-card">
            <a-statistic
              title="滞销商品数"
              :value="statistics.slowMovingCount"
              suffix="种"
              style="text-align: center"
            >
              <template #prefix>
                <icon-font type="icon-warning" />
              </template>
            </a-statistic>
            <div class="metric-footer">
              <span :class="statistics.slowMovingChange <= 0 ? 'up' : 'down'">
                <icon-font :type="statistics.slowMovingChange <= 0 ? 'icon-rise' : 'icon-fall'" />
                {{ Math.abs(statistics.slowMovingChange) }}%
              </span>
              <span class="metric-desc">环比变化</span>
            </div>
          </a-card>
        </a-col>
      </a-row>

      <a-row :gutter="24" style="margin-top: 24px">
        <a-col :span="12">
          <a-card class="metric-card">
            <div class="metric-title">库存周转率趋势</div>
            <div class="metric-chart">
              <a-spin :spinning="loading">
                <v-chart class="chart" :option="turnoverTrendChart" autoresize />
              </a-spin>
            </div>
          </a-card>
        </a-col>
        <a-col :span="12">
          <a-card class="metric-card">
            <div class="metric-title">产品类别库存周转对比</div>
            <div class="metric-chart">
              <a-spin :spinning="loading">
                <v-chart class="chart" :option="categoryTurnoverChart" autoresize />
              </a-spin>
            </div>
          </a-card>
        </a-col>
      </a-row>

      <a-row :gutter="24" style="margin-top: 24px">
        <a-col :span="12">
          <a-card class="metric-card">
            <div class="metric-title">ABC库存分析</div>
            <div class="metric-chart">
              <a-spin :spinning="loading">
                <v-chart class="chart" :option="abcAnalysisChart" autoresize />
              </a-spin>
            </div>
          </a-card>
        </a-col>
        <a-col :span="12">
          <a-card class="metric-card">
            <div class="metric-title">库存健康状况</div>
            <div class="metric-chart">
              <a-spin :spinning="loading">
                <v-chart class="chart" :option="inventoryHealthChart" autoresize />
              </a-spin>
            </div>
          </a-card>
        </a-col>
      </a-row>

      <a-card style="margin-top: 24px" :bordered="false" title="产品库存周转明细">
        <a-table
          :loading="loading"
          :columns="columns"
          :data-source="tableData"
          :pagination="pagination"
          @change="handleTableChange"
        >
          <template slot="inventoryValue" slot-scope="text">
            ¥{{ formatNumber(text) }}
          </template>
          <template slot="costOfGoodsSold" slot-scope="text">
            ¥{{ formatNumber(text) }}
          </template>
          <template slot="avgInventoryValue" slot-scope="text">
            ¥{{ formatNumber(text) }}
          </template>
          <template slot="turnoverRate" slot-scope="text">
            {{ text }}次/年
          </template>
          <template slot="status" slot-scope="text">
            <a-tag :color="getStatusColor(text)">{{ text }}</a-tag>
          </template>
        </a-table>
      </a-card>
    </a-card>
  </div>
</template>

<script>
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { PieChart, BarChart, LineChart, RadarChart } from 'echarts/charts'
import {
  GridComponent,
  TooltipComponent,
  TitleComponent,
  LegendComponent,
  ToolboxComponent
} from 'echarts/components'
import VChart from 'vue-echarts'
import { getInventoryTurnover } from '@/api/inventory'
import { IconFont } from '@/core/icons'
import moment from 'moment'

use([
  CanvasRenderer,
  PieChart,
  BarChart,
  LineChart,
  RadarChart,
  GridComponent,
  TooltipComponent,
  TitleComponent,
  LegendComponent,
  ToolboxComponent
])

export default {
  name: 'InventoryTurnover',
  components: {
    VChart,
    IconFont
  },
  data() {
    return {
      loading: false,
      dateFormat: 'YYYY-MM-DD',
      dateRange: [moment().subtract(365, 'days'), moment()],
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
        avgTurnoverRate: 5.2,
        avgTurnoverDays: 70,
        totalInventoryValue: 1250000,
        slowMovingCount: 15,
        turnoverRateChange: 8.3,
        turnoverDaysChange: -7.8,
        inventoryValueChange: -3.5,
        slowMovingChange: -12.5
      },
      // 库存周转率趋势图
      turnoverTrendChart: {
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
          data: ['周转率', '周转天数']
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
            name: '周转率(次/年)',
            position: 'left'
          },
          {
            type: 'value',
            name: '周转天数(天)',
            position: 'right'
          }
        ],
        series: [
          {
            name: '周转率',
            type: 'line',
            smooth: true,
            data: [],
            itemStyle: {
              color: '#1890ff'
            }
          },
          {
            name: '周转天数',
            type: 'line',
            smooth: true,
            yAxisIndex: 1,
            data: [],
            itemStyle: {
              color: '#faad14'
            }
          }
        ]
      },
      // 产品类别库存周转对比
      categoryTurnoverChart: {
        tooltip: {
          trigger: 'axis',
          axisPointer: {
            type: 'shadow'
          }
        },
        legend: {
          data: ['周转率', '周转天数', '库存金额']
        },
        grid: {
          left: '3%',
          right: '4%',
          bottom: '3%',
          containLabel: true
        },
        xAxis: {
          type: 'category',
          data: ['电子产品', '服装', '家居', '食品', '美妆', '运动户外']
        },
        yAxis: [
          {
            type: 'value',
            name: '周转率/天数',
            position: 'left'
          },
          {
            type: 'value',
            name: '库存金额(万元)',
            position: 'right'
          }
        ],
        series: [
          {
            name: '周转率',
            type: 'bar',
            data: [],
            itemStyle: {
              color: '#1890ff'
            }
          },
          {
            name: '周转天数',
            type: 'bar',
            data: [],
            itemStyle: {
              color: '#faad14'
            }
          },
          {
            name: '库存金额',
            type: 'line',
            yAxisIndex: 1,
            data: [],
            itemStyle: {
              color: '#52c41a'
            }
          }
        ]
      },
      // ABC库存分析
      abcAnalysisChart: {
        tooltip: {
          trigger: 'item',
          formatter: '{a} <br/>{b}: {c} ({d}%)'
        },
        legend: {
          orient: 'vertical',
          right: 10,
          top: 'center',
          data: ['A类库存', 'B类库存', 'C类库存']
        },
        series: [
          {
            name: '库存分类',
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
              { value: 450000, name: 'A类库存', itemStyle: { color: '#52c41a' } },
              { value: 350000, name: 'B类库存', itemStyle: { color: '#1890ff' } },
              { value: 200000, name: 'C类库存', itemStyle: { color: '#faad14' } }
            ]
          }
        ]
      },
      // 库存健康状况
      inventoryHealthChart: {
        tooltip: {
          trigger: 'item',
          formatter: function(params) {
            return `${params.name}: ${params.value}%`;
          }
        },
        radar: {
          indicator: [
            { name: '周转率', max: 100 },
            { name: '库存准确率', max: 100 },
            { name: '库存利用率', max: 100 },
            { name: '库存服务水平', max: 100 },
            { name: '交付及时率', max: 100 }
          ],
          radius: '65%'
        },
        series: [
          {
            name: '库存健康状况',
            type: 'radar',
            data: [
              {
                value: [85, 92, 78, 88, 90],
                name: '当前水平',
                symbol: 'circle',
                symbolSize: 8,
                itemStyle: {
                  color: '#1890ff'
                },
                areaStyle: {
                  color: 'rgba(24, 144, 255, 0.3)'
                }
              },
              {
                value: [75, 85, 70, 80, 85],
                name: '行业平均',
                symbol: 'circle',
                symbolSize: 8,
                itemStyle: {
                  color: '#52c41a'
                },
                areaStyle: {
                  color: 'rgba(82, 196, 26, 0.3)'
                }
              }
            ]
          }
        ]
      },
      columns: [
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
          title: '库存数量',
          dataIndex: 'inventoryQuantity',
          key: 'inventoryQuantity',
          sorter: (a, b) => a.inventoryQuantity - b.inventoryQuantity
        },
        {
          title: '库存金额',
          dataIndex: 'inventoryValue',
          key: 'inventoryValue',
          sorter: (a, b) => a.inventoryValue - b.inventoryValue,
          scopedSlots: { customRender: 'inventoryValue' }
        },
        {
          title: '销货成本',
          dataIndex: 'costOfGoodsSold',
          key: 'costOfGoodsSold',
          sorter: (a, b) => a.costOfGoodsSold - b.costOfGoodsSold,
          scopedSlots: { customRender: 'costOfGoodsSold' }
        },
        {
          title: '平均库存金额',
          dataIndex: 'avgInventoryValue',
          key: 'avgInventoryValue',
          sorter: (a, b) => a.avgInventoryValue - b.avgInventoryValue,
          scopedSlots: { customRender: 'avgInventoryValue' }
        },
        {
          title: '周转率',
          dataIndex: 'turnoverRate',
          key: 'turnoverRate',
          sorter: (a, b) => a.turnoverRate - b.turnoverRate,
          scopedSlots: { customRender: 'turnoverRate' }
        },
        {
          title: '周转天数',
          dataIndex: 'turnoverDays',
          key: 'turnoverDays',
          sorter: (a, b) => a.turnoverDays - b.turnoverDays,
          customRender: (text) => `${text}天`
        },
        {
          title: '库存状态',
          dataIndex: 'status',
          key: 'status',
          filters: [
            { text: '正常', value: '正常' },
            { text: '过高', value: '过高' },
            { text: '过低', value: '过低' },
            { text: '滞销', value: '滞销' }
          ],
          onFilter: (value, record) => record.status === value,
          scopedSlots: { customRender: 'status' }
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
      getInventoryTurnover(this.queryParams)
        .then(res => {
          if (res.success) {
            this.processData(res.data);
          }
        })
        .catch(err => {
          console.error('获取库存周转数据失败', err);
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
        if (data.statistics) {
          this.statistics = data.statistics;
        }
        
        // 更新图表数据
        this.updateChartData(data);
      } else {
        this.loadMockData();
      }
    },
    updateChartData(data) {
      // 如果API返回了实际数据，在这里更新图表
      if (data.turnoverTrend) {
        this.updateTurnoverTrendChart(data.turnoverTrend);
      }
      
      if (data.categoryTurnover) {
        this.updateCategoryTurnoverChart(data.categoryTurnover);
      }
      
      if (data.abcAnalysis) {
        this.updateABCAnalysisChart(data.abcAnalysis);
      }
      
      if (data.inventoryHealth) {
        this.updateInventoryHealthChart(data.inventoryHealth);
      }
    },
    loadMockData() {
      // 生成库存周转趋势数据
      const months = ['1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月'];
      const turnoverRateData = [];
      const turnoverDaysData = [];
      
      // 模拟一年中的周转率和周转天数趋势
      for (let i = 0; i < 12; i++) {
        // 周转率在4-6之间波动，有一定的季节性
        const baseRate = 5;
        // 假设二季度和三季度销售较好，周转率高
        const seasonFactor = (i >= 3 && i <= 8) ? 1.1 : 0.9;
        const randomFactor = 0.9 + Math.random() * 0.2; // 0.9-1.1的随机因子
        const turnoverRate = +(baseRate * seasonFactor * randomFactor).toFixed(2);
        turnoverRateData.push(turnoverRate);
        
        // 周转天数 = 365 / 周转率
        const turnoverDays = Math.round(365 / turnoverRate);
        turnoverDaysData.push(turnoverDays);
      }
      
      // 更新库存周转率趋势图
      this.turnoverTrendChart.xAxis.data = months;
      this.turnoverTrendChart.series[0].data = turnoverRateData;
      this.turnoverTrendChart.series[1].data = turnoverDaysData;
      
      // 更新产品类别库存周转对比图
      const categories = ['电子产品', '服装', '家居', '食品', '美妆', '运动户外'];
      const categoryRates = [];
      const categoryDays = [];
      const categoryValues = [];
      
      categories.forEach((category, index) => {
        // 不同产品类别有不同的周转特性
        let baseRate;
        if (category === '食品') {
          baseRate = 8; // 食品周转率高
        } else if (category === '电子产品' || category === '家居') {
          baseRate = 3.5; // 电子产品和家居周转率低
        } else {
          baseRate = 5; // 其他类别中等
        }
        
        const randomFactor = 0.9 + Math.random() * 0.2;
        const rate = +(baseRate * randomFactor).toFixed(2);
        categoryRates.push(rate);
        
        const days = Math.round(365 / rate);
        categoryDays.push(days);
        
        // 库存金额在20-80万之间
        const inventoryValue = Math.round((20 + Math.random() * 60) * 10) / 10;
        categoryValues.push(inventoryValue);
      });
      
      this.categoryTurnoverChart.xAxis.data = categories;
      this.categoryTurnoverChart.series[0].data = categoryRates;
      this.categoryTurnoverChart.series[1].data = categoryDays;
      this.categoryTurnoverChart.series[2].data = categoryValues;
      
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
        
        // 根据产品类型设置不同的库存特性
        let baseRate, baseQuantity, baseValue;
        if (category === '食品') {
          baseRate = 8;
          baseQuantity = 300;
          baseValue = 200;
        } else if (category === '电子产品') {
          baseRate = 3.5;
          baseQuantity = 100;
          baseValue = 800;
        } else if (category === '家居') {
          baseRate = 3.5;
          baseQuantity = 150;
          baseValue = 500;
        } else {
          baseRate = 5;
          baseQuantity = 200;
          baseValue = 300;
        }
        
        // 添加随机波动
        const randomFactor = 0.8 + Math.random() * 0.4; // 0.8-1.2之间的随机因子
        const turnoverRate = +(baseRate * randomFactor).toFixed(2);
        const turnoverDays = Math.round(365 / turnoverRate);
        
        const inventoryQuantity = Math.round(baseQuantity * randomFactor);
        const unitValue = baseValue * (0.9 + Math.random() * 0.2);
        const inventoryValue = Math.round(inventoryQuantity * unitValue);
        const avgInventoryValue = Math.round(inventoryValue * 0.95); // 平均库存略低于当前库存
        const costOfGoodsSold = Math.round(avgInventoryValue * turnoverRate);
        
        // 根据周转率和行业基准确定库存状态
        let status;
        if (turnoverRate < baseRate * 0.7) {
          status = '滞销';
        } else if (turnoverRate < baseRate * 0.9) {
          status = '过高';
        } else if (turnoverRate > baseRate * 1.2) {
          status = '过低';
        } else {
          status = '正常';
        }
        
        mockProducts.push({
          productId: `P${10000 + index}`,
          productName: name,
          categoryName: category,
          inventoryQuantity,
          inventoryValue,
          avgInventoryValue,
          costOfGoodsSold,
          turnoverRate,
          turnoverDays,
          status,
          key: `P${10000 + index}`
        });
      });
      
      // 更新表格数据
      this.tableData = mockProducts;
      this.pagination.total = mockProducts.length;
      
      // 更新统计数据
      const totalInventoryValue = mockProducts.reduce((sum, p) => sum + p.inventoryValue, 0);
      const slowMovingCount = mockProducts.filter(p => p.status === '滞销').length;
      const avgTurnoverRate = +(mockProducts.reduce((sum, p) => sum + p.turnoverRate, 0) / mockProducts.length).toFixed(2);
      const avgTurnoverDays = Math.round(mockProducts.reduce((sum, p) => sum + p.turnoverDays, 0) / mockProducts.length);
      
      this.statistics = {
        avgTurnoverRate,
        avgTurnoverDays,
        totalInventoryValue,
        slowMovingCount,
        turnoverRateChange: 8.3,
        turnoverDaysChange: -7.8,
        inventoryValueChange: -3.5,
        slowMovingChange: -12.5
      };
    },
    getStatusColor(status) {
      const colorMap = {
        '正常': 'green',
        '过高': 'orange',
        '过低': 'blue',
        '滞销': 'red'
      };
      return colorMap[status] || 'default';
    },
    handleDateRangeChange(dates, dateStrings) {
      this.queryParams.startDate = dateStrings[0];
      this.queryParams.endDate = dateStrings[1];
    },
    resetQuery() {
      this.dateRange = [moment().subtract(365, 'days'), moment()];
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
.inventory-turnover {
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