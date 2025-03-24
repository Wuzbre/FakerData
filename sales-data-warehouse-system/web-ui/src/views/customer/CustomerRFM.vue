<template>
  <div class="customer-rfm">
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
              <span class="table-page-search-submitButtons">
                <a-button type="primary" @click="fetchData">查询</a-button>
                <a-button style="margin-left: 8px" @click="resetQuery">重置</a-button>
              </span>
            </a-col>
          </a-row>
        </a-form>
      </div>

      <a-row :gutter="24">
        <a-col :span="8">
          <a-card class="metric-card">
            <div class="metric-title">RFM客户分布</div>
            <div class="metric-chart">
              <a-spin :spinning="loading">
                <v-chart class="chart" :option="rfmDistributionChart" autoresize />
              </a-spin>
            </div>
            <div class="metric-desc">
              <ul>
                <li>高价值客户：R、F、M均高</li>
                <li>高潜力客户：F较低，但R和M较高</li>
                <li>忠诚客户：F高，M中等</li>
                <li>流失客户：R低，F和M较低</li>
                <li>一般价值客户：各指标中等</li>
              </ul>
            </div>
          </a-card>
        </a-col>

        <a-col :span="16">
          <a-card class="metric-card">
            <div class="metric-title">客户价值象限分布</div>
            <div class="metric-chart">
              <a-spin :spinning="loading">
                <v-chart class="chart" :option="customerQuadrantChart" autoresize />
              </a-spin>
            </div>
            <div class="metric-desc">
              散点图中的客户按照最近购买时间(R)和消费金额(M)分布在四个象限，颜色表示购买频率(F)
            </div>
          </a-card>
        </a-col>
      </a-row>

      <a-row :gutter="24" style="margin-top: 24px">
        <a-col :span="24">
          <a-card class="metric-card">
            <a-tabs default-active-key="1" @change="handleTabChange">
              <a-tab-pane key="1" tab="RFM指标分析">
                <a-row :gutter="24">
                  <a-col :span="8">
                    <div class="chart-wrapper">
                      <a-spin :spinning="loading">
                        <h4 class="chart-title">最近购买时间分布(R)</h4>
                        <v-chart class="segment-chart" :option="recencyChart" autoresize />
                      </a-spin>
                    </div>
                  </a-col>
                  <a-col :span="8">
                    <div class="chart-wrapper">
                      <a-spin :spinning="loading">
                        <h4 class="chart-title">购买频率分布(F)</h4>
                        <v-chart class="segment-chart" :option="frequencyChart" autoresize />
                      </a-spin>
                    </div>
                  </a-col>
                  <a-col :span="8">
                    <div class="chart-wrapper">
                      <a-spin :spinning="loading">
                        <h4 class="chart-title">消费金额分布(M)</h4>
                        <v-chart class="segment-chart" :option="monetaryChart" autoresize />
                      </a-spin>
                    </div>
                  </a-col>
                </a-row>
              </a-tab-pane>
              <a-tab-pane key="2" tab="客户地域分布">
                <a-row :gutter="24">
                  <a-col :span="12">
                    <div class="chart-wrapper">
                      <a-spin :spinning="loading">
                        <h4 class="chart-title">客户地域分布</h4>
                        <v-chart class="segment-chart" :option="regionDistributionChart" autoresize />
                      </a-spin>
                    </div>
                  </a-col>
                  <a-col :span="12">
                    <div class="chart-wrapper">
                      <a-spin :spinning="loading">
                        <h4 class="chart-title">地域客户价值分布</h4>
                        <v-chart class="segment-chart" :option="regionValueChart" autoresize />
                      </a-spin>
                    </div>
                  </a-col>
                </a-row>
              </a-tab-pane>
            </a-tabs>
          </a-card>
        </a-col>
      </a-row>

      <a-card style="margin-top: 24px" :bordered="false" title="客户细分明细">
        <a-table
          :loading="loading"
          :columns="columns"
          :data-source="tableData"
          :pagination="pagination"
          @change="handleTableChange"
        >
          <template slot="monetary" slot-scope="text">
            ¥{{ formatNumber(text) }}
          </template>
          <template slot="avgOrderAmount" slot-scope="text">
            ¥{{ formatNumber(text) }}
          </template>
          <template slot="recency" slot-scope="text">
            {{ text }} 天
          </template>
          <template slot="customerTag" slot-scope="text">
            <a-tag :color="getTagColor(text)">{{ text }}</a-tag>
          </template>
        </a-table>
      </a-card>
    </a-card>
  </div>
</template>

<script>
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { PieChart, BarChart, ScatterChart } from 'echarts/charts'
import {
  GridComponent,
  TooltipComponent,
  TitleComponent,
  LegendComponent,
  VisualMapComponent
} from 'echarts/components'
import VChart from 'vue-echarts'
import { getCustomerRFM } from '@/api/sales'
import moment from 'moment'

use([
  CanvasRenderer,
  PieChart,
  BarChart,
  ScatterChart,
  GridComponent,
  TooltipComponent,
  TitleComponent,
  LegendComponent,
  VisualMapComponent
])

export default {
  name: 'CustomerRFM',
  components: {
    VChart
  },
  data() {
    return {
      loading: false,
      dateFormat: 'YYYY-MM-DD',
      dateRange: [moment().subtract(365, 'days'), moment()],
      activeTab: '1',
      queryParams: {
        startDate: null,
        endDate: null
      },
      // RFM客户分布饼图
      rfmDistributionChart: {
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
          data: ['高价值客户', '高潜力客户', '忠诚客户', '流失客户', '一般价值客户']
        },
        series: [
          {
            name: '客户分类',
            type: 'pie',
            radius: '55%',
            center: ['40%', '50%'],
            data: [
              { value: 335, name: '高价值客户' },
              { value: 310, name: '高潜力客户' },
              { value: 234, name: '忠诚客户' },
              { value: 135, name: '流失客户' },
              { value: 1548, name: '一般价值客户' }
            ],
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
      // 客户价值象限散点图
      customerQuadrantChart: {
        tooltip: {
          trigger: 'item',
          formatter: function(params) {
            return `客户ID: ${params.data[3]}<br/>
                    最近购买: ${params.data[0]}天前<br/>
                    购买频率: ${params.data[2]}次<br/>
                    消费金额: ¥${params.data[1].toLocaleString()}<br/>
                    客户类型: ${params.data[4]}`
          }
        },
        xAxis: {
          name: '最近购买时间(天)',
          type: 'value',
          axisLabel: {
            formatter: '{value}'
          },
          splitLine: {
            show: true
          }
        },
        yAxis: {
          name: '消费金额(元)',
          type: 'value',
          axisLabel: {
            formatter: '{value}'
          },
          splitLine: {
            show: true
          }
        },
        visualMap: [
          {
            type: 'continuous',
            min: 1,
            max: 30,
            dimension: 2,
            orient: 'vertical',
            right: 10,
            top: 'center',
            text: ['购买频率高', '购买频率低'],
            calculable: true,
            inRange: {
              color: ['#50a3ba', '#eac736', '#d94e5d']
            }
          }
        ],
        series: [
          {
            name: '客户分布',
            type: 'scatter',
            data: [
              // [R, M, F, 客户ID, 客户类型]
              [5, 5000, 20, 'C10001', '高价值客户'],
              [10, 4500, 18, 'C10002', '高价值客户'],
              [15, 4000, 15, 'C10003', '高价值客户'],
              [7, 3500, 12, 'C10004', '高潜力客户'],
              [25, 3000, 5, 'C10005', '高潜力客户'],
              [30, 2500, 3, 'C10006', '高潜力客户'],
              [10, 2000, 20, 'C10007', '忠诚客户'],
              [15, 1500, 25, 'C10008', '忠诚客户'],
              [20, 1000, 15, 'C10009', '忠诚客户'],
              [60, 800, 5, 'C10010', '流失客户'],
              [90, 600, 3, 'C10011', '流失客户'],
              [120, 500, 2, 'C10012', '流失客户'],
              [30, 1500, 8, 'C10013', '一般价值客户'],
              [40, 1200, 7, 'C10014', '一般价值客户'],
              [50, 1000, 6, 'C10015', '一般价值客户']
            ],
            symbolSize: function(data) {
              return Math.sqrt(data[2]) * 5;
            }
          }
        ]
      },
      // 最近购买时间分布图
      recencyChart: {
        tooltip: {
          trigger: 'axis',
          axisPointer: {
            type: 'shadow'
          }
        },
        xAxis: {
          type: 'category',
          data: ['0-7天', '8-30天', '31-60天', '61-180天', '181-365天', '>365天'],
          axisLabel: {
            interval: 0,
            rotate: 30
          }
        },
        yAxis: {
          type: 'value',
          name: '客户数量'
        },
        series: [
          {
            name: '客户数量',
            type: 'bar',
            data: [120, 200, 150, 80, 70, 110],
            itemStyle: {
              color: function(params) {
                // 颜色从绿到红
                const colorList = ['#91cc75', '#5470c6', '#ee6666', '#fac858', '#ee6666', '#73c0de'];
                return colorList[params.dataIndex];
              }
            }
          }
        ]
      },
      // 购买频率分布图
      frequencyChart: {
        tooltip: {
          trigger: 'axis',
          axisPointer: {
            type: 'shadow'
          }
        },
        xAxis: {
          type: 'category',
          data: ['1次', '2-3次', '4-6次', '7-10次', '11-20次', '>20次'],
          axisLabel: {
            interval: 0,
            rotate: 30
          }
        },
        yAxis: {
          type: 'value',
          name: '客户数量'
        },
        series: [
          {
            name: '客户数量',
            type: 'bar',
            data: [180, 230, 160, 90, 50, 20],
            itemStyle: {
              color: function(params) {
                // 颜色从绿到红
                const colorList = ['#ee6666', '#fac858', '#73c0de', '#5470c6', '#91cc75', '#91cc75'];
                return colorList[params.dataIndex];
              }
            }
          }
        ]
      },
      // 消费金额分布图
      monetaryChart: {
        tooltip: {
          trigger: 'axis',
          axisPointer: {
            type: 'shadow'
          }
        },
        xAxis: {
          type: 'category',
          data: ['<500', '500-1000', '1000-3000', '3000-5000', '5000-10000', '>10000'],
          axisLabel: {
            interval: 0,
            rotate: 30
          }
        },
        yAxis: {
          type: 'value',
          name: '客户数量'
        },
        series: [
          {
            name: '客户数量',
            type: 'bar',
            data: [200, 180, 160, 120, 60, 30],
            itemStyle: {
              color: function(params) {
                // 颜色从绿到红
                const colorList = ['#ee6666', '#fac858', '#73c0de', '#5470c6', '#91cc75', '#91cc75'];
                return colorList[params.dataIndex];
              }
            }
          }
        ]
      },
      // 客户地域分布图
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
            name: '客户分布',
            type: 'pie',
            radius: '55%',
            center: ['40%', '50%'],
            data: [
              { value: 335, name: '华东' },
              { value: 310, name: '华南' },
              { value: 234, name: '华北' },
              { value: 135, name: '华中' },
              { value: 102, name: '西南' },
              { value: 87, name: '东北' },
              { value: 48, name: '西北' }
            ],
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
      // 地域客户价值分布图
      regionValueChart: {
        tooltip: {
          trigger: 'axis',
          axisPointer: {
            type: 'shadow'
          }
        },
        legend: {
          data: ['高价值客户', '高潜力客户', '忠诚客户', '流失客户', '一般价值客户']
        },
        xAxis: {
          type: 'category',
          data: ['华东', '华南', '华北', '华中', '西南', '东北', '西北'],
          axisLabel: {
            interval: 0
          }
        },
        yAxis: {
          type: 'value',
          name: '客户数量'
        },
        series: [
          {
            name: '高价值客户',
            type: 'bar',
            stack: 'total',
            data: [120, 100, 90, 70, 50, 30, 20]
          },
          {
            name: '高潜力客户',
            type: 'bar',
            stack: 'total',
            data: [90, 80, 70, 60, 40, 30, 20]
          },
          {
            name: '忠诚客户',
            type: 'bar',
            stack: 'total',
            data: [80, 70, 60, 50, 30, 20, 10]
          },
          {
            name: '流失客户',
            type: 'bar',
            stack: 'total',
            data: [40, 35, 30, 25, 20, 15, 5]
          },
          {
            name: '一般价值客户',
            type: 'bar',
            stack: 'total',
            data: [160, 140, 130, 120, 100, 80, 60]
          }
        ]
      },
      columns: [
        {
          title: '客户ID',
          dataIndex: 'userId',
          key: 'userId'
        },
        {
          title: '客户名称',
          dataIndex: 'userName',
          key: 'userName'
        },
        {
          title: '客户等级',
          dataIndex: 'userLevel',
          key: 'userLevel',
          filters: [
            { text: '普通会员', value: '普通会员' },
            { text: '银卡会员', value: '银卡会员' },
            { text: '金卡会员', value: '金卡会员' },
            { text: '钻石会员', value: '钻石会员' }
          ],
          onFilter: (value, record) => record.userLevel === value
        },
        {
          title: '地区',
          dataIndex: 'region',
          key: 'region',
          filters: [
            { text: '华东', value: '华东' },
            { text: '华南', value: '华南' },
            { text: '华北', value: '华北' },
            { text: '华中', value: '华中' },
            { text: '西南', value: '西南' },
            { text: '东北', value: '东北' },
            { text: '西北', value: '西北' }
          ],
          onFilter: (value, record) => record.region === value
        },
        {
          title: '最近购买(R)',
          dataIndex: 'recency',
          key: 'recency',
          sorter: (a, b) => a.recency - b.recency,
          scopedSlots: { customRender: 'recency' }
        },
        {
          title: '购买频率(F)',
          dataIndex: 'frequency',
          key: 'frequency',
          sorter: (a, b) => a.frequency - b.frequency
        },
        {
          title: '消费金额(M)',
          dataIndex: 'monetary',
          key: 'monetary',
          sorter: (a, b) => a.monetary - b.monetary,
          scopedSlots: { customRender: 'monetary' }
        },
        {
          title: '平均订单金额',
          dataIndex: 'avgOrderAmount',
          key: 'avgOrderAmount',
          sorter: (a, b) => a.avgOrderAmount - b.avgOrderAmount,
          scopedSlots: { customRender: 'avgOrderAmount' }
        },
        {
          title: '客户分类',
          dataIndex: 'customerTag',
          key: 'customerTag',
          filters: [
            { text: '高价值客户', value: '高价值客户' },
            { text: '高潜力客户', value: '高潜力客户' },
            { text: '忠诚客户', value: '忠诚客户' },
            { text: '流失客户', value: '流失客户' },
            { text: '一般价值客户', value: '一般价值客户' }
          ],
          onFilter: (value, record) => record.customerTag === value,
          scopedSlots: { customRender: 'customerTag' }
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
      getCustomerRFM(this.queryParams)
        .then(res => {
          if (res.success) {
            this.processData(res.data);
          }
        })
        .catch(err => {
          console.error('获取客户RFM分析数据失败', err);
          // 加载模拟数据用于演示
          this.loadMockData();
        })
        .finally(() => {
          this.loading = false;
        });
    },
    processData(data) {
      // 如果API返回了实际数据，在这里处理
      if (data && data.customerList) {
        this.tableData = data.customerList;
        this.pagination.total = this.tableData.length;
        
        // 更新图表数据
        this.updateChartData(data);
      } else {
        this.loadMockData();
      }
    },
    loadMockData() {
      // 生成模拟的客户RFM数据
      const mockData = [];
      const userLevels = ['普通会员', '银卡会员', '金卡会员', '钻石会员'];
      const regions = ['华东', '华南', '华北', '华中', '西南', '东北', '西北'];
      const customerTags = ['高价值客户', '高潜力客户', '忠诚客户', '流失客户', '一般价值客户'];
      
      for (let i = 1; i <= 100; i++) {
        const userId = `C${10000 + i}`;
        const userName = `客户${i}`;
        const userLevel = userLevels[Math.floor(Math.random() * userLevels.length)];
        const region = regions[Math.floor(Math.random() * regions.length)];
        
        // RFM指标生成
        let recency, frequency, monetary, customerTag;
        
        // 根据客户ID尾号分类，确保有一定分布
        const lastDigit = i % 5;
        
        if (lastDigit === 0) {
          // 高价值客户
          recency = Math.floor(Math.random() * 15) + 1; // 1-15天
          frequency = Math.floor(Math.random() * 15) + 15; // 15-30次
          monetary = Math.floor(Math.random() * 5000) + 5000; // 5000-10000元
          customerTag = customerTags[0];
        } else if (lastDigit === 1) {
          // 高潜力客户
          recency = Math.floor(Math.random() * 20) + 5; // 5-25天
          frequency = Math.floor(Math.random() * 5) + 3; // 3-8次
          monetary = Math.floor(Math.random() * 4000) + 3000; // 3000-7000元
          customerTag = customerTags[1];
        } else if (lastDigit === 2) {
          // 忠诚客户
          recency = Math.floor(Math.random() * 30) + 15; // 15-45天
          frequency = Math.floor(Math.random() * 10) + 15; // 15-25次
          monetary = Math.floor(Math.random() * 2000) + 1000; // 1000-3000元
          customerTag = customerTags[2];
        } else if (lastDigit === 3) {
          // 流失客户
          recency = Math.floor(Math.random() * 180) + 60; // 60-240天
          frequency = Math.floor(Math.random() * 5) + 1; // 1-6次
          monetary = Math.floor(Math.random() * 1000) + 500; // 500-1500元
          customerTag = customerTags[3];
        } else {
          // 一般价值客户
          recency = Math.floor(Math.random() * 60) + 20; // 20-80天
          frequency = Math.floor(Math.random() * 8) + 5; // 5-13次
          monetary = Math.floor(Math.random() * 2500) + 1000; // 1000-3500元
          customerTag = customerTags[4];
        }
        
        // 计算平均订单金额
        const avgOrderAmount = Math.round(monetary / frequency);
        
        mockData.push({
          userId,
          userName,
          userLevel,
          region,
          recency,
          frequency,
          monetary,
          avgOrderAmount,
          customerTag,
          firstOrderDate: moment().subtract(Math.floor(Math.random() * 365) + 180, 'days').format('YYYY-MM-DD'),
          lastOrderDate: moment().subtract(recency, 'days').format('YYYY-MM-DD'),
          registerDate: moment().subtract(Math.floor(Math.random() * 730) + 365, 'days').format('YYYY-MM-DD'),
          key: userId
        });
      }
      
      this.tableData = mockData;
      this.pagination.total = mockData.length;
      
      // 生成象限散点图数据
      this.updateScatterData(mockData);
      
      // 更新其他图表数据
      this.updateDistributionData(mockData);
    },
    updateScatterData(data) {
      // 更新象限散点图数据
      const scatterData = data.map(customer => [
        customer.recency,
        customer.monetary,
        customer.frequency,
        customer.userId,
        customer.customerTag
      ]);
      
      this.customerQuadrantChart.series[0].data = scatterData;
    },
    updateDistributionData(data) {
      // 按客户分类统计
      const tagCounts = {};
      customerTags.forEach(tag => {
        tagCounts[tag] = 0;
      });
      
      data.forEach(customer => {
        if (tagCounts[customer.customerTag] !== undefined) {
          tagCounts[customer.customerTag]++;
        }
      });
      
      // 更新RFM分布饼图
      this.rfmDistributionChart.series[0].data = Object.keys(tagCounts).map(tag => ({
        name: tag,
        value: tagCounts[tag]
      }));
      
      // 统计最近购买时间分布
      const recencyCounts = [0, 0, 0, 0, 0, 0]; // [0-7, 8-30, 31-60, 61-180, 181-365, >365]
      data.forEach(customer => {
        if (customer.recency <= 7) recencyCounts[0]++;
        else if (customer.recency <= 30) recencyCounts[1]++;
        else if (customer.recency <= 60) recencyCounts[2]++;
        else if (customer.recency <= 180) recencyCounts[3]++;
        else if (customer.recency <= 365) recencyCounts[4]++;
        else recencyCounts[5]++;
      });
      this.recencyChart.series[0].data = recencyCounts;
      
      // 统计购买频率分布
      const frequencyCounts = [0, 0, 0, 0, 0, 0]; // [1, 2-3, 4-6, 7-10, 11-20, >20]
      data.forEach(customer => {
        if (customer.frequency === 1) frequencyCounts[0]++;
        else if (customer.frequency <= 3) frequencyCounts[1]++;
        else if (customer.frequency <= 6) frequencyCounts[2]++;
        else if (customer.frequency <= 10) frequencyCounts[3]++;
        else if (customer.frequency <= 20) frequencyCounts[4]++;
        else frequencyCounts[5]++;
      });
      this.frequencyChart.series[0].data = frequencyCounts;
      
      // 统计消费金额分布
      const monetaryCounts = [0, 0, 0, 0, 0, 0]; // [<500, 500-1000, 1000-3000, 3000-5000, 5000-10000, >10000]
      data.forEach(customer => {
        if (customer.monetary < 500) monetaryCounts[0]++;
        else if (customer.monetary < 1000) monetaryCounts[1]++;
        else if (customer.monetary < 3000) monetaryCounts[2]++;
        else if (customer.monetary < 5000) monetaryCounts[3]++;
        else if (customer.monetary < 10000) monetaryCounts[4]++;
        else monetaryCounts[5]++;
      });
      this.monetaryChart.series[0].data = monetaryCounts;
      
      // 按地域统计客户分布
      const regionCounts = {};
      const regions = ['华东', '华南', '华北', '华中', '西南', '东北', '西北'];
      regions.forEach(region => {
        regionCounts[region] = 0;
      });
      
      data.forEach(customer => {
        if (regionCounts[customer.region] !== undefined) {
          regionCounts[customer.region]++;
        }
      });
      
      // 更新地域分布饼图
      this.regionDistributionChart.series[0].data = Object.keys(regionCounts).map(region => ({
        name: region,
        value: regionCounts[region]
      }));
      
      // 按地域和客户类型统计
      const regionTagCounts = {};
      regions.forEach(region => {
        regionTagCounts[region] = {};
        customerTags.forEach(tag => {
          regionTagCounts[region][tag] = 0;
        });
      });
      
      data.forEach(customer => {
        if (regionTagCounts[customer.region] && 
            regionTagCounts[customer.region][customer.customerTag] !== undefined) {
          regionTagCounts[customer.region][customer.customerTag]++;
        }
      });
      
      // 更新地域客户价值分布图
      const seriesData = [];
      customerTags.forEach((tag, index) => {
        seriesData.push({
          name: tag,
          type: 'bar',
          stack: 'total',
          data: regions.map(region => regionTagCounts[region][tag])
        });
      });
      
      this.regionValueChart.series = seriesData;
    },
    getTagColor(tag) {
      const colorMap = {
        '高价值客户': '#f50',
        '高潜力客户': '#2db7f5',
        '忠诚客户': '#87d068',
        '流失客户': '#f5222d',
        '一般价值客户': '#faad14'
      };
      return colorMap[tag] || '#108ee9';
    },
    handleDateRangeChange(dates, dateStrings) {
      this.queryParams.startDate = dateStrings[0];
      this.queryParams.endDate = dateStrings[1];
    },
    resetQuery() {
      this.dateRange = [moment().subtract(365, 'days'), moment()];
      this.queryParams = {
        startDate: this.dateRange[0].format(this.dateFormat),
        endDate: this.dateRange[1].format(this.dateFormat)
      };
      this.fetchData();
    },
    handleTabChange(activeKey) {
      this.activeTab = activeKey;
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
.customer-rfm {
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

.metric-desc {
  margin-top: 16px;
  color: rgba(0, 0, 0, 0.45);
  font-size: 14px;
}

.chart-wrapper {
  margin: 16px 0;
  padding: 16px;
  background: #fff;
  border-radius: 2px;
}

.chart-title {
  margin-bottom: 16px;
  color: rgba(0, 0, 0, 0.85);
  font-weight: 500;
  text-align: center;
}

.segment-chart {
  height: 300px;
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