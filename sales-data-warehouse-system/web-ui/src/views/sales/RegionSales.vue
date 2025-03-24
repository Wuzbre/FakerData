<template>
  <div class="region-sales">
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
              <a-form-item label="地区等级">
                <a-select
                  v-model="queryParams.regionLevel"
                  placeholder="请选择地区等级"
                  style="width: 100%"
                >
                  <a-select-option value="province">省份</a-select-option>
                  <a-select-option value="city">城市</a-select-option>
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

      <a-card style="margin-top: 24px" :bordered="false">
        <div class="metric-title">销售地区分布地图</div>
        <div class="map-chart">
          <a-spin :spinning="loading">
            <v-chart class="chart" :option="mapChart" autoresize />
          </a-spin>
        </div>
      </a-card>

      <a-row :gutter="24" style="margin-top: 24px">
        <a-col :span="12">
          <a-card class="metric-card">
            <div class="metric-title">TOP 10 地区销售额</div>
            <div class="metric-chart">
              <a-spin :spinning="loading">
                <v-chart class="chart" :option="topRegionSalesChart" autoresize />
              </a-spin>
            </div>
          </a-card>
        </a-col>
        <a-col :span="12">
          <a-card class="metric-card">
            <div class="metric-title">TOP 10 地区销售增长率</div>
            <div class="metric-chart">
              <a-spin :spinning="loading">
                <v-chart class="chart" :option="topRegionGrowthChart" autoresize />
              </a-spin>
            </div>
          </a-card>
        </a-col>
      </a-row>

      <a-row :gutter="24" style="margin-top: 24px">
        <a-col :span="12">
          <a-card class="metric-card">
            <div class="metric-title">地区客单价对比</div>
            <div class="metric-chart">
              <a-spin :spinning="loading">
                <v-chart class="chart" :option="regionAOVChart" autoresize />
              </a-spin>
            </div>
          </a-card>
        </a-col>
        <a-col :span="12">
          <a-card class="metric-card">
            <div class="metric-title">地区销售季节性分析</div>
            <div class="metric-chart">
              <a-spin :spinning="loading">
                <v-chart class="chart" :option="seasonalityChart" autoresize />
              </a-spin>
            </div>
          </a-card>
        </a-col>
      </a-row>

      <a-card style="margin-top: 24px" :bordered="false" title="地区销售明细">
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
          <template slot="salesDensity" slot-scope="text">
            ¥{{ formatNumber(text) }}
          </template>
          <template slot="growthRate" slot-scope="text">
            <span :style="{ color: text >= 0 ? '#52c41a' : '#f5222d' }">
              {{ text >= 0 ? '+' : '' }}{{ text }}%
            </span>
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
  GeoComponent,
  VisualMapComponent
} from 'echarts/components'
import VChart from 'vue-echarts'
import { getRegionSales } from '@/api/sales'
import moment from 'moment'
import chinaMap from '@/assets/map/china.json'

use([
  CanvasRenderer,
  PieChart,
  BarChart,
  LineChart,
  GridComponent,
  TooltipComponent,
  TitleComponent,
  LegendComponent,
  GeoComponent,
  VisualMapComponent
])

export default {
  name: 'RegionSales',
  components: {
    VChart
  },
  data() {
    return {
      loading: false,
      dateFormat: 'YYYY-MM-DD',
      dateRange: [moment().subtract(30, 'days'), moment()],
      queryParams: {
        startDate: null,
        endDate: null,
        regionLevel: 'province'
      },
      // 中国地图
      mapChart: {
        title: {
          text: '全国销售热力图',
          left: 'center'
        },
        tooltip: {
          trigger: 'item',
          formatter: '{b}: ¥{c} (销售额)'
        },
        visualMap: {
          min: 0,
          max: 50000,
          text: ['高', '低'],
          realtime: false,
          calculable: true,
          inRange: {
            color: ['#e0f7fa', '#4fc3f7', '#0288d1', '#01579b']
          }
        },
        geo: {
          map: 'china',
          roam: true,
          emphasis: {
            label: {
              show: true
            },
            itemStyle: {
              areaColor: '#f3b329'
            }
          },
          itemStyle: {
            areaColor: '#e0f7fa',
            borderColor: '#111'
          },
          zoom: 1.2
        },
        series: [
          {
            name: '销售额',
            type: 'map',
            geoIndex: 0,
            data: []
          }
        ]
      },
      // TOP 10 地区销售额
      topRegionSalesChart: {
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
          name: '销售额(元)'
        },
        yAxis: {
          type: 'category',
          data: [],
          axisLabel: {
            formatter: function(value) {
              if (value.length > 8) {
                return value.substring(0, 8) + '...';
              }
              return value;
            }
          }
        },
        series: [
          {
            name: '销售额',
            type: 'bar',
            data: [],
            itemStyle: {
              color: function(params) {
                // 颜色从深蓝到浅蓝的渐变
                const colorList = [
                  '#01579b', '#0277bd', '#0288d1', '#039be5', 
                  '#03a9f4', '#29b6f6', '#4fc3f7', '#81d4fa',
                  '#b3e5fc', '#e1f5fe'
                ];
                return colorList[params.dataIndex % colorList.length];
              }
            }
          }
        ]
      },
      // TOP 10 地区销售增长率
      topRegionGrowthChart: {
        tooltip: {
          trigger: 'axis',
          axisPointer: {
            type: 'shadow'
          },
          formatter: function(params) {
            return `${params[0].name}<br/>
                    增长率: ${params[0].value}%<br/>
                    销售额: ¥${params[0].data.sales.toLocaleString()}`
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
          name: '增长率(%)',
          axisLabel: {
            formatter: '{value}%'
          }
        },
        yAxis: {
          type: 'category',
          data: [],
          axisLabel: {
            formatter: function(value) {
              if (value.length > 8) {
                return value.substring(0, 8) + '...';
              }
              return value;
            }
          }
        },
        series: [
          {
            name: '增长率',
            type: 'bar',
            data: [],
            itemStyle: {
              color: function(params) {
                // 根据增长率的正负来设置颜色
                return params.value >= 0 ? '#52c41a' : '#f5222d';
              }
            }
          }
        ]
      },
      // 地区客单价对比
      regionAOVChart: {
        tooltip: {
          trigger: 'axis',
          axisPointer: {
            type: 'shadow'
          }
        },
        legend: {
          data: ['客单价', '同比增长']
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
            name: '客单价(元)',
            axisLabel: {
              formatter: '¥{value}'
            }
          },
          {
            type: 'value',
            name: '增长率(%)',
            axisLabel: {
              formatter: '{value}%'
            }
          }
        ],
        series: [
          {
            name: '客单价',
            type: 'bar',
            data: [],
            itemStyle: {
              color: '#1890ff'
            }
          },
          {
            name: '同比增长',
            type: 'line',
            yAxisIndex: 1,
            data: [],
            itemStyle: {
              color: '#722ed1'
            }
          }
        ]
      },
      // 地区销售季节性分析
      seasonalityChart: {
        tooltip: {
          trigger: 'axis',
          axisPointer: {
            type: 'cross'
          }
        },
        legend: {
          data: ['华东', '华南', '华北', '华中', '西南', '东北', '西北']
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
          data: ['1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月']
        },
        yAxis: {
          type: 'value',
          name: '销售额(元)'
        },
        series: [
          {
            name: '华东',
            type: 'line',
            data: []
          },
          {
            name: '华南',
            type: 'line',
            data: []
          },
          {
            name: '华北',
            type: 'line',
            data: []
          },
          {
            name: '华中',
            type: 'line',
            data: []
          },
          {
            name: '西南',
            type: 'line',
            data: []
          },
          {
            name: '东北',
            type: 'line',
            data: []
          },
          {
            name: '西北',
            type: 'line',
            data: []
          }
        ]
      },
      columns: [
        {
          title: '地区名称',
          dataIndex: 'regionName',
          key: 'regionName',
          sorter: (a, b) => a.regionName.localeCompare(b.regionName)
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
          title: '销售密度',
          dataIndex: 'salesDensity',
          key: 'salesDensity',
          sorter: (a, b) => a.salesDensity - b.salesDensity,
          scopedSlots: { customRender: 'salesDensity' }
        },
        {
          title: '同比增长率',
          dataIndex: 'growthRate',
          key: 'growthRate',
          sorter: (a, b) => a.growthRate - b.growthRate,
          scopedSlots: { customRender: 'growthRate' }
        },
        {
          title: '客户数',
          dataIndex: 'customers',
          key: 'customers',
          sorter: (a, b) => a.customers - b.customers
        },
        {
          title: '渗透率',
          dataIndex: 'penetrationRate',
          key: 'penetrationRate',
          sorter: (a, b) => a.penetrationRate - b.penetrationRate,
          customRender: text => `${text}%`
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
    // 注册中国地图
    this.$echarts.registerMap('china', chinaMap);
    
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
      getRegionSales(this.queryParams)
        .then(res => {
          if (res.success) {
            this.processData(res.data);
          }
        })
        .catch(err => {
          console.error('获取地区销售数据失败', err);
          // 加载模拟数据用于演示
          this.loadMockData();
        })
        .finally(() => {
          this.loading = false;
        });
    },
    processData(data) {
      // 如果API返回了实际数据，在这里处理
      if (data && data.regionList) {
        this.tableData = data.regionList;
        this.pagination.total = this.tableData.length;
        
        // 更新图表数据
        this.updateChartData(data);
      } else {
        this.loadMockData();
      }
    },
    updateChartData(data) {
      // 如果API返回了实际数据，在这里更新图表
      if (data.mapData) {
        this.updateMapChart(data.mapData);
      }
      
      if (data.topRegionSales) {
        this.updateTopRegionSalesChart(data.topRegionSales);
      }
      
      if (data.topRegionGrowth) {
        this.updateTopRegionGrowthChart(data.topRegionGrowth);
      }
      
      if (data.regionAOV) {
        this.updateRegionAOVChart(data.regionAOV);
      }
      
      if (data.seasonalityData) {
        this.updateSeasonalityChart(data.seasonalityData);
      }
    },
    loadMockData() {
      // 省份数据，包含各省销售额
      const provinces = [
        { name: '北京', value: 45000, growthRate: 14.5 },
        { name: '天津', value: 22500, growthRate: 10.2 },
        { name: '河北', value: 28700, growthRate: 5.8 },
        { name: '山西', value: 18400, growthRate: 6.3 },
        { name: '内蒙古', value: 15200, growthRate: 9.1 },
        { name: '辽宁', value: 26400, growthRate: 4.7 },
        { name: '吉林', value: 19800, growthRate: 3.5 },
        { name: '黑龙江', value: 21500, growthRate: 2.8 },
        { name: '上海', value: 47800, growthRate: 15.3 },
        { name: '江苏', value: 43600, growthRate: 12.8 },
        { name: '浙江', value: 41200, growthRate: 13.6 },
        { name: '安徽', value: 25300, growthRate: 8.5 },
        { name: '福建', value: 29700, growthRate: 9.8 },
        { name: '江西', value: 22100, growthRate: 7.6 },
        { name: '山东', value: 38400, growthRate: 10.5 },
        { name: '河南', value: 32500, growthRate: 8.3 },
        { name: '湖北', value: 28900, growthRate: 9.5 },
        { name: '湖南', value: 27600, growthRate: 8.7 },
        { name: '广东', value: 49500, growthRate: 16.2 },
        { name: '广西', value: 23400, growthRate: 7.4 },
        { name: '海南', value: 18700, growthRate: 11.2 },
        { name: '重庆', value: 27800, growthRate: 9.9 },
        { name: '四川', value: 35600, growthRate: 8.9 },
        { name: '贵州', value: 19500, growthRate: 6.8 },
        { name: '云南', value: 22900, growthRate: 7.2 },
        { name: '西藏', value: 8500, growthRate: 15.8 },
        { name: '陕西', value: 24700, growthRate: 8.2 },
        { name: '甘肃', value: 14600, growthRate: 5.1 },
        { name: '青海', value: 9800, growthRate: 6.7 },
        { name: '宁夏', value: 10500, growthRate: 7.5 },
        { name: '新疆', value: 16800, growthRate: 9.3 }
      ];
      
      // 地区分组
      const regionGroups = {
        '华东': ['上海', '江苏', '浙江', '安徽', '福建', '江西', '山东'],
        '华南': ['广东', '广西', '海南'],
        '华北': ['北京', '天津', '河北', '山西', '内蒙古'],
        '华中': ['河南', '湖北', '湖南'],
        '西南': ['重庆', '四川', '贵州', '云南', '西藏'],
        '东北': ['辽宁', '吉林', '黑龙江'],
        '西北': ['陕西', '甘肃', '青海', '宁夏', '新疆']
      };
      
      // 更新中国地图数据
      this.mapChart.series[0].data = provinces;
      
      // 更新TOP 10 地区销售额图表
      const topSalesProvinces = [...provinces].sort((a, b) => b.value - a.value).slice(0, 10);
      this.topRegionSalesChart.yAxis.data = topSalesProvinces.map(p => p.name).reverse();
      this.topRegionSalesChart.series[0].data = topSalesProvinces.map(p => p.value).reverse();
      
      // 更新TOP 10 地区销售增长率图表
      const topGrowthProvinces = [...provinces].sort((a, b) => b.growthRate - a.growthRate).slice(0, 10);
      this.topRegionGrowthChart.yAxis.data = topGrowthProvinces.map(p => p.name).reverse();
      this.topRegionGrowthChart.series[0].data = topGrowthProvinces.map(p => ({
        value: p.growthRate,
        sales: p.value
      })).reverse();
      
      // 更新地区客单价对比图表
      const regions = Object.keys(regionGroups);
      const regionAOV = [];
      const regionAOVGrowth = [];
      
      regions.forEach(region => {
        const regionProvinces = regionGroups[region];
        let totalSales = 0;
        regionProvinces.forEach(province => {
          const provinceData = provinces.find(p => p.name === province);
          if (provinceData) {
            totalSales += provinceData.value;
          }
        });
        const aov = Math.round(100 + Math.random() * 200);
        regionAOV.push(aov);
        regionAOVGrowth.push(Math.round(Math.random() * 20 - 5));
      });
      
      this.regionAOVChart.xAxis.data = regions;
      this.regionAOVChart.series[0].data = regionAOV;
      this.regionAOVChart.series[1].data = regionAOVGrowth;
      
      // 更新地区销售季节性分析图表
      const months = ['1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月'];
      const seasonalityData = [];
      
      regions.forEach((region, index) => {
        const monthlyData = [];
        
        // 生成该地区每月的销售数据
        for (let i = 0; i < 12; i++) {
          // 不同地区的季节性模式稍有不同
          let seasonFactor;
          
          if (region === '华南' || region === '华东') {
            // 夏季较高，冬季较低
            seasonFactor = (i >= 5 && i <= 8) ? 1.3 : ((i >= 0 && i <= 1) || (i >= 10 && i <= 11)) ? 0.8 : 1;
          } else if (region === '华北' || region === '东北') {
            // 冬季较高，夏季适中
            seasonFactor = (i >= 10 || i <= 1) ? 1.2 : (i >= 6 && i <= 8) ? 0.9 : 1;
          } else {
            // 其他地区相对平稳
            seasonFactor = 0.9 + Math.sin(i / 12 * Math.PI * 2) * 0.3;
          }
          
          // 基础销售额 + 季节因子 + 随机波动
          const baseSales = 20000 + index * 2000;
          const sales = Math.round(baseSales * seasonFactor * (0.9 + Math.random() * 0.2));
          monthlyData.push(sales);
        }
        
        this.seasonalityChart.series[index].data = monthlyData;
      });
      
      // 生成地区销售明细数据
      const tableData = [];
      
      // 先添加七大区域的汇总数据
      regions.forEach((region, index) => {
        const regionProvinces = regionGroups[region];
        let totalSales = 0;
        let totalGrowth = 0;
        
        regionProvinces.forEach(province => {
          const provinceData = provinces.find(p => p.name === province);
          if (provinceData) {
            totalSales += provinceData.value;
            totalGrowth += provinceData.growthRate;
          }
        });
        
        const avgGrowth = Math.round(totalGrowth / regionProvinces.length * 10) / 10;
        const orders = Math.round(totalSales / regionAOV[index]);
        const customers = Math.round(orders * 0.7); // 假设平均一个客户下1.4个订单
        
        tableData.push({
          regionName: region,
          sales: totalSales,
          orders,
          avgOrderValue: regionAOV[index],
          salesDensity: Math.round(totalSales / regionProvinces.length),
          growthRate: avgGrowth,
          customers,
          penetrationRate: Math.round(customers / (1000 * regionProvinces.length) * 100) / 10,
          key: region
        });
      });
      
      // 然后添加各省份数据
      provinces.forEach((province, index) => {
        // 找出该省份所属的大区
        let regionName = '';
        for (const [region, provinceList] of Object.entries(regionGroups)) {
          if (provinceList.includes(province.name)) {
            regionName = region;
            break;
          }
        }
        
        const orders = Math.round(province.value / (100 + Math.random() * 100));
        const avgOrderValue = Math.round(province.value / orders);
        const customers = Math.round(orders * 0.7);
        
        tableData.push({
          regionName: province.name,
          sales: province.value,
          orders,
          avgOrderValue,
          salesDensity: Math.round(province.value / 10),
          growthRate: province.growthRate,
          customers,
          penetrationRate: Math.round(customers / 1000 * 100) / 10,
          key: `province-${index}`
        });
      });
      
      // 更新表格数据
      if (this.queryParams.regionLevel === 'province') {
        this.tableData = tableData.filter(item => !regions.includes(item.regionName));
      } else {
        this.tableData = tableData.filter(item => regions.includes(item.regionName));
      }
      this.pagination.total = this.tableData.length;
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
        regionLevel: 'province'
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
.region-sales {
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

.map-chart {
  height: 600px;
  width: 100%;
}

.metric-chart {
  height: 400px;
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