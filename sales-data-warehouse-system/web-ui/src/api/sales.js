import request from '@/utils/request'

/**
 * 获取销售趋势数据
 * @param {Object} params
 * @param {string} params.timeDimension 时间维度: day, week, month, year
 * @param {string} params.startDate 开始日期
 * @param {string} params.endDate 结束日期
 * @returns {Promise}
 */
export function getSalesTrend(params) {
  return request({
    url: '/sales/analysis/trend',
    method: 'get',
    params
  })
}

/**
 * 获取热销产品数据
 * @param {Object} params
 * @param {number} params.topN 前N条记录
 * @param {string} params.category 产品类别
 * @param {string} params.startDate 开始日期
 * @param {string} params.endDate 结束日期
 * @returns {Promise}
 */
export function getTopProducts(params) {
  return request({
    url: '/sales/analysis/products/top',
    method: 'get',
    params
  })
}

/**
 * 获取销售地域分布数据
 * @param {Object} params
 * @param {string} params.startDate 开始日期
 * @param {string} params.endDate 结束日期
 * @returns {Promise}
 */
export function getRegionSales(params) {
  return request({
    url: '/sales/analysis/region',
    method: 'get',
    params
  })
}

/**
 * 获取客户RFM分析数据
 * @param {Object} params
 * @param {string} params.startDate 开始日期
 * @param {string} params.endDate 结束日期
 * @returns {Promise}
 */
export function getCustomerRFM(params) {
  return request({
    url: '/sales/analysis/customer/rfm',
    method: 'get',
    params
  })
}

/**
 * 获取销售仪表盘概览数据
 * @param {Object} params
 * @param {string} params.date 日期
 * @returns {Promise}
 */
export function getSalesDashboard(params) {
  return request({
    url: '/sales/analysis/dashboard',
    method: 'get',
    params
  })
} 