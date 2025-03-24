package com.sales.api.service;

import com.sales.api.model.analysis.ProductSalesVO;
import com.sales.api.model.analysis.SalesTrendVO;

import java.util.List;
import java.util.Map;

/**
 * 销售分析服务接口
 */
public interface SalesAnalysisService {
    
    /**
     * 获取销售趋势数据
     *
     * @param timeDimension 时间维度: day, week, month, year
     * @param startDate     开始日期
     * @param endDate       结束日期
     * @return 销售趋势数据列表
     */
    List<SalesTrendVO> getSalesTrend(String timeDimension, String startDate, String endDate);
    
    /**
     * 获取热销产品数据
     *
     * @param topN      Top N产品
     * @param category  产品类别(可选)
     * @param startDate 开始日期
     * @param endDate   结束日期
     * @return 热销产品数据列表
     */
    List<ProductSalesVO> getTopProducts(int topN, String category, String startDate, String endDate);
    
    /**
     * 获取销售地域分布数据
     *
     * @param startDate 开始日期
     * @param endDate   结束日期
     * @return 地域销售数据
     */
    Map<String, Object> getRegionSales(String startDate, String endDate);
    
    /**
     * 获取客户RFM分析数据
     *
     * @param startDate 开始日期
     * @param endDate   结束日期
     * @return 客户RFM分析数据
     */
    Map<String, Object> getCustomerRFM(String startDate, String endDate);
    
    /**
     * 获取销售仪表盘概览数据
     *
     * @param date 日期
     * @return 仪表盘数据
     */
    Map<String, Object> getSalesDashboard(String date);
} 