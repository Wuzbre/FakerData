package com.sales.api.controller;

import com.sales.api.model.analysis.ProductSalesVO;
import com.sales.api.model.analysis.SalesTrendVO;
import com.sales.api.model.common.ApiResponse;
import com.sales.api.service.SalesAnalysisService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 销售分析控制器
 */
@Slf4j
@RestController
@RequestMapping("/sales/analysis")
@Api(tags = "销售分析接口")
public class SalesAnalysisController {
    
    @Autowired
    private SalesAnalysisService salesAnalysisService;
    
    /**
     * 获取销售趋势数据
     */
    @GetMapping("/trend")
    @ApiOperation("获取销售趋势数据")
    public ApiResponse<List<SalesTrendVO>> getSalesTrend(
            @ApiParam(value = "时间维度", defaultValue = "day", required = true)
            @RequestParam(value = "timeDimension", defaultValue = "day") String timeDimension,
            @ApiParam(value = "开始日期", example = "2023-01-01")
            @RequestParam(value = "startDate", required = false) String startDate,
            @ApiParam(value = "结束日期", example = "2023-12-31")
            @RequestParam(value = "endDate", required = false) String endDate) {
        
        try {
            List<SalesTrendVO> result = salesAnalysisService.getSalesTrend(timeDimension, startDate, endDate);
            return ApiResponse.success(result, "获取销售趋势数据成功");
        } catch (Exception e) {
            log.error("获取销售趋势数据失败", e);
            return ApiResponse.failure("获取销售趋势数据失败: " + e.getMessage());
        }
    }
    
    /**
     * 获取热销产品数据
     */
    @GetMapping("/products/top")
    @ApiOperation("获取热销产品数据")
    public ApiResponse<List<ProductSalesVO>> getTopProducts(
            @ApiParam(value = "Top N", defaultValue = "10", required = true)
            @RequestParam(value = "topN", defaultValue = "10") int topN,
            @ApiParam(value = "产品类别")
            @RequestParam(value = "category", required = false) String category,
            @ApiParam(value = "开始日期", example = "2023-01-01")
            @RequestParam(value = "startDate", required = false) String startDate,
            @ApiParam(value = "结束日期", example = "2023-12-31")
            @RequestParam(value = "endDate", required = false) String endDate) {
        
        try {
            List<ProductSalesVO> result = salesAnalysisService.getTopProducts(topN, category, startDate, endDate);
            return ApiResponse.success(result, "获取热销产品数据成功");
        } catch (Exception e) {
            log.error("获取热销产品数据失败", e);
            return ApiResponse.failure("获取热销产品数据失败: " + e.getMessage());
        }
    }
    
    /**
     * 获取销售地域分布数据
     */
    @GetMapping("/region")
    @ApiOperation("获取销售地域分布数据")
    public ApiResponse<Map<String, Object>> getRegionSales(
            @ApiParam(value = "开始日期", example = "2023-01-01")
            @RequestParam(value = "startDate", required = false) String startDate,
            @ApiParam(value = "结束日期", example = "2023-12-31")
            @RequestParam(value = "endDate", required = false) String endDate) {
        
        try {
            Map<String, Object> result = salesAnalysisService.getRegionSales(startDate, endDate);
            return ApiResponse.success(result, "获取销售地域分布数据成功");
        } catch (Exception e) {
            log.error("获取销售地域分布数据失败", e);
            return ApiResponse.failure("获取销售地域分布数据失败: " + e.getMessage());
        }
    }
    
    /**
     * 获取客户RFM分析数据
     */
    @GetMapping("/customer/rfm")
    @ApiOperation("获取客户RFM分析数据")
    public ApiResponse<Map<String, Object>> getCustomerRFM(
            @ApiParam(value = "开始日期", example = "2023-01-01")
            @RequestParam(value = "startDate", required = false) String startDate,
            @ApiParam(value = "结束日期", example = "2023-12-31")
            @RequestParam(value = "endDate", required = false) String endDate) {
        
        try {
            Map<String, Object> result = salesAnalysisService.getCustomerRFM(startDate, endDate);
            return ApiResponse.success(result, "获取客户RFM分析数据成功");
        } catch (Exception e) {
            log.error("获取客户RFM分析数据失败", e);
            return ApiResponse.failure("获取客户RFM分析数据失败: " + e.getMessage());
        }
    }
    
    /**
     * 获取销售仪表盘概览数据
     */
    @GetMapping("/dashboard")
    @ApiOperation("获取销售仪表盘概览数据")
    public ApiResponse<Map<String, Object>> getSalesDashboard(
            @ApiParam(value = "日期", example = "2023-01-01")
            @RequestParam(value = "date", required = false) String date) {
        
        try {
            Map<String, Object> result = salesAnalysisService.getSalesDashboard(date);
            return ApiResponse.success(result, "获取销售仪表盘概览数据成功");
        } catch (Exception e) {
            log.error("获取销售仪表盘概览数据失败", e);
            return ApiResponse.failure("获取销售仪表盘概览数据失败: " + e.getMessage());
        }
    }
} 