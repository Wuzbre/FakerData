package com.sales.api.model.analysis;

import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

/**
 * 销售趋势数据模型
 */
@Data
public class SalesTrendVO implements Serializable {
    private static final long serialVersionUID = 1L;
    
    /**
     * 日期（yyyy-MM-dd）
     */
    private String orderDay;
    
    /**
     * 月份（yyyy-MM）
     */
    private String orderMonth;
    
    /**
     * 周（yyyy-W）
     */
    private String orderWeek;
    
    /**
     * 订单数量
     */
    private Integer orderCount;
    
    /**
     * 销售金额
     */
    private BigDecimal salesAmount;
    
    /**
     * 客户数量
     */
    private Integer customerCount;
} 