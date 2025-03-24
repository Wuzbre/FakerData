package com.sales.api.model.analysis;

import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 产品销售数据模型
 */
@Data
public class ProductSalesVO implements Serializable {
    private static final long serialVersionUID = 1L;
    
    /**
     * 产品ID
     */
    private Long productId;
    
    /**
     * 产品名称
     */
    private String productName;
    
    /**
     * 产品类别
     */
    private String category;
    
    /**
     * 销售数量
     */
    private Integer totalQuantity;
    
    /**
     * 销售金额
     */
    private BigDecimal totalAmount;
    
    /**
     * 订单数量
     */
    private Integer orderCount;
    
    /**
     * 平均价格
     */
    private BigDecimal avgPrice;
} 