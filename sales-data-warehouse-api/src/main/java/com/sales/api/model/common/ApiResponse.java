package com.sales.api.model.common;

import lombok.Data;

import java.io.Serializable;

/**
 * API统一响应格式
 *
 * @param <T> 响应数据类型
 */
@Data
public class ApiResponse<T> implements Serializable {
    private static final long serialVersionUID = 1L;
    
    /**
     * 状态码
     */
    private Integer code;
    
    /**
     * 消息
     */
    private String message;
    
    /**
     * 数据
     */
    private T data;
    
    /**
     * 是否成功
     */
    private Boolean success;
    
    /**
     * 时间戳
     */
    private Long timestamp;
    
    private ApiResponse() {
        this.timestamp = System.currentTimeMillis();
    }
    
    /**
     * 成功响应
     *
     * @param <T> 数据类型
     * @return ApiResponse
     */
    public static <T> ApiResponse<T> success() {
        return success(null);
    }
    
    /**
     * 成功响应
     *
     * @param data 响应数据
     * @param <T>  数据类型
     * @return ApiResponse
     */
    public static <T> ApiResponse<T> success(T data) {
        return success(data, "操作成功");
    }
    
    /**
     * 成功响应
     *
     * @param data    响应数据
     * @param message 消息
     * @param <T>     数据类型
     * @return ApiResponse
     */
    public static <T> ApiResponse<T> success(T data, String message) {
        ApiResponse<T> response = new ApiResponse<>();
        response.setCode(200);
        response.setMessage(message);
        response.setData(data);
        response.setSuccess(true);
        return response;
    }
    
    /**
     * 失败响应
     *
     * @param <T> 数据类型
     * @return ApiResponse
     */
    public static <T> ApiResponse<T> failure() {
        return failure("操作失败");
    }
    
    /**
     * 失败响应
     *
     * @param message 消息
     * @param <T>     数据类型
     * @return ApiResponse
     */
    public static <T> ApiResponse<T> failure(String message) {
        return failure(message, 500);
    }
    
    /**
     * 失败响应
     *
     * @param message 消息
     * @param code    状态码
     * @param <T>     数据类型
     * @return ApiResponse
     */
    public static <T> ApiResponse<T> failure(String message, Integer code) {
        ApiResponse<T> response = new ApiResponse<>();
        response.setCode(code);
        response.setMessage(message);
        response.setData(null);
        response.setSuccess(false);
        return response;
    }
} 