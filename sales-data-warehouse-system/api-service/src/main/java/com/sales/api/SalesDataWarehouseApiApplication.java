package com.sales.api;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import springfox.documentation.oas.annotations.EnableOpenApi;

/**
 * 销售数据仓库API服务主应用类
 */
@SpringBootApplication
@EnableOpenApi
@MapperScan("com.sales.api.repository")
public class SalesDataWarehouseApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(SalesDataWarehouseApiApplication.class, args);
    }
} 