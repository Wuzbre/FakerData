# API服务模块

## 概述

API服务模块是整个购销数据仓库系统的后端服务层，基于Spring Boot框架开发，提供RESTful API接口，用于连接前端UI与数据仓库。该模块负责从Doris数据库中查询汇总数据，处理业务逻辑，并将结果以JSON格式返回给前端展示。

## 功能特点

- 基于Spring Boot的REST API架构
- 完整的业务指标查询和分析功能
- 灵活的数据过滤和聚合能力
- 支持多种时间粒度的数据统计
- 内置缓存机制提高查询性能
- 完善的权限控制和认证机制
- 详细的API文档和调试工具

## 目录结构

```
api-service/
├── pom.xml                     # Maven配置
└── src/                        # 源代码
    └── main/
        ├── java/               # Java代码
        │   └── com/sales/api/
        │       ├── SalesApiApplication.java  # 应用入口
        │       ├── controller/  # API控制器
        │       │   ├── SalesController.java
        │       │   ├── InventoryController.java
        │       │   └── UserController.java
        │       ├── service/     # 业务逻辑
        │       │   ├── SalesService.java
        │       │   ├── InventoryService.java
        │       │   └── UserService.java
        │       ├── model/       # 数据模型
        │       │   ├── SalesSummary.java
        │       │   ├── ProductSales.java
        │       │   └── InventoryStatus.java
        │       ├── repository/  # 数据访问
        │       │   ├── SalesRepository.java
        │       │   └── InventoryRepository.java
        │       ├── config/      # 配置类
        │       │   ├── DorisConfig.java
        │       │   ├── CacheConfig.java
        │       │   └── SecurityConfig.java
        │       ├── exception/   # 异常处理
        │       │   ├── ApiException.java
        │       │   └── GlobalExceptionHandler.java
        │       └── util/        # 工具类
        │           ├── DateUtils.java
        │           └── ResponseUtils.java
        └── resources/          # 资源文件
            ├── application.yml  # 应用配置
            ├── application-dev.yml  # 开发环境配置
            ├── application-prod.yml # 生产环境配置
            └── logback.xml      # 日志配置
```

## 环境要求

- JDK 1.8+
- Maven 3.6+
- Doris 1.0+（用于数据查询）
- MySQL 5.7+（用于存储系统配置）

## 构建与部署

### 构建项目

```bash
cd api-service
mvn clean package -DskipTests
```

成功构建后，将在`target`目录下生成`sales-data-warehouse-api-1.0.0.jar`文件。

### 配置文件

主要配置文件`application.yml`包含以下重要配置：

```yaml
server:
  port: 8080
  servlet:
    context-path: /api

spring:
  application:
    name: sales-data-warehouse-api
  datasource:
    doris:
      driver-class-name: com.mysql.cj.jdbc.Driver
      url: jdbc:mysql://localhost:9030/sales_ads
      username: root
      password: password
    mysql:
      driver-class-name: com.mysql.cj.jdbc.Driver
      url: jdbc:mysql://localhost:3306/sales_config
      username: root
      password: password
  cache:
    type: caffeine
    caffeine:
      spec: maximumSize=500,expireAfterWrite=300s

logging:
  level:
    root: INFO
    com.sales.api: DEBUG
  file:
    name: logs/api-service.log

api:
  security:
    enabled: true
    jwt:
      secret: your-secret-key
      expiration: 86400000
  cache:
    enabled: true
    default-ttl: 300
  cors:
    allowed-origins: "*"
    allowed-methods: GET,POST,PUT,DELETE
```

根据部署环境修改相关配置参数。

## API接口文档

### 用户相关接口

#### 用户认证

```
POST /api/auth/login
```

请求体：
```json
{
  "username": "admin",
  "password": "password"
}
```

响应：
```json
{
  "token": "eyJhbGciOiJIUzI1NiJ9...",
  "expires": 86400000,
  "user": {
    "id": 1,
    "username": "admin",
    "role": "ADMIN"
  }
}
```

### 销售分析接口

#### 销售概览

```
GET /api/sales/summary
```

参数：
- `startDate`: 开始日期 (yyyy-MM-dd)
- `endDate`: 结束日期 (yyyy-MM-dd)
- `granularity`: 时间粒度 (day/week/month/quarter/year)

响应：
```json
{
  "totalSales": 1256789.50,
  "orderCount": 3456,
  "avgOrderValue": 363.65,
  "timeline": [
    {
      "period": "2023-01",
      "sales": 425678.25,
      "orders": 1200
    },
    {
      "period": "2023-02",
      "sales": 389654.75,
      "orders": 1100
    },
    {
      "period": "2023-03",
      "sales": 441456.50,
      "orders": 1156
    }
  ]
}
```

#### 产品销售分析

```
GET /api/sales/products
```

参数：
- `startDate`: 开始日期 (yyyy-MM-dd)
- `endDate`: 结束日期 (yyyy-MM-dd)
- `category`: 产品类别 (可选)
- `limit`: 返回结果数量限制 (默认10)

响应：
```json
{
  "products": [
    {
      "productId": "P001",
      "productName": "智能手机A",
      "category": "电子产品",
      "sales": 356789.50,
      "quantity": 890,
      "avgPrice": 400.89
    },
    {
      "productId": "P002",
      "productName": "笔记本电脑B",
      "category": "电子产品",
      "sales": 289456.75,
      "quantity": 234,
      "avgPrice": 1237.85
    }
  ],
  "categorySummary": [
    {
      "category": "电子产品",
      "sales": 845678.50,
      "percentage": 67.3
    },
    {
      "category": "服装",
      "sales": 256789.25,
      "percentage": 20.4
    }
  ]
}
```

#### 区域销售分析

```
GET /api/sales/regions
```

参数：
- `startDate`: 开始日期 (yyyy-MM-dd)
- `endDate`: 结束日期 (yyyy-MM-dd)

响应：
```json
{
  "regions": [
    {
      "region": "华东",
      "sales": 456789.50,
      "orderCount": 1234,
      "percentage": 36.3
    },
    {
      "region": "华北",
      "sales": 356789.25,
      "orderCount": 987,
      "percentage": 28.4
    }
  ]
}
```

### 库存分析接口

#### 库存概览

```
GET /api/inventory/summary
```

参数：
- `date`: 日期 (yyyy-MM-dd, 默认当前日期)

响应：
```json
{
  "totalProducts": 250,
  "totalValue": 1256789.50,
  "lowStockCount": 15,
  "overStockCount": 8,
  "categorySummary": [
    {
      "category": "电子产品",
      "stockValue": 756789.50,
      "stockCount": 95,
      "percentage": 60.2
    },
    {
      "category": "服装",
      "stockValue": 245678.25,
      "stockCount": 120,
      "percentage": 19.5
    }
  ]
}
```

#### 库存周转率分析

```
GET /api/inventory/turnover
```

参数：
- `startDate`: 开始日期 (yyyy-MM-dd)
- `endDate`: 结束日期 (yyyy-MM-dd)
- `category`: 产品类别 (可选)

响应：
```json
{
  "overall": {
    "turnoverRate": 5.6,
    "avgDaysInStock": 65.2
  },
  "categories": [
    {
      "category": "电子产品",
      "turnoverRate": 7.8,
      "avgDaysInStock": 46.8
    },
    {
      "category": "服装",
      "turnoverRate": 6.2,
      "avgDaysInStock": 58.9
    }
  ],
  "products": [
    {
      "productId": "P001",
      "productName": "智能手机A",
      "turnoverRate": 12.5,
      "avgDaysInStock": 29.2
    }
  ]
}
```

## 安全与认证

API服务采用JWT (JSON Web Token) 认证机制，保护敏感API端点。认证流程如下：

1. 用户通过 `/api/auth/login` 端点提交用户名和密码
2. 服务验证凭据并返回JWT令牌
3. 客户端在后续请求中通过Authorization头部传递令牌
4. 服务验证令牌并授权访问受保护资源

示例请求头：
```
Authorization: Bearer eyJhbGciOiJIUzI1NiJ9...
```

## 缓存机制

API服务使用多级缓存策略提高性能：

1. **内存缓存**: 使用Caffeine缓存频繁访问的数据
2. **查询结果缓存**: 缓存复杂查询的结果，避免重复计算
3. **自定义缓存键**: 基于请求参数生成缓存键

可通过配置文件调整缓存策略：

```yaml
api:
  cache:
    enabled: true
    default-ttl: 300  # 秒
    sales-summary-ttl: 600  # 秒
    inventory-summary-ttl: 1800  # 秒
```

## 启动服务

### 开发环境

```bash
java -jar -Dspring.profiles.active=dev target/sales-data-warehouse-api-1.0.0.jar
```

### 生产环境

```bash
java -jar -Dspring.profiles.active=prod target/sales-data-warehouse-api-1.0.0.jar
```

## 监控与管理

API服务集成了Spring Boot Actuator，提供运行时监控和管理功能：

```
GET /api/actuator/health  # 健康检查
GET /api/actuator/info    # 应用信息
GET /api/actuator/metrics # 性能指标
```

## 扩展与定制

要添加新的API端点，遵循以下步骤：

1. 在`model`包中创建数据模型类
2. 在`repository`包中添加数据访问方法
3. 在`service`包中实现业务逻辑
4. 在`controller`包中创建REST控制器

示例控制器:

```java
@RestController
@RequestMapping("/api/sales")
public class CategorySalesController {
    
    private final CategorySalesService service;
    
    @Autowired
    public CategorySalesController(CategorySalesService service) {
        this.service = service;
    }
    
    @GetMapping("/categories")
    public ResponseEntity<CategorySalesResponse> getCategorySales(
            @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate startDate,
            @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate endDate) {
        
        CategorySalesResponse response = service.getCategorySales(startDate, endDate);
        return ResponseEntity.ok(response);
    }
}
```

## 错误处理

API服务使用统一的异常处理机制，返回标准化的错误响应：

```json
{
  "status": 400,
  "error": "Bad Request",
  "message": "无效的日期格式",
  "timestamp": "2023-12-31T23:59:59.999+00:00",
  "path": "/api/sales/summary"
}
```

常见HTTP状态码：
- 200: 请求成功
- 400: 无效的请求参数
- 401: 未授权访问
- 403: 权限不足
- 404: 资源不存在
- 500: 服务器内部错误

## 性能优化建议

1. 增加应用内存: `-Xmx2g -Xms2g`
2. 调整数据库连接池参数
3. 根据查询模式优化Doris表设计
4. 启用响应压缩
5. 实施合理的缓存策略 