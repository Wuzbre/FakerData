-- 为ODS层表添加分区的脚本
-- 负责在数据加载后为分区表添加分区

-- 定义处理日期变量（从命令行传入）
-- 使用示例: hive -hivevar dt=20240101 -f add_ods_partitions.hql
-- ${hivevar:dt} 为传入的日期分区，格式为yyyyMMdd

USE purchase_sales_ods;

-- 为users表添加分区
ALTER TABLE ods_users ADD IF NOT EXISTS PARTITION (dt='${hivevar:dt}')
LOCATION '/user/hive/warehouse/purchase_sales_ods.db/ods_users/dt=${hivevar:dt}';

-- 为employees表添加分区
ALTER TABLE ods_employees ADD IF NOT EXISTS PARTITION (dt='${hivevar:dt}')
LOCATION '/user/hive/warehouse/purchase_sales_ods.db/ods_employees/dt=${hivevar:dt}';

-- 为products表添加分区
ALTER TABLE ods_products ADD IF NOT EXISTS PARTITION (dt='${hivevar:dt}')
LOCATION '/user/hive/warehouse/purchase_sales_ods.db/ods_products/dt=${hivevar:dt}';

-- 为sales_orders表添加分区
ALTER TABLE ods_sales_orders ADD IF NOT EXISTS PARTITION (dt='${hivevar:dt}')
LOCATION '/user/hive/warehouse/purchase_sales_ods.db/ods_sales_orders/dt=${hivevar:dt}';

-- 为sales_order_items表添加分区
ALTER TABLE ods_sales_order_items ADD IF NOT EXISTS PARTITION (dt='${hivevar:dt}')
LOCATION '/user/hive/warehouse/purchase_sales_ods.db/ods_sales_order_items/dt=${hivevar:dt}';

-- 为purchase_orders表添加分区
ALTER TABLE ods_purchase_orders ADD IF NOT EXISTS PARTITION (dt='${hivevar:dt}')
LOCATION '/user/hive/warehouse/purchase_sales_ods.db/ods_purchase_orders/dt=${hivevar:dt}';

-- 为purchase_order_items表添加分区
ALTER TABLE ods_purchase_order_items ADD IF NOT EXISTS PARTITION (dt='${hivevar:dt}')
LOCATION '/user/hive/warehouse/purchase_sales_ods.db/ods_purchase_order_items/dt=${hivevar:dt}';

-- 为suppliers表添加分区（如果存在）
ALTER TABLE ods_suppliers ADD IF NOT EXISTS PARTITION (dt='${hivevar:dt}')
LOCATION '/user/hive/warehouse/purchase_sales_ods.db/ods_suppliers/dt=${hivevar:dt}';

-- 打印分区添加完成信息
SELECT 'ODS层分区添加完成，处理日期: ${hivevar:dt}' AS status; 