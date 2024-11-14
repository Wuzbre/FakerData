import csv
import random
from faker import Faker
from datetime import datetime, timedelta

# 初始化 Faker 并设置中文
fake = Faker('zh_CN')

# 定义数据生成量
NUM_CUSTOMERS = 100
NUM_SUPPLIERS = 50
NUM_PRODUCTS = 200
NUM_PURCHASE_ORDERS = 1000
NUM_SALES_ORDERS = 1000
NUM_STOCK_IN_RECORDS = 500
NUM_STOCK_OUT_RECORDS = 500
NUM_REVENUES = 800
NUM_EXPENSES = 800
NUM_LOGS = 1000


# 用于生成唯一ID
def generate_id(prefix):
    return f"{prefix}_{fake.uuid4()}"


# 生成客户和供应商基本信息
def generate_customers_suppliers():
    customers = []
    suppliers = []
    for _ in range(NUM_CUSTOMERS):
        customer = {
            "entity_id": generate_id("customer"),
            "entity_type": "客户",
            "name": fake.company(),
            "contact_info": fake.phone_number(),
            "address": fake.address(),
            "region": fake.province(),
            "credit_rating": random.choice(["A", "B", "C"]),
            "total_orders": random.randint(1, 100),
            "total_spend": round(random.uniform(5000, 100000), 2),
            "account_manager": fake.name(),
            "registration_date": fake.date_this_decade(),
            "industry": random.choice(["制造业", "零售业", "服务业"]),
            "annual_revenue": round(random.uniform(100000, 1000000), 2),
            "payment_terms": random.choice(["30天", "60天", "90天"])
        }
        customers.append(customer)

    for _ in range(NUM_SUPPLIERS):
        supplier = {
            "entity_id": generate_id("supplier"),
            "entity_type": "供应商",
            "name": fake.company(),
            "contact_info": fake.phone_number(),
            "address": fake.address(),
            "region": fake.province(),
            "credit_rating": random.choice(["A", "B", "C"]),
            "total_orders": random.randint(1, 100),
            "total_spend": round(random.uniform(5000, 100000), 2),
            "account_manager": fake.name(),
            "registration_date": fake.date_this_decade(),
            "industry": random.choice(["制造业", "零售业", "服务业"]),
            "annual_revenue": round(random.uniform(100000, 1000000), 2),
            "payment_terms": random.choice(["30天", "60天", "90天"])
        }
        suppliers.append(supplier)

    return customers, suppliers


# 生成商品信息
def generate_products():
    products = []
    for _ in range(NUM_PRODUCTS):
        product = {
            "product_id": generate_id("product"),
            "name": fake.word(),
            "category": random.choice(["电子产品", "服装", "食品", "家具"]),
            "price": round(random.uniform(10, 1000), 2),
            "supplier_id": generate_id("supplier")  # 随机关联供应商ID
        }
        products.append(product)
    return products


# 生成采购订单
def generate_purchase_orders(customers, suppliers, products):
    purchase_orders = []
    for _ in range(NUM_PURCHASE_ORDERS):
        order = {
            "purchase_order_id": generate_id("po"),
            "supplier_id": random.choice(suppliers)["entity_id"],
            "product_id": random.choice(products)["product_id"],
            "order_date": fake.date_this_year(),
            "expected_delivery_date": fake.date_this_year(),
            "actual_delivery_date": fake.date_this_year(),
            "quantity": random.randint(1, 100),
            "unit_price": round(random.uniform(5, 500), 2),
            "total_amount": round(random.uniform(500, 50000), 2),
            "currency": "CNY",
            "payment_terms": random.choice(["30天", "60天", "90天"]),
            "payment_status": random.choice(["未支付", "已支付"]),
            "order_status": random.choice(["待发货", "已完成"]),
            "created_by": fake.name(),
            "created_at": fake.date_time_this_year(),
            "approved_by": fake.name(),
            "approved_at": fake.date_time_this_year(),
            "warehouse_id": generate_id("warehouse"),
            "delivery_address": fake.address(),
            "contact_person": fake.name(),
            "contact_phone": fake.phone_number(),
            "remarks": fake.sentence(),
            "tax_rate": round(random.uniform(0.05, 0.2), 2),
            "tax_amount": round(random.uniform(5, 500), 2)
        }
        purchase_orders.append(order)
    return purchase_orders


# 生成销售订单
def generate_sales_orders(customers, products):
    sales_orders = []
    for _ in range(NUM_SALES_ORDERS):
        order = {
            "sales_order_id": generate_id("so"),
            "customer_id": random.choice(customers)["entity_id"],
            "product_id": random.choice(products)["product_id"],
            "order_date": fake.date_this_year(),
            "expected_shipping_date": fake.date_this_year(),
            "actual_shipping_date": fake.date_this_year(),
            "quantity": random.randint(1, 100),
            "unit_price": round(random.uniform(10, 1000), 2),
            "discount": round(random.uniform(0, 100), 2),
            "total_amount": round(random.uniform(100, 10000), 2),
            "currency": "CNY",
            "payment_terms": random.choice(["30天", "60天", "90天"]),
            "payment_status": random.choice(["未支付", "已支付"]),
            "order_status": random.choice(["待发货", "已完成"]),
            "sales_channel": random.choice(["线上", "线下"]),
            "created_by": fake.name(),
            "created_at": fake.date_time_this_year(),
            "approved_by": fake.name(),
            "approved_at": fake.date_time_this_year(),
            "shipping_address": fake.address(),
            "contact_person": fake.name(),
            "contact_phone": fake.phone_number(),
            "shipment_id": generate_id("shipment"),
            "shipping_company": fake.company(),
            "tax_rate": round(random.uniform(0.05, 0.2), 2),
            "tax_amount": round(random.uniform(10, 500), 2)
        }
        sales_orders.append(order)
    return sales_orders


# 保存数据到CSV文件
def save_to_csv(data, filename):
    with open(filename, mode='w', encoding='utf-8-sig', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)


# 主函数，生成数据并保存到文件
def main():
    # 生成数据
    customers, suppliers = generate_customers_suppliers()
    products = generate_products()
    purchase_orders = generate_purchase_orders(customers, suppliers, products)
    sales_orders = generate_sales_orders(customers, products)

    # 保存数据到文件
    save_to_csv(customers, "customers.csv")
    save_to_csv(suppliers, "suppliers.csv")
    save_to_csv(products, "products.csv")
    save_to_csv(purchase_orders, "purchase_orders.csv")
    save_to_csv(sales_orders, "sales_orders.csv")


if __name__ == "__main__":
    main()
