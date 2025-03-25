import random
from datetime import datetime, timedelta
from decimal import Decimal
from ..base_generator import BaseGenerator
from ..config import INVENTORY_TURNOVER, SEASONAL_PRODUCTS

class InventoryGenerator(BaseGenerator):
    def __init__(self, product_info, order_details, category_seasons):
        super().__init__()
        self.product_info = product_info
        self.order_details = order_details
        self.category_seasons = category_seasons
        self.inventory_logs = []
        self.price_histories = []

    def generate_initial_inventory(self):
        """生成初始库存记录"""
        print("开始生成初始库存记录...")
        inventory_values = []
        price_values = []

        for product_id, info in self.product_info.items():
            # 生成入库记录
            inventory_values.append((
                self.generate_uuid(),        # log_id
                product_id,                  # product_id
                1,                          # change_type (入库)
                info['inventory_params']['current_stock'],  # change_quantity
                0,                          # before_quantity
                info['inventory_params']['current_stock'],  # after_quantity
                None,                       # related_order_id
                'system',                   # operator
                '初始库存入库',              # remark
                datetime.now()              # created_at
            ))

            # 生成初始价格记录
            price_values.append((
                self.generate_uuid(),        # history_id
                product_id,                  # product_id
                0,                          # old_price
                info['price'],              # new_price
                'initial',                  # change_reason
                'system',                   # operator
                datetime.now()              # created_at
            ))

        # 批量插入初始库存记录
        inventory_columns = [
            'log_id', 'product_id', 'change_type', 'change_quantity',
            'before_quantity', 'after_quantity', 'related_order_id',
            'operator', 'remark', 'created_at'
        ]
        self.batch_insert('inventory_log', inventory_columns, inventory_values)

        # 批量插入初始价格记录
        price_columns = [
            'history_id', 'product_id', 'old_price', 'new_price',
            'change_reason', 'operator', 'created_at'
        ]
        self.batch_insert('price_history', price_columns, price_values)

        print(f"成功生成 {len(inventory_values)} 条初始库存记录")
        print(f"成功生成 {len(price_values)} 条初始价格记录")

    def generate_order_inventory_changes(self):
        """根据订单生成库存变动记录"""
        print("开始生成订单相关的库存变动记录...")
        inventory_values = []

        for order_id, products in self.order_details.items():
            for product_id, details in products.items():
                quantity = details['quantity']
                
                # 获取当前库存
                current_stock = self.execute_query(
                    """
                    SELECT after_quantity 
                    FROM inventory_log 
                    WHERE product_id = %s 
                    ORDER BY created_at DESC 
                    LIMIT 1
                    """,
                    (product_id,)
                )[0]['after_quantity']

                # 生成出库记录
                inventory_values.append((
                    self.generate_uuid(),    # log_id
                    product_id,              # product_id
                    2,                      # change_type (出库)
                    quantity,               # change_quantity
                    current_stock,          # before_quantity
                    current_stock - quantity,  # after_quantity
                    order_id,               # related_order_id
                    'system',               # operator
                    '订单出库',              # remark
                    datetime.now()          # created_at
                ))

        # 批量插入库存变动记录
        if inventory_values:
            inventory_columns = [
                'log_id', 'product_id', 'change_type', 'change_quantity',
                'before_quantity', 'after_quantity', 'related_order_id',
                'operator', 'remark', 'created_at'
            ]
            self.batch_insert('inventory_log', inventory_columns, inventory_values)
            print(f"成功生成 {len(inventory_values)} 条订单库存变动记录")

    def generate_seasonal_price_changes(self, start_date, end_date):
        """生成季节性价格变动记录"""
        print("开始生成季节性价格变动记录...")
        price_values = []
        current_date = start_date

        while current_date <= end_date:
            # 获取当前季节
            current_season = list(SEASONAL_PRODUCTS.keys())[current_date.month % 4]
            
            # 检查每个商品是否需要季节性调价
            for product_id, info in self.product_info.items():
                category_id = info['category_id']
                seasonal_info = self.category_seasons.get(category_id, {})
                
                if seasonal_info and seasonal_info.get('primary_season') == current_season:
                    # 获取当前价格
                    current_price = self.execute_query(
                        """
                        SELECT new_price 
                        FROM price_history 
                        WHERE product_id = %s 
                        ORDER BY created_at DESC 
                        LIMIT 1
                        """,
                        (product_id,)
                    )[0]['new_price']

                    # 计算季节性价格调整（旺季上浮20%）
                    price_adjustment = Decimal('1.20')
                    new_price = round(current_price * price_adjustment, 2)
                    
                    price_values.append((
                        self.generate_uuid(),    # history_id
                        product_id,              # product_id
                        current_price,           # old_price
                        new_price,               # new_price
                        'seasonal',              # change_reason
                        'system',                # operator
                        current_date             # created_at
                    ))

            current_date += timedelta(days=90)  # 每季度检查一次

        # 批量插入价格变动记录
        if price_values:
            price_columns = [
                'history_id', 'product_id', 'old_price', 'new_price',
                'change_reason', 'operator', 'created_at'
            ]
            self.batch_insert('price_history', price_columns, price_values)
            print(f"成功生成 {len(price_values)} 条季节性价格变动记录") 