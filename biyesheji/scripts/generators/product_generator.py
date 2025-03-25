import random
from datetime import datetime, timedelta
from ..base_generator import BaseGenerator
from ..config import (
    PRODUCT_POPULARITY, SEASONAL_PRODUCTS, INVENTORY_TURNOVER,
    REGIONS, DATA_CONFIG
)

class ProductGenerator(BaseGenerator):
    def __init__(self):
        super().__init__()
        self.product_ids = []
        self.product_info = {}  # 存储商品的详细信息，用于后续订单生成

    def _generate_product_name(self, category_name, brand_name):
        """根据分类和品牌生成商品名称"""
        adjectives = ['优质', '精选', '特级', '高端', '经典', '时尚', '轻奢', '简约']
        features = ['舒适', '耐用', '环保', '时尚', '实用', '便携', '高效', '智能']
        return f"{brand_name} {random.choice(adjectives)}{category_name} {random.choice(features)}款"

    def _generate_brand_name(self, category_name):
        """根据分类生成品牌名称"""
        brand_prefixes = ['优', '乐', '美', '康', '新', '雅', '金', '银', '铂', '钻']
        brand_suffixes = ['家', '达', '特', '尔', '斯', '克', '莱', '德', '可', '雅']
        return f"{random.choice(brand_prefixes)}{random.choice(brand_suffixes)}{category_name[:1]}"

    def _calculate_inventory_params(self, popularity_level, turnover_type):
        """计算库存相关参数"""
        turnover_config = INVENTORY_TURNOVER[turnover_type]
        popularity_config = PRODUCT_POPULARITY[popularity_level]
        
        # 基础库存范围
        base_stock_range = popularity_config['stock_range']
        base_stock = random.randint(base_stock_range[0], base_stock_range[1])
        
        # 计算安全库存和再订货点
        safety_factor = turnover_config['safety_stock_factor']
        safety_stock = int(base_stock * safety_factor)
        reorder_point = int(safety_stock * 1.5)  # 再订货点略高于安全库存
        
        # 计算周转天数和周转率
        days_range = turnover_config['turnover_days_range']
        turnover_days = random.randint(days_range[0], days_range[1])
        turnover_rate = round(365 / turnover_days, 2)
        
        return {
            'current_stock': base_stock,
            'safety_stock': safety_stock,
            'reorder_point': reorder_point,
            'turnover_days': turnover_days,
            'turnover_rate': turnover_rate
        }

    def _calculate_price_factors(self, category_id, popularity_level):
        """计算价格因素"""
        popularity_config = PRODUCT_POPULARITY[popularity_level]
        base_price = random.uniform(50, 1000)  # 基础价格范围
        
        # 应用热度因子
        price = base_price * popularity_config['price_factor']
        
        # 获取商品分类信息
        category_info = self.execute_query(
            "SELECT category_name FROM category WHERE category_id = %s",
            (category_id,)
        )[0]
        
        # 检查是否是季节性商品
        for season, config in SEASONAL_PRODUCTS.items():
            if any(cat in category_info['category_name'] for cat in config['categories']):
                current_month = datetime.now().month
                if current_month in config['months']:
                    price *= config['boost']
                break
        
        return round(price, 2)

    def generate_products(self, num_products, category_data, supplier_data):
        """生成商品数据
        
        Args:
            num_products: 要生成的商品总数
            category_data: 分类数据
            supplier_data: 供应商数据元组 (supplier_ids, supplier_regions, supplier_stabilities)
            
        Returns:
            tuple: (product_ids, product_info)
        """
        print("开始生成商品数据...")
        values = []
        
        # 获取所有三级分类
        third_level_categories = self.execute_query(
            "SELECT category_id, category_name FROM category WHERE category_level = 3"
        )
        
        # 解包供应商数据
        supplier_ids, supplier_regions, supplier_stabilities = supplier_data
        
        # 计算每个三级分类平均应该生成的商品数量
        avg_products_per_category = max(1, num_products // len(third_level_categories))
        remaining_products = num_products
        
        # 为每个三级分类生成商品
        for category in third_level_categories:
            # 确定该分类下要生成的商品数量
            if category == third_level_categories[-1]:
                # 最后一个分类，使用剩余的商品数量
                num_category_products = remaining_products
            else:
                # 在平均值的基础上随机浮动±20%
                variation = int(avg_products_per_category * 0.2)
                num_category_products = random.randint(
                    max(1, avg_products_per_category - variation),
                    avg_products_per_category + variation
                )
                num_category_products = min(num_category_products, remaining_products)
            
            remaining_products -= num_category_products
            
            for _ in range(num_category_products):
                product_id = self.generate_uuid()
                product_name = self._generate_product_name(
                    category['category_name'],
                    self._generate_brand_name(category['category_name'])
                )
                
                # 随机选择供应商
                supplier_id = random.choice(supplier_ids)
                
                # 确定商品热度等级
                popularity_weights = [
                    (PRODUCT_POPULARITY['hot']['ratio'], 'hot'),
                    (PRODUCT_POPULARITY['normal']['ratio'], 'normal'),
                    (PRODUCT_POPULARITY['cold']['ratio'], 'cold')
                ]
                popularity_level = random.choices(
                    [level for _, level in popularity_weights],
                    weights=[weight for weight, _ in popularity_weights]
                )[0]
                
                # 确定库存周转类型
                category_name = category['category_name']
                turnover_type = 'normal'  # 默认为正常周转
                
                # 根据分类确定周转类型
                for t_type, config in INVENTORY_TURNOVER.items():
                    if any(cat in category_name for cat in config['categories']):
                        turnover_type = t_type
                        break
                
                # 计算库存相关参数
                inventory_params = self._calculate_inventory_params(
                    popularity_level, turnover_type
                )
                
                # 计算价格
                price = self._calculate_price_factors(
                    category['category_id'], popularity_level
                )
                
                # 确定是否为季节性商品
                is_seasonal = False
                season_type = None
                for season, config in SEASONAL_PRODUCTS.items():
                    if any(cat in category_name for cat in config['categories']):
                        is_seasonal = True
                        season_type = season
                        break
                
                values.append((
                    product_id,                    # product_id
                    product_name,                  # product_name
                    category['category_id'],       # category_id
                    supplier_id,                   # supplier_id
                    price,                         # price
                    inventory_params['current_stock'],  # stock
                    '个',                          # unit
                    f"{product_name}的详细描述",    # description
                    popularity_level,              # popularity_level
                    is_seasonal,                   # is_seasonal
                    season_type,                   # season_type
                    inventory_params['safety_stock'],   # min_stock
                    inventory_params['current_stock'] * 2,  # max_stock
                    inventory_params['reorder_point'],  # reorder_point
                    inventory_params['safety_stock'],   # safety_stock
                    inventory_params['turnover_days'],  # turnover_days
                    inventory_params['turnover_rate'],  # turnover_rate
                    0,                             # sales_volume
                    0.0,                           # sales_amount
                    5.0,                           # avg_rating
                    datetime.now(),                # created_at
                    datetime.now()                 # updated_at
                ))
                
                # 存储商品信息供后续使用
                self.product_info[product_id] = {
                    'name': product_name,
                    'category_id': category['category_id'],
                    'supplier_id': supplier_id,
                    'price': price,
                    'popularity_level': popularity_level,
                    'inventory_params': inventory_params,
                    'is_seasonal': is_seasonal,
                    'season_type': season_type
                }
                self.product_ids.append(product_id)
        
        # 批量插入数据
        columns = [
            'product_id', 'product_name', 'category_id', 'supplier_id',
            'price', 'stock', 'unit', 'description', 'popularity_level',
            'is_seasonal', 'season_type', 'min_stock', 'max_stock',
            'reorder_point', 'safety_stock', 'turnover_days', 'turnover_rate',
            'sales_volume', 'sales_amount', 'avg_rating', 'created_at', 'updated_at'
        ]
        self.batch_insert('product', columns, values)
        print(f"成功生成 {len(values)} 条商品数据")
        
        return self.product_ids, self.product_info 