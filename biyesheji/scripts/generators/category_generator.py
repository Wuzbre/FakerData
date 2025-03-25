import random
from ..base_generator import BaseGenerator
from ..config import CATEGORY_LEVELS, SEASONAL_PRODUCTS

class CategoryGenerator(BaseGenerator):
    def __init__(self):
        super().__init__()
        self.category_ids = []
        self.category_seasons = {}
        self.parent_categories = {}

    def _generate_category_name(self, level, parent_name=None):
        """生成分类名称"""
        if level == 1:
            first_level = [
                '服装', '电子', '食品', '家居', '美妆', '母婴', 
                '运动', '数码', '图书', '玩具'
            ]
            return random.choice(first_level)
        elif level == 2:
            second_level = {
                '服装': ['男装', '女装', '童装', '内衣', '运动服', '商务装', '休闲装'],
                '电子': ['手机', '电脑', '相机', '智能设备', '办公设备', '音响'],
                '食品': ['零食', '饮料', '生鲜', '粮油', '调味品', '酒水'],
                '家居': ['家具', '家纺', '厨具', '清洁用品', '装饰品', '灯具'],
                '美妆': ['护肤', '彩妆', '香水', '美容工具', '美发护发'],
                '母婴': ['奶粉', '尿裤', '玩具', '童车', '婴儿用品', '孕妇用品'],
                '运动': ['健身器材', '户外装备', '运动鞋', '运动配件', '游泳用品'],
                '数码': ['游戏机', '耳机', '平板', '配件', '智能穿戴'],
                '图书': ['文学', '教育', '童书', '生活', '艺术', '科技'],
                '玩具': ['益智玩具', '电动玩具', '积木拼装', '毛绒玩具', '模型']
            }
            return random.choice(second_level.get(parent_name, ['未分类']))
        else:
            third_level = {
                '男装': ['衬衫', 'T恤', '外套', '裤装', '西装'],
                '女装': ['连衣裙', '上衣', '裤装', '外套', '套装'],
                '手机': ['安卓手机', '苹果手机', '手机配件', '充电设备'],
                '电脑': ['笔记本', '台式机', '平板电脑', '配件'],
                '零食': ['饼干', '糖果', '坚果', '膨化食品'],
                '家具': ['沙发', '床', '桌椅', '柜子', '架子'],
                '护肤': ['面霜', '精华', '面膜', '洁面', '防晒'],
                '奶粉': ['婴儿奶粉', '儿童奶粉', '成人奶粉', '特殊配方'],
                '健身器材': ['跑步机', '哑铃', '瑜伽垫', '健身车'],
                '游戏机': ['主机', '掌机', '游戏卡带', '配件']
            }
            return random.choice(third_level.get(parent_name, ['其他']))

    def generate_categories(self):
        """生成商品分类数据"""
        print("开始生成商品分类数据...")
        values = []
        level1_categories = {}  # 存储一级分类
        level2_categories = {}  # 存储二级分类
        
        # 生成一级分类
        for i in range(CATEGORY_LEVELS[1]):
            category_id = self.generate_uuid()
            category_name = self._generate_category_name(1)
            
            # 确定是否为季节性分类
            is_seasonal = category_name in ['服装', '运动', '食品']
            seasonal_info = {}
            if is_seasonal:
                # 为季节性分类随机分配主要和次要季节
                all_seasons = list(SEASONAL_PRODUCTS.keys())
                primary_season = random.choice(all_seasons)
                remaining_seasons = [s for s in all_seasons if s != primary_season]
                secondary_season = random.choice(remaining_seasons)
                seasonal_info = {
                    "primary_season": primary_season,
                    "secondary_season": secondary_season
                }
            
            values.append((
                category_id,           # category_id
                category_name,         # category_name
                None,                  # parent_id
                1,                     # category_level
                i + 1,                 # sort_order
                is_seasonal,           # is_seasonal
                seasonal_info.get("primary_season"),    # season_type
                0,                     # sales_volume
                0.0                    # sales_amount
            ))
            
            self.category_ids.append(category_id)
            level1_categories[category_name] = {
                'id': category_id,
                'seasonal_info': seasonal_info if is_seasonal else None
            }
            if is_seasonal:
                self.category_seasons[category_id] = seasonal_info
        
        # 生成二级分类
        sort_order = 1
        for parent_name, parent_info in level1_categories.items():
            parent_id = parent_info['id']
            for _ in range(random.randint(3, 6)):  # 每个一级分类下3-6个二级分类
                category_id = self.generate_uuid()
                category_name = self._generate_category_name(2, parent_name)
                
                # 继承父分类的季节性特征
                parent_seasonal_info = parent_info['seasonal_info']
                
                values.append((
                    category_id,           # category_id
                    category_name,         # category_name
                    parent_id,             # parent_id
                    2,                     # category_level
                    sort_order,            # sort_order
                    bool(parent_seasonal_info),  # is_seasonal
                    parent_seasonal_info.get("primary_season") if parent_seasonal_info else None,  # season_type
                    0,                     # sales_volume
                    0.0                    # sales_amount
                ))
                
                self.category_ids.append(category_id)
                level2_categories[category_name] = {
                    'id': category_id,
                    'seasonal_info': parent_seasonal_info
                }
                if parent_seasonal_info:
                    self.category_seasons[category_id] = parent_seasonal_info
                sort_order += 1
        
        # 生成三级分类
        for parent_name, parent_info in level2_categories.items():
            parent_id = parent_info['id']
            for _ in range(random.randint(3, 8)):  # 每个二级分类下3-8个三级分类
                category_id = self.generate_uuid()
                category_name = self._generate_category_name(3, parent_name)
                
                # 继承父分类的季节性特征
                parent_seasonal_info = parent_info['seasonal_info']
                
                values.append((
                    category_id,           # category_id
                    category_name,         # category_name
                    parent_id,             # parent_id
                    3,                     # category_level
                    sort_order,            # sort_order
                    bool(parent_seasonal_info),  # is_seasonal
                    parent_seasonal_info.get("primary_season") if parent_seasonal_info else None,  # season_type
                    0,                     # sales_volume
                    0.0                    # sales_amount
                ))
                
                self.category_ids.append(category_id)
                if parent_seasonal_info:
                    self.category_seasons[category_id] = parent_seasonal_info
                sort_order += 1
        
        # 批量插入数据
        columns = [
            'category_id', 'category_name', 'parent_id', 'category_level',
            'sort_order', 'is_seasonal', 'season_type', 'sales_volume',
            'sales_amount'
        ]
        self.batch_insert('category', columns, values)
        print(f"成功生成 {len(values)} 条分类数据")
        
        return self.category_ids, self.category_seasons 