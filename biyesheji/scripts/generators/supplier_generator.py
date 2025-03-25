from datetime import datetime, timedelta
import random
from ..base_generator import BaseGenerator
from ..config import REGIONS

class SupplierGenerator(BaseGenerator):
    def __init__(self):
        super().__init__()
        self.supplier_ids = []
        self.supplier_regions = {}
        self.supplier_stabilities = {}

    def _generate_contact_name(self):
        """生成联系人姓名"""
        first_names = '赵钱孙李周吴郑王冯陈褚卫蒋沈韩杨'
        last_names = '伟强明永健世洪海峰军平'
        return random.choice(first_names) + random.choice(last_names)

    def _generate_contact_phone(self):
        """生成联系电话"""
        prefixes = ['130', '131', '132', '133', '134', '135', '136', '137', '138', '139',
                   '150', '151', '152', '153', '155', '156', '157', '158', '159',
                   '180', '181', '182', '183', '184', '185', '186', '187', '188', '189']
        return random.choice(prefixes) + ''.join(str(random.randint(0, 9)) for _ in range(8))

    def _generate_email(self, supplier_name):
        """生成邮箱"""
        domains = ['company.com', 'corp.cn', 'enterprise.com', 'business.cn', 'trade.com']
        return f"{supplier_name.lower()}@{random.choice(domains)}"

    def _generate_address(self, region, city):
        """生成地址"""
        areas = ['工业园区', '经济开发区', '高新技术产业开发区', '保税区', '物流园区']
        streets = ['科技路', '创业路', '兴业路', '工业大道', '发展大道']
        return f"{region}{city}{random.choice(areas)}{random.choice(streets)}{random.randint(1, 999)}号"

    def generate_suppliers(self, num_suppliers):
        """生成供应商数据"""
        print("开始生成供应商数据...")
        values = []
        
        # 生成开始日期和结束日期
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365*5)  # 最长5年的合作历史
        
        progress_bar = self.get_progress_bar(num_suppliers, "Generating Suppliers")
        
        # 按区域分配供应商
        for region, config in REGIONS.items():
            # 计算该区域应分配的供应商数量
            region_suppliers = int(num_suppliers * config['sales_factor'] / sum(r['sales_factor'] for r in REGIONS.values()))
            cities = config['cities']
            
            for _ in range(region_suppliers):
                supplier_id = self.generate_uuid()
                self.supplier_ids.append(supplier_id)
                
                # 随机选择城市
                city = random.choice(cities)
                
                # 生成供应商名称
                supplier_name = f"supplier_{supplier_id[:8]}"
                
                # 生成合作开始时间
                cooperation_start = self.get_random_date(start_date, end_date)
                cooperation_days = (end_date - cooperation_start).days
                
                # 生成信用评分 (60-100)
                credit_score = random.randint(60, 100)
                
                # 计算供应商稳定性 (根据合作时间和信用评分)
                time_factor = min(cooperation_days / (365 * 5), 1)  # 最长5年
                credit_factor = (credit_score - 60) / 40  # 60-100 归一化
                stability = round((time_factor * 0.6 + credit_factor * 0.4), 2)  # 时间权重0.6，信用权重0.4
                
                # 生成供应能力 (根据区域和稳定性调整)
                base_capacity = random.randint(1000, 10000)
                region_factor = config['sales_factor']
                supply_capacity = int(base_capacity * region_factor * (0.8 + stability * 0.4))
                
                # 生成平均供货天数 (根据稳定性调整)
                avg_delivery_days = max(1, int(5 * (1.5 - stability)))  # 稳定性越高，供货天数越短
                
                values.append((
                    supplier_id,                # supplier_id
                    supplier_name,              # supplier_name
                    self._generate_contact_name(),  # contact_name
                    self._generate_contact_phone(), # contact_phone
                    self._generate_email(supplier_name),  # email
                    self._generate_address(region, city),  # address
                    region,                     # region
                    city,                       # city
                    supply_capacity,            # supply_capacity
                    cooperation_start.date(),   # cooperation_start_date
                    credit_score,               # credit_score
                    stability,                  # supply_stability
                    avg_delivery_days           # avg_delivery_days
                ))
                
                # 保存供应商区域和稳定性信息供其他生成器使用
                self.supplier_regions[supplier_id] = (region, city)
                self.supplier_stabilities[supplier_id] = stability
                
                progress_bar.update(1)
        
        progress_bar.close()
        
        # 批量插入数据
        columns = [
            'supplier_id', 'supplier_name', 'contact_name', 'contact_phone',
            'email', 'address', 'region', 'city', 'supply_capacity',
            'cooperation_start_date', 'credit_score', 'supply_stability',
            'avg_delivery_days'
        ]
        self.batch_insert('supplier', columns, values)
        print(f"成功生成 {len(values)} 条供应商数据")
        
        return self.supplier_ids, self.supplier_regions, self.supplier_stabilities 