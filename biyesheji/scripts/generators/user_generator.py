from datetime import datetime, timedelta
import random
import string
from ..base_generator import BaseGenerator
from ..utils import DataGeneratorUtils
from ..config import REGIONS, USER_BEHAVIOR

class UserGenerator(BaseGenerator):
    def __init__(self):
        super().__init__()
        self.user_ids = []
        self.user_behaviors = {}
        self.user_regions = {}

    def _generate_password(self):
        """生成随机密码"""
        chars = string.ascii_letters + string.digits + '!@#$%^&*'
        return ''.join(random.choice(chars) for _ in range(12))

    def _generate_email(self, username):
        """生成邮箱"""
        domains = ['gmail.com', 'yahoo.com', 'hotmail.com', '163.com', 'qq.com']
        return f"{username}@{random.choice(domains)}"

    def _generate_phone(self):
        """生成手机号"""
        prefixes = ['130', '131', '132', '133', '134', '135', '136', '137', '138', '139',
                   '150', '151', '152', '153', '155', '156', '157', '158', '159',
                   '180', '181', '182', '183', '184', '185', '186', '187', '188', '189']
        return random.choice(prefixes) + ''.join(random.choice(string.digits) for _ in range(8))

    def _generate_address(self, region, city):
        """生成地址"""
        streets = ['中山路', '解放路', '人民路', '建设路', '和平路', '新华路', '长江路', '黄河路']
        communities = ['阳光小区', '和谐花园', '幸福苑', '康居园', '翠竹园', '金色家园', '龙湖花园']
        return f"{region}{city}{random.choice(streets)}{random.randint(1, 999)}号{random.choice(communities)}{random.randint(1, 33)}栋{random.randint(101, 2899)}室"

    def generate_users(self, num_users):
        """生成用户数据"""
        print("开始生成用户数据...")
        values = []
        
        # 生成开始日期和结束日期
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365*2)  # 2年内的注册时间
        
        # 根据用户行为类型的分布生成用户
        behavior_distribution = {
            'frequent': int(num_users * USER_BEHAVIOR['frequent']['ratio']),
            'regular': int(num_users * USER_BEHAVIOR['regular']['ratio']),
            'occasional': num_users - int(num_users * USER_BEHAVIOR['frequent']['ratio']) 
                        - int(num_users * USER_BEHAVIOR['regular']['ratio'])
        }
        
        progress_bar = self.get_progress_bar(num_users, "Generating Users")
        
        # 按照区域分布生成用户
        for region, config in REGIONS.items():
            # 计算该区域应该分配的用户数量
            region_users = int(num_users * config['user_distribution'])
            cities = config['cities']
            
            for _ in range(region_users):
                user_id = self.generate_uuid()
                self.user_ids.append(user_id)
                
                # 随机选择城市
                city = random.choice(cities)
                
                # 随机选择用户行为类型（考虑剩余配额）
                available_behaviors = [b for b, c in behavior_distribution.items() if c > 0]
                if not available_behaviors:
                    continue
                behavior = random.choice(available_behaviors)
                behavior_distribution[behavior] -= 1
                
                # 生成注册时间
                register_time = self.get_random_date(start_date, end_date)
                
                # 生成最后登录时间
                last_login_time = self.get_random_date(register_time, end_date)
                
                # 生成用户名
                username = f"user_{user_id[:8]}"
                
                # 根据用户类型生成基础统计数据
                behavior_config = USER_BEHAVIOR[behavior]
                total_orders = random.randint(
                    behavior_config['order_frequency'][0],
                    behavior_config['order_frequency'][1]
                )
                avg_order_amount = random.uniform(
                    behavior_config['avg_order_amount'][0],
                    behavior_config['avg_order_amount'][1]
                )
                total_amount = total_orders * avg_order_amount
                
                # 生成用户数据
                values.append((
                    user_id,                     # user_id
                    username,                    # username
                    self._generate_password(),   # password
                    self._generate_email(username),  # email
                    self._generate_phone(),      # phone
                    self._generate_address(region, city),  # address
                    region,                      # region
                    city,                        # city
                    behavior,                    # user_level
                    register_time,               # register_time
                    last_login_time,             # last_login_time
                    total_orders,                # total_orders
                    round(total_amount, 2),      # total_amount
                    behavior_config['promotion_sensitivity']  # promotion_sensitivity
                ))
                
                # 保存用户行为和区域信息供其他生成器使用
                self.user_behaviors[user_id] = behavior
                self.user_regions[user_id] = (region, city)
                
                progress_bar.update(1)
        
        progress_bar.close()
        
        # 批量插入数据
        columns = [
            'user_id', 'username', 'password', 'email', 'phone',
            'address', 'region', 'city', 'user_level', 'register_time',
            'last_login_time', 'total_orders', 'total_amount',
            'promotion_sensitivity'
        ]
        self.batch_insert('user', columns, values)
        print(f"成功生成 {len(values)} 条用户数据")
        
        return self.user_ids, self.user_behaviors, self.user_regions 