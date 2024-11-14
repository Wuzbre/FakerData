import random
from datetime import datetime

from faker import Faker

import holidays

print('2024-10-08' in holidays.China())


fake = Faker('zh_CN')
# for i in range(100):
#     print(fake.date_time_between(datetime(2024, 11, 1), datetime(2024, 11, 2)))
# print(random.random())

