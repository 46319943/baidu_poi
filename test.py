import os
# 更改当前目录为文件锁在目录
current_path = os.path.abspath(__file__)
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)

import json
f = open('./json.txt', "r", encoding='utf-8')
setting = json.load(f)
print(setting)