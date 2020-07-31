# flake8: noqa

import os
import sys
# 将repostory的目录i，作为根目录，添加到系统环境中。
ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(ROOT_PATH)
print(f'append {ROOT_PATH} into sys.path')

from vnpy.api.easytrader import server

server.run(port=1430)
