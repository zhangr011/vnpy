# flake8: noqa
"""
下载通达信指数合约1分钟bar => vnpy项目目录/bar_data/
上海股票 => SSE子目录
深圳股票 => SZSE子目录
"""
import os
import sys
import csv
import json
from collections import OrderedDict
import pandas as pd

vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vnpy_root not in sys.path:
    sys.path.append(vnpy_root)

os.environ["VNPY_TESTING"] = "1"

from vnpy.data.tdx.tdx_stock_data import *
from vnpy.trader.utility import load_json
from vnpy.trader.utility import get_csv_last_dt

# 保存的1分钟指数 bar目录
bar_data_folder = os.path.abspath(os.path.join(vnpy_root, 'bar_data'))

# 开始日期（每年大概需要几分钟）
start_date = '20160101'

# 创建API对象
api_01 = TdxStockData()

# 更新本地合约缓存信息
stock_list = load_json('stock_list.json')

symbol_dict = api_01.symbol_dict

# 逐一指数合约下载并更新
for stock_code in stock_list:
    market_id = get_tdx_market_code(stock_code)
    if market_id == 0:
        exchange_name = '深交所'
        exchange = Exchange.SZSE
    else:
        exchange_name = '上交所'
        exchange = Exchange.SSE

    symbol_info = symbol_dict.get(f'{stock_code}_{market_id}')
    stock_name = symbol_info.get('name')
    print(f'开始更新:{exchange_name}/{stock_name}, 代码:{stock_code}')
    bar_file_folder = os.path.abspath(os.path.join(bar_data_folder, f'{exchange.value}'))
    if not os.path.exists(bar_file_folder):
        os.makedirs(bar_file_folder)
    # csv数据文件名
    bar_file_path = os.path.abspath(os.path.join(bar_file_folder, f'{stock_code}_{start_date}_1m.csv'))

    # 如果文件存在，
    if os.path.exists(bar_file_path):
        # 取最后一条时间
        last_dt = get_csv_last_dt(bar_file_path)
    else:
        last_dt = None

    if last_dt:
        start_dt = last_dt - timedelta(days=1)
        print(f'文件{bar_file_path}存在，最后时间:{start_date}')
    else:
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        print(f'文件{bar_file_path}不存在，或读取最后记录错误,开始时间:{start_date}')

    result, bars = api_01.get_bars(symbol=stock_code,
                           period='1min',
                           callback=None,
                           start_dt=start_dt,
                           return_bar=False)
    # [dict] => dataframe
    if not result or len(bars) == 0:
        continue
    if last_dt is None:
        data_df = pd.DataFrame(bars)
        data_df.set_index('datetime', inplace=True)
        data_df = data_df.sort_index()
        # print(data_df.head())
        print(data_df.tail())
        data_df.to_csv(bar_file_path, index=True)
        print(f'首次更新{stock_code} {stock_name}数据 => 文件{bar_file_path}')
        continue

    # 获取标题
    headers = []
    with open(bar_file_path, "r", encoding='utf8') as f:
        reader = csv.reader(f)
        for header in reader:
            headers = header
            break

    bar_count = 0
    # 写入所有大于最后bar时间的数据
    with open(bar_file_path, 'a', encoding='utf8', newline='\n') as csvWriteFile:

        writer = csv.DictWriter(f=csvWriteFile, fieldnames=headers, dialect='excel',
                                extrasaction='ignore')
        for bar in bars:
            if bar['datetime'] <= last_dt:
                continue
            bar_count += 1
            writer.writerow(bar)

        print(f'更新{stock_code}  {stock_name} 数据 => 文件{bar_file_path}, 最后记录:{bars[-1]}')


print('更新完毕')
os._exit(0)
