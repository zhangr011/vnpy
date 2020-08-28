# flake8: noqa
"""
下载rqdata股票合约1分钟bar => vnpy项目目录/bar_data/
"""
import os
import sys
import json
from collections import OrderedDict
import pandas as pd

vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vnpy_root not in sys.path:
    sys.path.append(vnpy_root)

os.environ["VNPY_TESTING"] = "1"

from vnpy.data.tdx.tdx_stock_data import *
from vnpy.trader.utility import load_json
from vnpy.trader.rqdata import RqdataClient
# 保存的1分钟指数 bar目录
bar_data_folder = os.path.abspath(os.path.join(vnpy_root, 'bar_data'))

# 开始日期（每年大概需要几分钟）
start_date = '20160101'

# 创建API对象
api_01 = TdxStockData()
rq = RqdataClient()

rq.init(username='incenselee@hotmail.com', password='123456')

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
        df_old = pd.read_csv(bar_file_path, index_col=0)
        df_old = df_old.rename(lambda x: pd.to_datetime(x, format="%Y-%m-%d %H:%M:%S"))
        # 取最后一条时间
        last_dt = df_old.index[-1]
        start_dt = last_dt - timedelta(days=1)
        print(f'文件{bar_file_path}存在，最后时间:{start_date}')
    else:
        df_old = None
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        print(f'文件{bar_file_path}不存在，开始时间:{start_date}')

    result, bars = api_01.get_bars(symbol=stock_code,
                           period='1min',
                           callback=None,
                           start_dt=start_dt,
                           return_bar=False)
    # [dict] => dataframe
    if not result or len(bars) == 0:
        continue
    df_extern = pd.DataFrame(bars)
    df_extern.set_index('datetime', inplace=True)

    if df_old is not None:
        # 扩展数据
        print('扩展数据')
        data_df = pd.concat([df_old, df_extern], axis=0)
    else:
        data_df = df_extern

    # 数据按时间戳去重
    print('按时间戳去重')
    data_df = data_df[~data_df.index.duplicated(keep='first')]
    # 排序
    data_df = data_df.sort_index()
    # print(data_df.head())
    print(data_df.tail())
    data_df.to_csv(bar_file_path, index=True)
    print(f'更新{stock_name} {stock_code}数据 => 文件{bar_file_path}')

print('更新完毕')
os._exit(0)
