# flake8: noqa
"""
下载证券宝5分钟bar => vnpy项目目录/bar_data/
"""
import os
import sys
import csv
import json
from collections import OrderedDict
import pandas as pd
from datetime import datetime, timedelta

vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vnpy_root not in sys.path:
    sys.path.append(vnpy_root)

os.environ["VNPY_TESTING"] = "1"
import baostock as bs

from vnpy.trader.constant import Exchange
from vnpy.data.tdx.tdx_common import get_tdx_market_code
from vnpy.trader.utility import load_json, get_csv_last_dt
from vnpy.data.stock.stock_base import get_stock_base
# 保存的1分钟指数 bar目录
bar_data_folder = os.path.abspath(os.path.join(vnpy_root, 'bar_data'))

# 开始日期（每年大概需要几分钟）
start_date = '20060101'

# 证券宝连接
login_msg = bs.login()
if login_msg.error_code != '0':
    print(f'证券宝登录错误代码:{login_msg.error_code}, 错误信息:{login_msg.error_msg}')

# 更新本地合约缓存信息
stock_list = load_json('stock_list.json')

symbol_dict = get_stock_base()

day_fields = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST"
min_fields = "date,time,code,open,high,low,close,volume,amount,adjustflag"

# 逐一股票下载并更新
for stock_code in stock_list:
    market_id = get_tdx_market_code(stock_code)
    if market_id == 0:
        exchange_name = '深交所'
        exchange = Exchange.SZSE
        exchange_code = 'sz'
    else:
        exchange_name = '上交所'
        exchange = Exchange.SSE
        exchange_code = 'sh'

    symbol_info = symbol_dict.get(f'{stock_code}.{exchange.value}')
    stock_name = symbol_info.get('name')
    print(f'开始更新:{exchange_name}/{stock_name}, 代码:{stock_code}')
    bar_file_folder = os.path.abspath(os.path.join(bar_data_folder, f'{exchange.value}'))
    if not os.path.exists(bar_file_folder):
        os.makedirs(bar_file_folder)
    # csv数据文件名
    bar_file_path = os.path.abspath(os.path.join(bar_file_folder, f'{stock_code}_{start_date}_5m.csv'))

    # 如果文件存在，
    if os.path.exists(bar_file_path):
        # df_old = pd.read_csv(bar_file_path, index_col=0)
        # df_old = df_old.rename(lambda x: pd.to_datetime(x, format="%Y-%m-%d %H:%M:%S"))
        # 取最后一条时间
        # last_dt = df_old.index[-1]
        last_dt = get_csv_last_dt(bar_file_path)
        start_dt = last_dt - timedelta(days=1)
        print(f'文件{bar_file_path}存在，最后时间:{start_date}')
    else:
        last_dt = None
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        print(f'文件{bar_file_path}不存在，开始时间:{start_date}')

    rs = bs.query_history_k_data_plus(
        code=f'{exchange_code}.{stock_code}',
        fields=min_fields,
        start_date=start_dt.strftime('%Y-%m-%d'), end_date=datetime.now().strftime('%Y-%m-%d'),
        frequency="5",
        adjustflag="3"
    )
    if rs.error_code != '0':
        print(f'证券宝获取沪深A股历史K线数据错误代码:{rs.error_code}, 错误信息:{rs.error_msg}')
        continue

    # [dict] => dataframe
    bars = []
    while (rs.error_code == '0') and rs.next():
        row = rs.get_row_data()
        dt = datetime.strptime(row[1], '%Y%m%d%H%M%S%f')
        if last_dt and last_dt > dt:
            continue
        bar = {
            'datetime': dt,
            'open': float(row[3]),
            'close': float(row[6]),
            'high': float(row[4]),
            'low': float(row[5]),
            'volume': float(row[7]),
            'amount': float(row[8]),
            'symbol': stock_code,
            'trading_date': row[0],
            'date': row[0],
            'time': dt.strftime('%H:%M:%S')
        }
        bars.append(bar)

    # 获取标题
    if len(bars) == 0:
        continue

    headers = list(bars[0].keys())
    if headers[0] != 'datetime':
        headers.remove('datetime')
        headers.insert(0, 'datetime')

    bar_count = 0
    # 写入所有大于最后bar时间的数据
    with open(bar_file_path, 'a', encoding='utf8', newline='\n') as csvWriteFile:

        writer = csv.DictWriter(f=csvWriteFile, fieldnames=headers, dialect='excel',
                                extrasaction='ignore')
        if last_dt is None:
            writer.writeheader()
        for bar in bars:
            bar_count += 1
            writer.writerow(bar)

        print(f'更新{stock_code}数据 => 文件{bar_file_path}, 最后记录:{bars[-1]}')


print('更新完毕')
bs.logout()
os._exit(0)
