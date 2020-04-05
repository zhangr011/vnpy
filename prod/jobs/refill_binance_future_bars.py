# flake8: noqa


import os
import sys
import csv
import pandas as pd

# 将repostory的目录i，作为根目录，添加到系统环境中。
ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT_PATH not in sys.path:
    sys.path.append(ROOT_PATH)
    print(f'append {ROOT_PATH} into sys.path')

from datetime import datetime, timedelta
from vnpy.data.binance.binance_future_data import BinanceFutureData, HistoryRequest, Exchange, Interval
from vnpy.trader.utility import get_csv_last_dt, append_data

# 获取币安合约交易的所有期货合约
future_data = BinanceFutureData()
contracts = BinanceFutureData.load_contracts()
if len(contracts) == 0:
    future_data.save_contracts()
    contracts = BinanceFutureData.load_contracts()

# 开始下载日期
start_date = '20190101'

def download_symbol(symbol, start_dt, bar_file_path):
    req = HistoryRequest(
        symbol=symbol,
        exchange=Exchange(contract_info.get('exchange')),
        interval=Interval.MINUTE,
        start=start_dt
    )

    bars = future_data.get_bars(req=req, return_dict=True)
    future_data.export_to(bars, file_name=bar_file_path)

# 逐一合约进行下载
for vt_symbol, contract_info in contracts.items():
    symbol = contract_info.get('symbol')

    bar_file_path = os.path.abspath(os.path.join(
        ROOT_PATH,
        'bar_data',
        'binance',
        f'{symbol}_{start_date}_1m.csv'))

    # 不存在文件，直接下载，并保存
    if not os.path.exists(bar_file_path):
        print(f'文件{bar_file_path}不存在，开始时间:{start_date}')
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        download_symbol(symbol, start_dt, bar_file_path)
        continue

    # 如果存在文件，获取最后的bar时间
    last_dt = get_csv_last_dt(bar_file_path)

    # 获取不到时间，重新下载
    if last_dt is None:
        print(f'获取文件{bar_file_path}的最后时间失败，开始时间:{start_date}')
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        download_symbol(symbol, start_dt, bar_file_path)
        continue

    # 获取到时间，变成那天的开始时间，下载数据
    start_dt = last_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    print(f'文件{bar_file_path}存在，最后时间:{last_dt}, 调整数据获取开始时间:{start_dt}')
    req = HistoryRequest(
        symbol=symbol,
        exchange=Exchange(contract_info.get('exchange')),
        interval=Interval.MINUTE,
        start=start_dt
    )

    bars = future_data.get_bars(req=req, return_dict=True)
    if len(bars) <= 0:
        print(f'下载{symbol} 1分钟数据为空白')
        continue

    bar_count = 0

    # 获取标题
    headers = []
    with open(bar_file_path, "r", encoding='utf8') as f:
        reader = csv.reader(f)
        for header in reader:
            headers = header
            break

    # 写入所有大于最后bar时间的数据
    with open(bar_file_path, 'a', encoding='utf8', newline='\n') as csvWriteFile:

        writer = csv.DictWriter(f=csvWriteFile, fieldnames=headers, dialect='excel',
                                extrasaction='ignore')
        for bar in bars:
            if bar['datetime'] <= last_dt:
                continue
            bar_count += 1
            writer.writerow(bar)

        print(f'更新{symbol}数据 => 文件{bar_file_path}, 最后记录:{bars[-1]}')


