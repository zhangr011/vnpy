# flake8: noqa
# 自动补全股票renko bar => Mongodb
# 下载的tick数据缓存 => tick_data/tdx/future
import sys, os, copy, csv, signal

vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vnpy_root not in sys.path:
    print(f'append {vnpy_root} into sys.path')
    sys.path.append(vnpy_root)

os.environ["VNPY_TESTING"] = "1"

from vnpy.data.renko.rebuild_stock import *


if __name__ == "__main__":

    if len(sys.argv) < 4:
        print(u'请输入三个参数 host symbol pricetick [ bar]')
        exit()
    print(sys.argv)
    host = sys.argv[1]

    setting = {
        "host": host,
        "db_name": STOCK_RENKO_DB_NAME,
        "cache_folder": os.path.join(vnpy_root, 'tick_data', 'tdx', 'stock'),
        'bar_folder': os.path.join(vnpy_root,'bar_data')
    }
    builder = StockRenkoRebuilder(setting)

    symbol = sys.argv[2]
    price_tick = float(sys.argv[3])
    if len(sys.argv) >= 5 and sys.argv[4] == 'bar':
        using_bar = True
    else:
        using_bar = False

    if using_bar:
        print(f'启动股票bar=> renko补全,数据库:{host}/{STOCK_RENKO_DB_NAME} 合约:{symbol}')
        builder.start_with_bar(symbol=symbol, price_tick=price_tick, height=['K3', 'K5', 'K10'])

    else:
        print(f'启动股票tick=> renko补全,数据库:{host}/{STOCK_RENKO_DB_NAME} 合约:{symbol}')
        builder.start(symbol=symbol, price_tick=price_tick, height=['K3', 'K5', 'K10'], refill=True)

    print(f'exit refill {symbol} renkos')

