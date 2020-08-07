# flake8: noqa
# 自动导出mongodb补全期货指数合约renko bar => csv文件
# 供renko bar 批量测试使用
import sys, os, copy, csv, signal

vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vnpy_root not in sys.path:
    print(f'append {vnpy_root} into sys.path')
    sys.path.append(vnpy_root)

os.environ["VNPY_TESTING"] = "1"

from vnpy.data.renko.rebuild_future import *

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print(f'请输入参数 host <db_name> <idx_symbol> <start_date> <end_date> \n '
              f'例如: python export_future_renko_bars.py 127.0.0.1 {FUTURE_RENKO_DB_NAME} RB99')
        exit()
    print(sys.argv)
    # Mongo host
    host = sys.argv[1]
    # 数据库
    if len(sys.argv) >= 3:
        db_name = sys.argv[2]
    else:
        db_name = FUTURE_RENKO_DB_NAME

    # 导出指数合约
    if len(sys.argv) >= 4:
        idx_symbol = sys.argv[3]
    else:
        idx_symbol = 'all'

    if len(sys.argv) >= 5:
        start_date = sys.argv[4]
    else:
        start_date = '2016-01-01'

    if len(sys.argv) >= 6:
        end_date = sys.argv[6]
    else:
        end_date = '2099-01-01'

    setting = {
        "host": host,
        "db_name": FUTURE_RENKO_DB_NAME,
        "cache_folder": os.path.join(vnpy_root, 'tick_data', 'tdx', 'future')
    }
    builder = FutureRenkoRebuilder(setting)

    if idx_symbol.upper() == 'ALL':
        print(u'导出所有合约')
        csv_folder = os.path.abspath(os.path.join(vnpy_root, 'bar_data', 'future_renko'))
        builder.export_all(start_date, end_date, csv_folder)

    else:
        for height in [3, 5, 10, 'K3', 'K5', 'K10']:
            csv_file = os.path.abspath(os.path.join(vnpy_root, 'bar_data', 'future_renko_{}_{}_{}_{}.csv'
                                                    .format(idx_symbol, height, start_date.replace('-', ''),
                                                            end_date.replace('-', ''))))
            builder.export(symbol=idx_symbol, height=height, start_date=start_date, end_date=end_date,
                           csv_file=csv_file)
