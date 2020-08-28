# flake8: noqa
"""
移除过期日志文件
"""
import os
import sys
from datetime import datetime, timedelta

vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if vnpy_root not in sys.path:
    sys.path.append(vnpy_root)

os.environ["VNPY_TESTING"] = "1"

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print(f'请输入{vnpy_root}下检查目录，例如 prod/account01', file=sys.stderr)
        exit()
    print(sys.argv)

    keep_days = 4

    if len(sys.argv) == 3:
        keep_days = int(sys.argv[2])
        print(f'保留最近{keep_days}日数据')

    log_path = os.path.abspath(os.path.join(vnpy_root, sys.argv[1], 'log'))
    if not os.path.exists(log_path):
        print(f'{log_path}不存在', file=sys.stderr)
        exit()
    print(f'开始检查{log_path}下的日志文件')

    dt_now = datetime.now()

    # 匹配日期
    delete_dates = []
    for n in range(keep_days, 30, 1):
        delete_date = dt_now - timedelta(days=n)
        delete_dates.append(delete_date.strftime('%Y-%m-%d'))
        delete_dates.append(delete_date.strftime('%Y%m%d'))

    # 移除匹配日期
    for dirpath, dirnames, filenames in os.walk(str(log_path)):

        for file_name in filenames:

            for k in delete_dates:
                if k in file_name:
                    file_path = os.path.abspath(os.path.join(dirpath, file_name))
                    print(f'移除{file_path}')
                    os.remove(file_path)
