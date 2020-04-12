# flake8: noqa
"""
# 追加/更新股票基础信息
"""

import os
import sys
import json
from typing import Any
from collections import OrderedDict
import pandas as pd

vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vnpy_root not in sys.path:
    sys.path.append(vnpy_root)

os.environ["VNPY_TESTING"] = "1"
import baostock as bs
from vnpy.trader.constant import Exchange
from vnpy.trader.utility import load_json, load_data_from_pkb2, save_data_to_pkb2
from vnpy.data.tdx.tdx_common import get_stock_type
import baostock as bs

stock_type_map = {
    "1": '股票', "2": "指数", "3": "其他"
}
STOCK_BASE_FILE = 'stock_base.pkb2'


def get_stock_base():
    """ 获取股票基础信息"""
    base_file_name = os.path.abspath(os.path.join(os.path.dirname(__file__), STOCK_BASE_FILE))

    base_data = load_data_from_pkb2(base_file_name)
    if base_data is None:
        return update_stock_base()
    else:
        return base_data


def update_stock_base():
    """
    更新股票基础信息
    :return:
    """
    base_file_name = os.path.abspath(os.path.join(os.path.dirname(__file__), STOCK_BASE_FILE))

    base_data = load_data_from_pkb2(base_file_name)

    if base_data is None:
        base_data = dict()

    login_msg = bs.login()
    if login_msg.error_code != '0':
        print(f'证券宝登录错误代码:{login_msg.error_code}, 错误信息:{login_msg.error_msg}')
        return base_data

    rs = bs.query_stock_basic()
    if rs.error_code != '0':
        print(f'证券宝获取沪深A股历史K线数据错误代码:{rs.error_code}, 错误信息:{rs.error_msg}')
        return

    # [dict] => dataframe

    print(f'返回字段:{rs.fields}')
    while (rs.error_code == '0') and rs.next():
        row = rs.get_row_data()
        exchange_code, stock_code = row[0].split('.')
        exchange = Exchange.SSE if exchange_code == 'sh' else Exchange.SZSE
        d = {
            'exchange': exchange.value,
            'code': stock_code,
            'name': row[1],
            'ipo_date': row[2],
            'out_date': row[3],
            '类型': stock_type_map.get(row[4], '其他'),
            'type': get_stock_type(stock_code),
            'status': '上市' if row[5] == '1' else '退市'
        }
        base_data.update({f'{stock_code}.{exchange.value}': d})
        # print(f'{d}')

    save_data_to_pkb2(base_data, base_file_name)
    print(f'更新完毕')

    return base_data


if __name__ == '__main__':
    update_stock_base()
