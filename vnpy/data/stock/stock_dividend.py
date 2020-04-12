# flake8: noqa
"""
# 追加/更新股票除权除息
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
from vnpy.trader.utility import load_json, load_data_from_pkb2, save_data_to_pkb2, extract_vt_symbol
from vnpy.data.tdx.tdx_common import get_stock_type
from vnpy.data.stock.stock_base import get_stock_base
import baostock as bs
import pandas as pd

STOCK_DIVIDEND_FILE = 'stock_dividend.csv'
years = [str(y) for y in range(2006, 2020)]

def update_stock_devidend():
    """
    更新股票除权除息信息
    :return:
    """
    stocks_data = get_stock_base()

    login_msg = bs.login()
    if login_msg.error_code != '0':
        print(f'证券宝登录错误代码:{login_msg.error_code}, 错误信息:{login_msg.error_msg}')
        return

    result_list = []

    for k, v in stocks_data.items():
        if v.get('类型') != '股票':
            continue
        symbol, exchange = extract_vt_symbol(k)
        bs_code = '.'.join(['sh' if exchange == Exchange.SSE else 'sz', symbol])
        stock_name = v.get('name')

        print(f'开始获取{stock_name} {bs_code}得除权除息')

        for year in years:
            rs = bs.query_dividend_data(
                code=bs_code,
                year=year
            )
            if rs.error_code != '0':
                print(f'证券宝获取沪深A股除权除息数据，错误代码:{rs.error_code}, 错误信息:{rs.error_msg}')
                continue

            # [dict] => dataframe

            #print(f'返回字段:{rs.fields}')
            while (rs.error_code == '0') and rs.next():
                row = rs.get_row_data()
                exchange_code, stock_code = row[0].split('.')
                exchange = Exchange.SSE if exchange_code == 'sh' else Exchange.SZSE
                d = {
                    'exchange': exchange.value,
                    'code': stock_code,
                    'name': stock_name,
                    'dividPreNoticeDate': row[1], # 预批露公告日
                    'dividAgmPumDate': row[2],   # 股东大会公告日期
                    'dividPlanAnnounceDate': row[3],  # 预案公告日
                    'dividPlanDate': row[4],   # 分红实施公告日
                    'dividRegistDate': row[5],  # 股权登记告日
                    'dividOperateDate': row[6],  # 除权除息日期
                    'dividPayDate': row[7],   # 派息日
                    'dividStockMarketDate': row[8],  # 红股上市交易日
                    'dividCashPsBeforeTax': row[9],  # 每股股利税前
                    'dividCashPsAfterTax': row[10],  # 每股股利税后
                    'dividStocksPs': row[11],  # 每股红股
                    'dividCashStock': row[12],  # 分红送转
                    'dividReserveToStockPs': row[13] # 每股转增资本
                }
                result_list.append(d)

                print(f'{d}')

    if len(result_list) > 0:
        df = pd.DataFrame(result_list)

        export_file_name = os.path.abspath(os.path.join(os.path.dirname(__file__), STOCK_DIVIDEND_FILE))

        df.to_csv(export_file_name)

        print(f'保存除权除息至文件:{export_file_name}')


if __name__ == '__main__':
    update_stock_devidend()
