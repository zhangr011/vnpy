# flake8: noqa
"""
更新主力合约
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

from vnpy.data.tdx.tdx_future_data import *
from vnpy.trader.util_wechat import send_wx_msg
from vnpy.trader.utility import load_json, save_json

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print(f'请输入{vnpy_root}下检查目录，例如 prod/account01', file=sys.stderr)
        exit()
    print(sys.argv)

    for account_folder in sys.argv[1:]:
        cta_path = os.path.abspath(os.path.join(vnpy_root, account_folder))
        if not os.path.exists(cta_path):
            print(f'{cta_path}不存在', file=sys.stderr)
            exit()
        print(f'开始检查{cta_path}下的策略运行配置文件')
        account_name = account_folder.split('/')[-1]
        # 创建API对象
        api_01 = TdxFutureData()

        # 更新本地合约缓存信息
        api_01.update_mi_contracts()

        setting_file_path = os.path.abspath(os.path.join(cta_path, 'cta_strategy_pro_setting.json'))
        settings = load_json(setting_file_path, auto_save=False)

        if len(settings) == 0:
            print('无策略配置')
            os._exit(0)

        changed = False
        for strategy_name, setting in settings.items():

            vt_symbol = setting.get('vt_symbol')
            if not vt_symbol:
                print(f'{strategy_name}配置中无vt_symbol', file=sys.stderr)
                continue

            if '.' in vt_symbol:
                symbol, exchange = vt_symbol.split('.')
            else:
                symbol = vt_symbol
                exchange = None

            if exchange == Exchange.SPD:
                print(f"暂不处理自定义套利合约{vt_symbol}")
                continue

            full_symbol = get_full_symbol(symbol).upper()

            underlying_symbol = get_underlying_symbol(symbol).upper()

            contract_info = api_01.future_contracts.get(underlying_symbol)

            if not contract_info:
                print(f'{account_name}主力合约配置中，找不到{underlying_symbol}', file=sys.stderr)
                continue
            if 'mi_symbol' not in contract_info or 'exchange' not in contract_info or 'full_symbol' not in contract_info:
                print(f'{account_name}主力合约配置中，找不到mi_symbol/exchange/full_symbol. {contract_info}', file=sys.stderr)
                continue

            new_mi_symbol = contract_info.get('mi_symbol')
            new_exchange = contract_info.get('exchange')

            new_vt_symbol = '.'.join([new_mi_symbol, new_exchange])
            new_full_symbol = contract_info.get('full_symbol', '').upper()
            if full_symbol >= new_full_symbol:
                print(f'{account_name}策略配置：长合约{full_symbol}， 主力长合约{new_full_symbol}，不更新')
                continue

            if exchange:
                if len(vt_symbol) != len(new_vt_symbol):
                    print(f'{account_name}配置中，合约{vt_symbol} 与{new_vt_symbol} 长度不匹配，不更新', file=sys.stderr)
                    continue
            else:
                if len(symbol) != len(new_mi_symbol):
                    print(f'{account_name}配置中，合约{vt_symbol} 与{new_mi_symbol} 长度不匹配，不更新', file=sys.stderr)
                    continue

            setting.update({'vt_symbol': new_vt_symbol})
            send_wx_msg(f'{account_name}{strategy_name} 主力合约更换:{vt_symbol} => {new_vt_symbol} ')
            changed = True

        if changed:
            save_json(setting_file_path, settings)
            print(f'保存{account_name}新配置')

    print('更新完毕')
    os._exit(0)
