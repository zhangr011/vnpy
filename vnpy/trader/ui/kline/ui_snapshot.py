# flake8: noqa
"""
多周期显示K线切片，
华富资产
"""

import sys
import os
import ctypes
import bz2
import pickle
import zlib
import pandas as pd

from vnpy.trader.ui.kline.crosshair import Crosshair
from vnpy.trader.ui.kline.kline import *

class UiSnapshot(object):
    """查看切片"""
    def __init__(self):

        pass

    def show(self, snapshot_file: str):

        if not os.path.exists(snapshot_file):
            print(f'{snapshot_file}不存在', file=sys.stderr)
            return

        d = None
        with bz2.BZ2File(snapshot_file, 'rb') as f:
             d = pickle.load(f)

        use_zlib = d.get('zlib', False)
        klines = d.pop('klines', None)

        # 如果使用压缩，则解压
        if use_zlib and klines:
            print('use zlib decompress klines')
            klines = pickle.loads(zlib.decompress(klines))

        kline_settings = {}
        for k, v in klines.items():
            # 获取bar各种数据/指标列表
            data_list = v.pop('data_list', None)
            if data_list is None:
                continue

            # 主图指标 / 附图指标清单
            main_indicators = v.get('main_indicators', [])
            sub_indicators = v.get('sub_indicators', [])

            df = pd.DataFrame(data_list)
            df = df.set_index(pd.DatetimeIndex(df['datetime']))

            kline_settings.update(
                {
                    k:
                        {
                            "data_frame": df,
                            "main_indicators": [x.get('name') for x in main_indicators],
                            "sub_indicators": [x.get('name') for x in sub_indicators]
                        }
                }
            )
        # K线界面
        try:
            w = GridKline(kline_settings=kline_settings, title=d.get('strategy',''))
            w.showMaximized()

        except Exception as ex:
            print(u'exception:{},trace:{}'.format(str(ex), traceback.format_exc()))
