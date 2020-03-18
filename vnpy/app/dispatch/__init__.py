# encoding: UTF-8

import os
from pathlib import Path
from vnpy.trader.app import BaseApp
from .dispatch_engine import DispatchEngine, APP_NAME


class DispatchApp(BaseApp):
    """"""
    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = u'调度引擎'
    engine_class = DispatchEngine
