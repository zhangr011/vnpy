# encoding: UTF-8

# 策略调度引擎

# 华富资产
from vnpy.event import EventEngine
from vnpy.trader.constant import Exchange  # noqa
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.event import EVENT_TIMER  # noqa

APP_NAME = 'DispatchEngine'


class DispatchEngine(BaseEngine):

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.main_engine = main_engine
        self.event_engine = event_engine
        self.create_logger(logger_name=APP_NAME)
