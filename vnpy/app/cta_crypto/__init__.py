from pathlib import Path

from vnpy.trader.app import BaseApp
from .base import APP_NAME, StopOrder

from .engine import CtaEngine

from .template import (
    Direction,
    Offset,
    Status,
    OrderType,
    Interval,
    TickData,
    BarData,
    TradeData,
    OrderData,
    CtaTemplate,  CtaFutureTemplate)  # noqa
from vnpy.trader.utility import BarGenerator, ArrayManager  # noqa


class CtaCryptoApp(BaseApp):
    """"""
    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "CTA策略CRYPTO"
    engine_class = CtaEngine
    widget_name = "CtaManager"
    icon_name = "cta.ico"
