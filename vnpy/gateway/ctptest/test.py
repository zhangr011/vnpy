# flake8: noqa

import sys
import os
import traceback
from time import sleep

vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if vnpy_root not in sys.path:
    print(u'append {}'.format(vnpy_root))
    sys.path.append(vnpy_root)

from vnpy.gateway.ctptest import CtptestGateway
from vnpy.event import EventEngine
from vnpy.trader.constant import Exchange,OrderType
from vnpy.trader.event import (
    EVENT_TICK,
    EVENT_ORDER,
    EVENT_TRADE,
    EVENT_POSITION,
    EVENT_ACCOUNT,
    EVENT_LOG,
)
from vnpy.trader.object import (
    SubscribeRequest,OrderRequest,Direction,Offset,CancelRequest
)
# 这里放期货公司需要你连接的测试系统的相关信息
ctp_setting = {
    "用户名": "12000430",
    "密码": "11112222w",
    "经纪商代码": "0187",
    "交易服务器": "tcp://110.87.99.14:61209",
    "行情服务器": "tcp://110.87.99.14:61219",
    "产品名称": "client_huafu_2.0.0",
    "授权编码": "BON2HDZHJBKLXKUK",
    "产品信息": ""
}


def test():
    """测试"""
    from qtpy import QtCore
    import sys

    def print_log(event):
        log = event.data
        print(f'{log.time}: {log.msg}\n')

    def print_event(event):
        data = event.data
        print(f'{data.__dict__}')

    app = QtCore.QCoreApplication(sys.argv)

    event_engine = EventEngine()
    event_engine.register(EVENT_LOG, print_log)
    event_engine.register(EVENT_TICK, print_event)
    event_engine.register(EVENT_ACCOUNT, print_event)
    event_engine.register(EVENT_ORDER, print_event)
    event_engine.register(EVENT_TRADE, print_event)
    event_engine.register(EVENT_POSITION, print_event)

    event_engine.start()

    gateway = CtptestGateway(event_engine)
    print(f'开始接入仿真测试:{ctp_setting}')
    gateway.connect(ctp_setting)

    # gateway.connect()
    auto_subscribe_symbols = ['rb2101']
    for symbol in auto_subscribe_symbols:
        print(u'自动订阅合约:{}'.format(symbol))
        sub = SubscribeRequest(symbol=symbol, exchange=Exchange.SHFE)
        sub.symbol = symbol
        gateway.subscribe(sub)

    couter = 20
    gateway.init_query()

    while couter > 0:
        print(u'{}'.format(couter))
        sleep(1)
        couter -= 1

    for i in range(5):
        print(f'发出rb2101的买入委托{i+1}')
        order_req = OrderRequest(
            strategy_name='',
            symbol='rb2101',
            exchange=Exchange.SHFE,
            direction=Direction.LONG,
            offset=Offset.OPEN,
            type=OrderType.LIMIT,
            price=3800,
            volume=i+1
        )
        gateway.send_order(order_req)


    for i in range(5):
        print(f'发出rb2101的平仓委托{i+1}')
        order_req = OrderRequest(
            strategy_name='',
            symbol='rb2101',
            exchange=Exchange.SHFE,
            direction=Direction.LONG,
            offset=Offset.CLOSETODAY,
            type=OrderType.LIMIT,
            price=3801,
            volume=i+1
        )
        gateway.send_order(order_req)

    #
    for i in range(5):
        print(f'发出rb2101的撤单委托{i + 1}')
        cancel_req = CancelRequest(
            orderid=f'5_-78969411_{i+1}',
            symbol='rb2101',
            exchange=Exchange.SHFE
        )
        gateway.cancel_order(cancel_req)


    sys.exit(app.exec_())


if __name__ == '__main__':

    try:
        test()
    except Exception as ex:
        print(u'异常:{},{}'.format(str(ex), traceback.format_exc()), file=sys.stderr)
    print('Finished')
