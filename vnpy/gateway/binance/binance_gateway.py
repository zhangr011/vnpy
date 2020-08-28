"""
Gateway for Binance Crypto Exchange.
"""

import urllib
import hashlib
import hmac
import time
import json

from copy import copy
from datetime import datetime, timedelta
from enum import Enum
from threading import Lock

from vnpy.api.rest import RestClient, Request
from vnpy.api.websocket import WebsocketClient
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Product,
    Status,
    OrderType,
    Interval
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    AccountData,
    PositionData,
    ContractData,
    BarData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest
)
from vnpy.trader.event import EVENT_TIMER
from vnpy.event import Event
from vnpy.trader.utility import print_dict
REST_HOST = "https://www.binance.com"
WEBSOCKET_TRADE_HOST = "wss://stream.binance.com:9443/ws/"
WEBSOCKET_DATA_HOST = "wss://stream.binance.com:9443/stream?streams="

STATUS_BINANCE2VT = {
    "NEW": Status.NOTTRADED,
    "PARTIALLY_FILLED": Status.PARTTRADED,
    "FILLED": Status.ALLTRADED,
    "CANCELED": Status.CANCELLED,
    "REJECTED": Status.REJECTED
}

ORDERTYPE_VT2BINANCE = {
    OrderType.LIMIT: "LIMIT",
    OrderType.MARKET: "MARKET"
}
ORDERTYPE_BINANCE2VT = {v: k for k, v in ORDERTYPE_VT2BINANCE.items()}

DIRECTION_VT2BINANCE = {
    Direction.LONG: "BUY",
    Direction.SHORT: "SELL"
}
DIRECTION_BINANCE2VT = {v: k for k, v in DIRECTION_VT2BINANCE.items()}

INTERVAL_VT2BINANCE = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1h",
    Interval.DAILY: "1d",
}

TIMEDELTA_MAP = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}


class Security(Enum):
    NONE = 0
    SIGNED = 1
    API_KEY = 2


symbol_name_map = {}


class BinanceGateway(BaseGateway):
    """
    VN Trader Gateway for Binance connection.
    """

    default_setting = {
        "key": "",
        "secret": "",
        "session_number": 3,
        "proxy_host": "",
        "proxy_port": 0,
    }

    exchanges = [Exchange.BINANCE]

    def __init__(self, event_engine, gateway_name="BINANCE"):
        """Constructor"""
        super().__init__(event_engine, gateway_name)
        self.count = 0
        self.trade_ws_api = BinanceTradeWebsocketApi(self)
        self.market_ws_api = BinanceDataWebsocketApi(self)
        self.rest_api = BinanceRestApi(self)

    def connect(self, setting: dict):
        """"""
        key = setting["key"]
        secret = setting["secret"]
        session_number = setting["session_number"]
        proxy_host = setting["proxy_host"]
        proxy_port = setting["proxy_port"]

        self.rest_api.connect(key, secret, session_number,
                              proxy_host, proxy_port)
        self.market_ws_api.connect(proxy_host, proxy_port)

        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def subscribe(self, req: SubscribeRequest):
        """"""
        self.market_ws_api.subscribe(req)

    def send_order(self, req: OrderRequest):
        """"""
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest):
        """"""
        self.rest_api.cancel_order(req)

    def query_account(self):
        """"""
        pass

    def query_position(self):
        """"""
        pass

    def query_history(self, req: HistoryRequest):
        """"""
        return self.rest_api.query_history(req)

    def close(self):
        """"""
        self.rest_api.stop()
        self.trade_ws_api.stop()
        self.market_ws_api.stop()

    def process_timer_event(self, event: Event):
        """"""
        self.rest_api.keep_user_stream()
        if self.status.get('td_con', False) \
                and self.status.get('tdws_con', False) \
                and self.status.get('mdws_con', False):
            self.status.update({'con': True})

        self.count += 1
        if self.count < 5:
            return
        self.count = 0
        if len(self.query_functions) > 0:
            func = self.query_functions.pop(0)
            func()
            self.query_functions.append(func)

    def get_order(self, orderid: str):
        return self.rest_api.get_order(orderid)

class BinanceRestApi(RestClient):
    """
    BINANCE REST API
    """

    def __init__(self, gateway: BinanceGateway):
        """"""
        super().__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.trade_ws_api = self.gateway.trade_ws_api

        self.key = ""
        self.secret = ""

        self.user_stream_key = ""
        self.keep_alive_count = 0
        self.recv_window = 5000
        self.time_offset = 0

        self.order_count = 1_000_000
        self.order_count_lock = Lock()
        self.connect_time = 0

        self.assets = {}    # 币资产的合约信息， symbol: contract
        self.positions = {}  # 币持仓信息， symbol: position

        self.orders = {}

        self.accountid = ""

    def sign(self, request):
        """
        Generate BINANCE signature.
        """
        security = request.data["security"]
        if security == Security.NONE:
            request.data = None
            return request

        if request.params:
            path = request.path + "?" + urllib.parse.urlencode(request.params)
        else:
            request.params = dict()
            path = request.path

        if security == Security.SIGNED:
            timestamp = int(time.time() * 1000)

            if self.time_offset > 0:
                timestamp -= abs(self.time_offset)
            elif self.time_offset < 0:
                timestamp += abs(self.time_offset)

            request.params["timestamp"] = timestamp

            query = urllib.parse.urlencode(sorted(request.params.items()))
            signature = hmac.new(self.secret, query.encode(
                "utf-8"), hashlib.sha256).hexdigest()

            query += "&signature={}".format(signature)
            path = request.path + "?" + query

        request.path = path
        request.params = {}
        request.data = {}

        # Add headers
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "X-MBX-APIKEY": self.key
        }

        if security in [Security.SIGNED, Security.API_KEY]:
            request.headers = headers

        return request

    def connect(
            self,
            key: str,
            secret: str,
            session_number: int,
            proxy_host: str,
            proxy_port: int
    ):
        """
        Initialize connection to REST server.
        """
        self.key = key
        self.secret = secret.encode()
        self.proxy_port = proxy_port
        self.proxy_host = proxy_host

        self.connect_time = (
                int(datetime.now().strftime("%y%m%d%H%M%S")) * self.order_count
        )

        self.init(REST_HOST, proxy_host, proxy_port)
        self.start(session_number)

        self.gateway.write_log("REST API启动成功")
        self.gateway.status.update({'md_con': True, 'md_con_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
        self.query_time()
        self.query_account()
        self.query_order()
        self.query_contract()
        self.start_user_stream()

        # 添加到定时查询队列中
        self.gateway.query_functions = [self.query_account]

    def query_time(self):
        """"""
        data = {
            "security": Security.NONE
        }
        path = "/api/v1/time"

        return self.add_request(
            "GET",
            path,
            callback=self.on_query_time,
            data=data
        )

    def query_account(self):
        """"""
        data = {"security": Security.SIGNED}

        self.add_request(
            method="GET",
            path="/api/v3/account",
            callback=self.on_query_account,
            data=data
        )

    def query_order(self):
        """"""
        data = {"security": Security.SIGNED}

        self.add_request(
            method="GET",
            path="/api/v3/openOrders",
            callback=self.on_query_order,
            data=data
        )

    def query_contract(self):
        """"""
        data = {
            "security": Security.NONE
        }
        self.add_request(
            method="GET",
            path="/api/v1/exchangeInfo",
            callback=self.on_query_contract,
            data=data
        )

    def _new_order_id(self):
        """"""
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count

    def get_order(self, orderid: str):
        """返回缓存的Order"""
        return self.orders.get(orderid, None)

    def send_order(self, req: OrderRequest):
        """"""
        orderid = str(self.connect_time + self._new_order_id())
        order = req.create_order_data(
            orderid,
            self.gateway_name
        )
        order.accountid = self.accountid
        order.vt_accountid = f"{self.gateway_name}.{self.accountid}"
        order.datetime = datetime.now()
        self.orders.update({orderid: copy(order)})
        self.gateway.write_log(f'委托返回订单更新:{order.__dict__}')
        self.gateway.on_order(order)

        data = {
            "security": Security.SIGNED
        }

        params = {
            "symbol": req.symbol,
            "timeInForce": "GTC",
            "side": DIRECTION_VT2BINANCE[req.direction],
            "type": ORDERTYPE_VT2BINANCE[req.type],
            "price": str(req.price),
            "quantity": str(req.volume),
            "newClientOrderId": orderid,
            "newOrderRespType": "ACK"
        }

        if req.type == OrderType.MARKET:
            params.pop('timeInForce', None)
            params.pop('price', None)

        self.add_request(
            method="POST",
            path="/api/v3/order",
            callback=self.on_send_order,
            data=data,
            params=params,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed
        )

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest):
        """"""
        data = {
            "security": Security.SIGNED
        }

        params = {
            "symbol": req.symbol,
            "origClientOrderId": req.orderid
        }

        self.add_request(
            method="DELETE",
            path="/api/v3/order",
            callback=self.on_cancel_order,
            params=params,
            data=data,
            extra=req
        )

    def start_user_stream(self):
        """"""
        data = {
            "security": Security.API_KEY
        }

        self.add_request(
            method="POST",
            path="/api/v1/userDataStream",
            callback=self.on_start_user_stream,
            data=data
        )

    def keep_user_stream(self):
        """"""
        self.keep_alive_count += 1
        if self.keep_alive_count < 600:
            return
        self.keep_alive_count = 0

        data = {
            "security": Security.API_KEY
        }

        params = {
            "listenKey": self.user_stream_key
        }

        self.add_request(
            method="PUT",
            path="/api/v1/userDataStream",
            callback=self.on_keep_user_stream,
            params=params,
            data=data
        )

    def on_query_time(self, data, request):
        """"""
        local_time = int(time.time() * 1000)
        server_time = int(data["serverTime"])
        self.time_offset = local_time - server_time

    def on_query_account(self, data, request):
        """"""
        # ==》 account event
        if not self.accountid:
            self.accountid = self.gateway_name

        account = AccountData(
            accountid=self.accountid,
            balance=0,
            frozen=0,
            gateway_name=self.gateway_name
        )

        put_event_account = True
        # ==> position event
        for account_data in data["balances"]:
            pos = PositionData(
                accountid=self.gateway_name,
                symbol=account_data["asset"],
                exchange=Exchange.BINANCE,
                direction=Direction.NET,
                name='{}现货'.format(account_data["asset"]),
                volume=float(account_data["free"]) + float(account_data["locked"]),
                yd_volume=float(account_data["free"]) + float(account_data["locked"]),
                frozen=float(account_data["locked"]),
                gateway_name=self.gateway_name
            )

            #
            if pos.volume > 0 or (pos.volume == 0 and pos.symbol in self.positions):
                # 判断是否在本地缓存持仓中
                self.positions.update({pos.symbol: pos})
                self.gateway.on_position(copy(pos))

                if pos.symbol == 'USDT':
                    pos.cur_price = 1
                    account.balance += pos.volume
                else:
                    price = self.gateway.prices.get(f'{pos.symbol}USDT.{pos.exchange.value}', None)

                    if price is None:
                        req = SubscribeRequest(
                            symbol=f'{pos.symbol}USDT',
                            exchange=pos.exchange
                        )
                        self.gateway.subscribe(req)
                        put_event_account = False
                    else:
                        pos.cur_price = price
                        account.balance += pos.volume * price


        if put_event_account:
            self.gateway.on_account(account)
        #self.gateway.write_log("账户资金查询成功")

    def on_query_order(self, data, request):
        """"""
        for d in data:
            dt = datetime.fromtimestamp(d["time"] / 1000)
            time = dt.strftime("%Y-%m-%d %H:%M:%S")

            order = OrderData(
                accountid=self.accountid,
                orderid=d["clientOrderId"],
                symbol=d["symbol"],
                exchange=Exchange.BINANCE,
                price=float(d["price"]),
                volume=float(d["origQty"]),
                type=ORDERTYPE_BINANCE2VT[d["type"]],
                direction=DIRECTION_BINANCE2VT[d["side"]],
                traded=float(d["executedQty"]),
                status=STATUS_BINANCE2VT.get(d["status"], None),
                time=time,
                gateway_name=self.gateway_name,
            )
            self.orders.update({order.orderid: copy(order)})
            self.gateway.write_log(f'返回订单查询结果：{order.__dict__}')
            self.gateway.on_order(order)

        self.gateway.write_log("委托信息查询成功")

    def on_query_contract(self, data, request):
        """"""
        for d in data["symbols"]:
            base_currency = d["baseAsset"]
            quote_currency = d["quoteAsset"]
            name = f"{base_currency.upper()}/{quote_currency.upper()}"

            pricetick = 1
            min_volume = 1

            for f in d["filters"]:
                if f["filterType"] == "PRICE_FILTER":
                    pricetick = float(f["tickSize"])
                elif f["filterType"] == "LOT_SIZE":
                    min_volume = float(f["stepSize"])

            contract = ContractData(
                symbol=d["symbol"],
                exchange=Exchange.BINANCE,
                name=name,
                pricetick=pricetick,
                size=1,
                min_volume=min_volume,
                product=Product.SPOT,
                history_data=True,
                gateway_name=self.gateway_name,
            )
            self.gateway.on_contract(contract)

            for symbol in [d.get("baseAsset"), d.get("quoteAsset")]:
                if symbol and symbol not in self.assets:
                    asset_contract = ContractData(
                        symbol=symbol,
                        exchange=Exchange.BINANCE,
                        name="{}现货".format(symbol),
                        pricetick=0.000001,
                        size=1,
                        min_volume=0.00001,
                        product=Product.SPOT,
                        history_data=True,
                        gateway_name=self.gateway_name,
                    )
                    self.assets.update({symbol: asset_contract})
                    self.gateway.on_contract(copy(asset_contract))

            symbol_name_map[contract.symbol] = contract.name

        self.gateway.write_log("合约信息查询成功")

    def on_send_order(self, data, request):
        """"""
        pass

    def on_send_order_failed(self, status_code: str, request: Request):
        """
        Callback when sending order failed on server.
        """
        order = request.extra
        order.status = Status.REJECTED
        self.orders.update({order.orderid: copy(order)})
        self.gateway.write_log(f'订单委托失败:{order.__dict__}')
        if not order.accountid:
            order.accountid = self.accountid
            order.vt_accountid = f"{self.gateway_name}.{self.accountid}"
        if not order.datetime:
            order.datetime = datetime.now()
        self.gateway.on_order(order)

        msg = f"委托失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)

    def on_send_order_error(
            self, exception_type: type, exception_value: Exception, tb, request: Request
    ):
        """
        Callback when sending order caused exception.
        """
        order = request.extra
        order.status = Status.REJECTED
        self.orders.update({order.orderid: copy(order)})
        self.gateway.write_log(f'发送订单异常:{order.__dict__}')
        if not order.accountid:
            order.accountid = self.accountid
            order.vt_accountid = f"{self.gateway_name}.{self.accountid}"
        if not order.datetime:
            order.datetime = datetime.now()
        self.gateway.on_order(order)

        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_order(self, data, request):
        """"""
        pass

    def on_start_user_stream(self, data, request):
        """"""
        self.user_stream_key = data["listenKey"]
        self.keep_alive_count = 0
        url = WEBSOCKET_TRADE_HOST + self.user_stream_key

        self.trade_ws_api.connect(url, self.proxy_host, self.proxy_port)

    def on_keep_user_stream(self, data, request):
        """"""
        pass

    def query_history(self, req: HistoryRequest):
        """"""
        history = []
        limit = 1000
        start_time = int(datetime.timestamp(req.start))

        while True:
            # Create query params
            params = {
                "symbol": req.symbol,
                "interval": INTERVAL_VT2BINANCE[req.interval],
                "limit": limit,
                "startTime": start_time * 1000,  # convert to millisecond
            }

            # Add end time if specified
            if req.end:
                end_time = int(datetime.timestamp(req.end))
                params["endTime"] = end_time * 1000  # convert to millisecond

            # Get response from server
            resp = self.request(
                "GET",
                "/api/v1/klines",
                data={"security": Security.NONE},
                params=params
            )

            # Break if request failed with other status code
            if resp.status_code // 100 != 2:
                msg = f"获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data = resp.json()
                if not data:
                    msg = f"获取历史数据为空，开始时间：{start_time}"
                    self.gateway.write_log(msg)
                    break

                buf = []

                for l in data:
                    dt = datetime.fromtimestamp(l[0] / 1000)  # convert to second

                    bar = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=dt,
                        interval=req.interval,
                        volume=float(l[5]),
                        open_price=float(l[1]),
                        high_price=float(l[2]),
                        low_price=float(l[3]),
                        close_price=float(l[4]),
                        gateway_name=self.gateway_name
                    )
                    buf.append(bar)

                history.extend(buf)

                begin = buf[0].datetime
                end = buf[-1].datetime
                msg = f"获取历史数据成功，{req.symbol} - {req.interval.value}，{begin} - {end}"
                self.gateway.write_log(msg)

                # Break if total data count less than limit (latest date collected)
                if len(data) < limit:
                    break

                # Update start time
                start_dt = bar.datetime + TIMEDELTA_MAP[req.interval]
                start_time = int(datetime.timestamp(start_dt))

        return history


class BinanceTradeWebsocketApi(WebsocketClient):
    """"""

    def __init__(self, gateway):
        """"""
        super().__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

    def connect(self, url, proxy_host, proxy_port):
        """"""
        self.init(url, proxy_host, proxy_port)
        self.start()

    def on_connected(self):
        """"""
        self.gateway.write_log("交易Websocket API连接成功")
        self.gateway.status.update({'tdws_con': True, 'tdws_con_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

    def on_packet(self, packet: dict):  # type: (dict)->None
        """"""
        if packet["e"] == "outboundAccountInfo":
            self.on_account(packet)
        elif packet["e"] == "executionReport":
            self.on_order(packet)

    def on_account(self, packet):
        """web socket的账号更新信息"""

        # 这里不处理，改为定时resful查询进行更新
        """
        for d in packet["B"]:
            account = AccountData(
                accountid=d["a"],
                balance=float(d["f"]) + float(d["l"]),
                frozen=float(d["l"]),
                gateway_name=self.gateway_name
            )

            if account.balance:
                self.gateway.on_account(account)
        """
        return

    def on_order(self, packet: dict):
        """"""
        self.gateway.write_log('ws返回订单更新:\n'.format(json.dumps(packet, indent=2)))
        dt = datetime.fromtimestamp(packet["O"] / 1000)
        time = dt.strftime("%Y-%m-%d %H:%M:%S")

        if packet["C"] == "null" or len(packet['C']) == 0:
            orderid = packet["c"]
        else:
            orderid = packet["C"]

        # 尝试从缓存中获取订单
        order = self.gateway.get_order(orderid)
        if order:
            order.traded = float(packet["z"])
            order.status = STATUS_BINANCE2VT[packet["X"]]
            if order.status in [Status.CANCELLED, Status.REJECTED]:
                order.cancel_time = time
            if len(order.sys_orderid) == 0:
                order.sys_orderid = str(packet["i"])
        else:
            self.gateway.write_log(f'缓存中根据orderid:{orderid} 找不到Order,创建一个新的')
            self.gateway.write_log(print_dict(packet))
            order = OrderData(
                accountid=self.gateway_name,
                symbol=packet["s"],
                exchange=Exchange.BINANCE,
                orderid=orderid,
                sys_orderid=str(packet['i']),
                type=ORDERTYPE_BINANCE2VT[packet["o"]],
                direction=DIRECTION_BINANCE2VT[packet["S"]],
                price=float(packet["p"]),
                volume=float(packet["q"]),
                traded=float(packet["z"]),
                status=STATUS_BINANCE2VT[packet["X"]],
                datetime=dt,
                time=time,
                gateway_name=self.gateway_name
            )
        self.gateway.write_log(f'WS订单更新:\n{order.__dict__}')
        self.gateway.on_order(order)

        # Push trade event
        trade_volume = float(packet["l"])
        if not trade_volume:
            return

        trade_dt = datetime.fromtimestamp(packet["T"] / 1000)
        trade_time = trade_dt.strftime("%Y-%m-%d %H:%M:%S")

        trade = TradeData(
            accountid=self.gateway_name,
            symbol=order.symbol,
            exchange=order.exchange,
            orderid=order.orderid,
            tradeid=packet["t"],
            direction=order.direction,
            price=float(packet["L"]),
            volume=trade_volume,
            time=trade_time,
            datetime=trade_dt,
            gateway_name=self.gateway_name,
        )
        self.gateway.write_log(f'WS成交更新:\n{trade.__dict__}')
        self.gateway.on_trade(trade)


class BinanceDataWebsocketApi(WebsocketClient):
    """"""

    def __init__(self, gateway):
        """"""
        super().__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.ticks = {}

    def connect(self, proxy_host: str, proxy_port: int):
        """"""
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port

    def on_connected(self):
        """"""
        self.gateway.write_log("行情Websocket API连接刷新")
        self.gateway.status.update({'mdws_con': True, 'mdws_con_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

    def subscribe(self, req: SubscribeRequest):
        """"""
        if req.symbol not in symbol_name_map:
            self.gateway.write_log(f"找不到该合约代码{req.symbol}")
            return

        # Create tick buf data
        tick = TickData(
            symbol=req.symbol,
            name=symbol_name_map.get(req.symbol, ""),
            exchange=Exchange.BINANCE,
            datetime=datetime.now(),
            gateway_name=self.gateway_name,
        )
        self.ticks[req.symbol.lower()] = tick

        # Close previous connection
        if self._active:
            self.stop()
            self.join()

        # Create new connection
        channels = []
        for ws_symbol in self.ticks.keys():
            channels.append(ws_symbol + "@ticker")
            channels.append(ws_symbol + "@depth5")

        url = WEBSOCKET_DATA_HOST + "/".join(channels)
        self.init(url, self.proxy_host, self.proxy_port)
        self.start()

    def on_packet(self, packet):
        """"""
        stream = packet["stream"]
        data = packet["data"]

        symbol, channel = stream.split("@")
        tick = self.ticks[symbol]

        if channel == "ticker":
            tick.volume = float(data['v'])
            tick.open_price = float(data['o'])
            tick.high_price = float(data['h'])
            tick.low_price = float(data['l'])
            tick.last_price = float(data['c'])
            tick.datetime = datetime.fromtimestamp(float(data['E']) / 1000)
        else:
            bids = data["bids"]
            for n in range(5):
                price, volume = bids[n]
                tick.__setattr__("bid_price_" + str(n + 1), float(price))
                tick.__setattr__("bid_volume_" + str(n + 1), float(volume))

            asks = data["asks"]
            for n in range(5):
                price, volume = asks[n]
                tick.__setattr__("ask_price_" + str(n + 1), float(price))
                tick.__setattr__("ask_volume_" + str(n + 1), float(volume))

        if tick.last_price:
            self.gateway.on_tick(copy(tick))
