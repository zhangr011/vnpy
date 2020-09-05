"""
"""

import pytz
from datetime import datetime
from time import sleep
from copy import copy
from vnpy.api.sopt import (
    MdApi,
    TdApi,
    THOST_FTDC_OAS_Submitted,
    THOST_FTDC_OAS_Accepted,
    THOST_FTDC_OAS_Rejected,
    THOST_FTDC_OST_NoTradeQueueing,
    THOST_FTDC_OST_PartTradedQueueing,
    THOST_FTDC_OST_AllTraded,
    THOST_FTDC_OST_Canceled,
    THOST_FTDC_D_Buy,
    THOST_FTDC_D_Sell,
    THOST_FTDC_PD_Long,
    THOST_FTDC_PD_Short,
    THOST_FTDC_OPT_LimitPrice,
    THOST_FTDC_OPT_AnyPrice,
    THOST_FTDC_OF_Open,
    THOST_FTDC_OFEN_Close,
    THOST_FTDC_OFEN_CloseYesterday,
    THOST_FTDC_OFEN_CloseToday,
    THOST_FTDC_PC_ETFOption,
    THOST_FTDC_PC_Stock,
    THOST_FTDC_PC_Combination,
    THOST_FTDC_CP_CallOptions,
    THOST_FTDC_CP_PutOptions,
    THOST_FTDC_HF_Speculation,
    THOST_FTDC_CC_Immediately,
    THOST_FTDC_FCC_NotForceClose,
    THOST_FTDC_TC_GFD,
    THOST_FTDC_VC_AV,
    THOST_FTDC_TC_IOC,
    THOST_FTDC_VC_CV,
    THOST_FTDC_AF_Delete
)
from vnpy.trader.constant import (
    Direction,
    Offset,
    Exchange,
    OrderType,
    Product,
    Status,
    OptionType
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    PositionData,
    AccountData,
    ContractData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
)
from vnpy.trader.utility import (
    extract_vt_symbol,
    get_folder_path,
    get_trading_date,
    get_underlying_symbol,
    round_to,
    BarGenerator,
    print_dict
)
from vnpy.trader.event import EVENT_TIMER


STATUS_SOPT2VT = {
    THOST_FTDC_OAS_Submitted: Status.SUBMITTING,
    THOST_FTDC_OAS_Accepted: Status.SUBMITTING,
    THOST_FTDC_OAS_Rejected: Status.REJECTED,
    THOST_FTDC_OST_NoTradeQueueing: Status.NOTTRADED,
    THOST_FTDC_OST_PartTradedQueueing: Status.PARTTRADED,
    THOST_FTDC_OST_AllTraded: Status.ALLTRADED,
    THOST_FTDC_OST_Canceled: Status.CANCELLED
}

DIRECTION_VT2SOPT = {
    Direction.LONG: THOST_FTDC_D_Buy,
    Direction.SHORT: THOST_FTDC_D_Sell
}
DIRECTION_SOPT2VT = {v: k for k, v in DIRECTION_VT2SOPT.items()}
DIRECTION_SOPT2VT[THOST_FTDC_PD_Long] = Direction.LONG
DIRECTION_SOPT2VT[THOST_FTDC_PD_Short] = Direction.SHORT

ORDERTYPE_VT2SOPT = {
    OrderType.LIMIT: THOST_FTDC_OPT_LimitPrice,
    OrderType.MARKET: THOST_FTDC_OPT_AnyPrice
}
ORDERTYPE_SOPT2VT = {v: k for k, v in ORDERTYPE_VT2SOPT.items()}

OFFSET_VT2SOPT = {
    Offset.OPEN: THOST_FTDC_OF_Open,
    Offset.CLOSE: THOST_FTDC_OFEN_Close,
    Offset.CLOSETODAY: THOST_FTDC_OFEN_CloseToday,
    Offset.CLOSEYESTERDAY: THOST_FTDC_OFEN_CloseYesterday,
}
OFFSET_SOPT2VT = {v: k for k, v in OFFSET_VT2SOPT.items()}

EXCHANGE_SOPT2VT = {
    "SZSE": Exchange.SZSE,
    "SSE": Exchange.SSE
}

PRODUCT_SOPT2VT = {
    THOST_FTDC_PC_Stock: Product.EQUITY,
    THOST_FTDC_PC_ETFOption: Product.OPTION,
    THOST_FTDC_PC_Combination: Product.SPREAD
}

OPTIONTYPE_SOPT2VT = {
    THOST_FTDC_CP_CallOptions: OptionType.CALL,
    THOST_FTDC_CP_PutOptions: OptionType.PUT
}

CHINA_TZ = pytz.timezone("Asia/Shanghai")

symbol_exchange_map = {}
symbol_name_map = {}
symbol_size_map = {}
option_name_map = {}

class SoptGateway(BaseGateway):
    """
    VN Trader Gateway for SOPT .
    """

    default_setting = {
        "用户名": "",
        "密码": "",
        "经纪商代码": "",
        "交易服务器": "",
        "行情服务器": "",
        "产品名称": "",
        "授权编码": "",
        "产品信息": ""
    }

    exchanges = list(EXCHANGE_SOPT2VT.values())

    def __init__(self, event_engine, gateway_name="SOPT"):
        """Constructor"""
        super().__init__(event_engine, gateway_name)

        self.td_api = SoptTdApi(self)
        self.md_api = SoptMdApi(self)

        self.subscribed_symbols = set()  # 已订阅合约代码

        # 自定义价差/加比的tick合成器
        self.combiners = {}
        self.tick_combiner_map = {}

    def connect(self, setting: dict):
        """"""
        userid = setting["用户名"]
        password = setting["密码"]
        brokerid = setting["经纪商代码"]
        td_address = setting["交易服务器"]
        md_address = setting["行情服务器"]
        appid = setting["产品名称"]
        auth_code = setting["授权编码"]
        product_info = setting["产品信息"]

        if not td_address.startswith("tcp://"):
            td_address = "tcp://" + td_address
        if not md_address.startswith("tcp://"):
            md_address = "tcp://" + md_address

        self.td_api.connect(td_address, userid, password, brokerid, auth_code, appid, product_info)
        self.md_api.connect(md_address, userid, password, brokerid)

        # 获取自定义价差/价比合约的配置
        try:
            from vnpy.trader.engine import CustomContract
            c = CustomContract()
            self.combiner_conf_dict = c.get_config()
            if len(self.combiner_conf_dict) > 0:
                self.write_log(u'加载的自定义价差/价比配置:{}'.format(self.combiner_conf_dict))

                contract_dict = c.get_contracts()
                for vt_symbol, contract in contract_dict.items():
                    contract.gateway_name = self.gateway_name
                    symbol_exchange_map[contract.symbol] = contract.exchange
                    self.on_contract(contract)

        except Exception as ex:  # noqa
            pass

        self.init_query()

        #  从新发出委托
        for (vt_symbol, is_bar) in list(self.subscribed_symbols):
            symbol, exchange = extract_vt_symbol(vt_symbol)
            req = SubscribeRequest(
                symbol=symbol,
                exchange=exchange,
                is_bar=is_bar
            )
            self.subscribe(req)

    def subscribe(self, req: SubscribeRequest):
        """"""
        # 如果是自定义的套利合约符号
        if req.symbol in self.combiner_conf_dict:
            self.write_log(u'订阅自定义套利合约:{}'.format(req.symbol))
            # 创建合成器
            if req.symbol not in self.combiners:
                setting = self.combiner_conf_dict.get(req.symbol)
                setting.update({"symbol": req.symbol})
                combiner = TickCombiner(self, setting)
                # 更新合成器
                self.write_log(u'添加{}与合成器映射'.format(req.symbol))
                self.combiners.update({setting.get('symbol'): combiner})

                # 增加映射（ leg1 对应的合成器列表映射)
                leg1_symbol = setting.get('leg1_symbol')
                leg1_exchange = Exchange(setting.get('leg1_exchange'))
                combiner_list = self.tick_combiner_map.get(leg1_symbol, [])
                if combiner not in combiner_list:
                    self.write_log(u'添加Leg1:{}与合成器得映射'.format(leg1_symbol))
                    combiner_list.append(combiner)
                self.tick_combiner_map.update({leg1_symbol: combiner_list})

                # 增加映射（ leg2 对应的合成器列表映射)
                leg2_symbol = setting.get('leg2_symbol')
                leg2_exchange = Exchange(setting.get('leg2_exchange'))
                combiner_list = self.tick_combiner_map.get(leg2_symbol, [])
                if combiner not in combiner_list:
                    self.write_log(u'添加Leg2:{}与合成器得映射'.format(leg2_symbol))
                    combiner_list.append(combiner)
                self.tick_combiner_map.update({leg2_symbol: combiner_list})

                self.write_log(u'订阅leg1:{}'.format(leg1_symbol))
                leg1_req = SubscribeRequest(
                    symbol=leg1_symbol,
                    exchange=leg1_exchange
                )
                self.subscribe(leg1_req)

                self.write_log(u'订阅leg2:{}'.format(leg2_symbol))
                leg2_req = SubscribeRequest(
                    symbol=leg2_symbol,
                    exchange=leg2_exchange
                )
                self.subscribe(leg2_req)

                self.subscribed_symbols.add((req.vt_symbol, req.is_bar))
            else:
                self.write_log(u'{}合成器已经在存在'.format(req.symbol))
            return
        elif req.exchange == Exchange.SPD:
            self.write_error(u'自定义合约{}不在CTP设置中'.format(req.symbol))

        self.md_api.subscribe(req)

    def send_order(self, req: OrderRequest):
        """"""
        return self.td_api.send_order(req)

    def cancel_order(self, req: CancelRequest):
        """"""
        self.td_api.cancel_order(req)

    def query_account(self):
        """"""
        self.td_api.query_account()

    def query_position(self):
        """"""
        self.td_api.query_position()

    def close(self):
        """"""
        self.td_api.close()
        self.md_api.close()

    #def write_error(self, msg: str, error: dict):
    #    """"""
    #    error_id = error["ErrorID"]
    #    error_msg = error["ErrorMsg"]
    #    msg = f"{msg}，代码：{error_id}，信息：{error_msg}"
    #    self.write_log(msg)

    def process_timer_event(self, event):
        """"""
        self.count += 1
        if self.count < 2:
            return
        self.count = 0

        func = self.query_functions.pop(0)
        func()
        self.query_functions.append(func)

    def init_query(self):
        """"""
        self.count = 0
        self.query_functions = [self.query_account, self.query_position]
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)


    def on_custom_tick(self, tick):
        """推送自定义合约行情"""
        # 自定义合约行情

        for combiner in self.tick_combiner_map.get(tick.symbol, []):
            tick = copy(tick)
            combiner.on_tick(tick)


class SoptMdApi(MdApi):
    """"""

    def __init__(self, gateway):
        """Constructor"""
        super().__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.reqid = 0

        self.connect_status = False
        self.login_status = False
        self.subscribed = set()

        self.userid = ""
        self.password = ""
        self.brokerid = ""

    def onFrontConnected(self):
        """
        Callback when front server is connected.
        """
        self.gateway.write_log("行情服务器连接成功")
        self.login()
        self.gateway.status.update({'md_con': True, 'md_con_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

    def onFrontDisconnected(self, reason: int):
        """
        Callback when front server is disconnected.
        """
        self.login_status = False
        self.gateway.write_log(f"行情服务器连接断开，原因{reason}")
        self.gateway.status.update({'md_con': False, 'md_dis_con_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool):
        """
        Callback when user is logged in.
        """
        if not error["ErrorID"]:
            self.login_status = True
            self.gateway.write_log("行情服务器登录成功")

            for symbol in self.subscribed:
                self.subscribeMarketData(symbol)
        else:
            self.gateway.write_error("行情服务器登录失败", error)

    def onRspError(self, error: dict, reqid: int, last: bool):
        """
        Callback when error occured.
        """
        self.gateway.write_error("行情接口报错", error)

    def onRspSubMarketData(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        if not error or not error["ErrorID"]:
            return

        self.gateway.write_error("行情订阅失败", error)

    def onRtnDepthMarketData(self, data: dict):
        """
        Callback of tick data update.
        """
        symbol = data["InstrumentID"]
        exchange = symbol_exchange_map.get(symbol, "")
        if not exchange:
            return
        timestamp = f"{data['TradingDay']} {data['UpdateTime']}.{int(data['UpdateMillisec']/100)}"
        dt = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S.%f")
        #dt = CHINA_TZ.localize(dt)

        tick = TickData(
            symbol=symbol,
            exchange=exchange,
            datetime=dt,
            name=symbol_name_map[symbol],
            volume=data["Volume"],
            open_interest=data["OpenInterest"],
            last_price=data["LastPrice"],
            limit_up=data["UpperLimitPrice"],
            limit_down=data["LowerLimitPrice"],
            open_price=data["OpenPrice"],
            high_price=data["HighestPrice"],
            low_price=data["LowestPrice"],
            pre_close=data["PreClosePrice"],
            bid_price_1=data["BidPrice1"],
            ask_price_1=data["AskPrice1"],
            bid_volume_1=data["BidVolume1"],
            ask_volume_1=data["AskVolume1"],
            gateway_name=self.gateway_name
        )

        tick.bid_price_2 = data["BidPrice2"]
        tick.bid_price_3 = data["BidPrice3"]
        tick.bid_price_4 = data["BidPrice4"]
        tick.bid_price_5 = data["BidPrice5"]

        tick.ask_price_2 = data["AskPrice2"]
        tick.ask_price_3 = data["AskPrice3"]
        tick.ask_price_4 = data["AskPrice4"]
        tick.ask_price_5 = data["AskPrice5"]

        tick.bid_volume_2 = data["BidVolume2"]
        tick.bid_volume_3 = data["BidVolume3"]
        tick.bid_volume_4 = data["BidVolume4"]
        tick.bid_volume_5 = data["BidVolume5"]

        tick.ask_volume_2 = data["AskVolume2"]
        tick.ask_volume_3 = data["AskVolume3"]
        tick.ask_volume_4 = data["AskVolume4"]
        tick.ask_volume_5 = data["AskVolume5"]

        self.gateway.on_tick(tick)
        self.gateway.on_custom_tick(tick)

    def connect(self, address: str, userid: str, password: str, brokerid: int):
        """
        Start connection to server.
        """
        self.userid = userid
        self.password = password
        self.brokerid = brokerid

        # If not connected, then start connection first.
        if not self.connect_status:
            path = get_folder_path(self.gateway_name.lower())
            self.createFtdcMdApi(str(path) + "\\Md")
            self.registerFront(address)
            self.init()
            self.connect_status = True

            # Sleep 1 second and check trigger callback manually
            # (temp fix of the bug of Huaxi futures SOPT system)
            sleep(1)
            if not self.login_status:
                self.onFrontConnected()

        # If already connected, then login immediately.
        elif not self.login_status:
            self.login()

    def login(self):
        """
        Login onto server.
        """
        req = {
            "UserID": self.userid,
            "Password": self.password,
            "BrokerID": self.brokerid
        }

        self.reqid += 1
        self.reqUserLogin(req, self.reqid)

    def subscribe(self, req: SubscribeRequest):
        """
        Subscribe to tick data update.
        """
        if self.login_status:
            self.gateway.write_log(f'订阅:{req.exchange} {req.symbol}')
            self.subscribeMarketData(req.symbol)
        self.subscribed.add(req.symbol)

    def close(self):
        """
        Close the connection.
        """
        if self.connect_status:
            self.exit()


class SoptTdApi(TdApi):
    """"""

    def __init__(self, gateway):
        """Constructor"""
        super().__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.reqid = 0
        self.order_ref = 0

        self.connect_status = False
        self.login_status = False
        self.auth_staus = False
        self.login_failed = False

        self.userid = ""
        self.password = ""
        self.brokerid = ""
        self.auth_code = ""
        self.appid = ""
        self.product_info = ""

        self.frontid = 0
        self.sessionid = 0

        self.order_data = []
        self.trade_data = []
        self.positions = {}
        self.sysid_orderid_map = {}

        self.long_option_cost = None    # 多头期权动态市值
        self.short_option_cost = None   # 空头期权动态市值

    def onFrontConnected(self):
        """"""
        self.gateway.write_log("交易服务器连接成功")

        if self.auth_code:
            self.authenticate()
        else:
            self.login()
            self.gateway.status.update({'td_con': True, 'td_con_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

    def onFrontDisconnected(self, reason: int):
        """"""
        self.login_status = False
        self.gateway.write_log(f"交易服务器连接断开，原因{reason}")
        self.gateway.status.update({'td_con': True, 'td_dis_con_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

    def onRspAuthenticate(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        if not error['ErrorID']:
            self.auth_staus = True
            self.gateway.write_log("交易服务器授权验证成功")
            self.login()
        else:
            self.gateway.write_error("交易服务器授权验证失败", error)

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        if not error["ErrorID"]:
            self.frontid = data["FrontID"]
            self.sessionid = data["SessionID"]
            self.login_status = True
            self.gateway.write_log("交易服务器登录成功")

            # Confirm settlement
            req = {
                "BrokerID": self.brokerid,
                "InvestorID": self.userid
            }
            self.reqid += 1
            self.reqSettlementInfoConfirm(req, self.reqid)
        else:
            self.login_failed = True

            self.gateway.write_error("交易服务器登录失败", error)

    def onRspOrderInsert(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        order_ref = data["OrderRef"]
        orderid = f"{self.frontid}_{self.sessionid}_{order_ref}"

        symbol = data["InstrumentID"]
        exchange = symbol_exchange_map[symbol]
        order_type = OrderType.LIMIT
        if data["OrderPriceType"] == THOST_FTDC_OPT_AnyPrice:
            order_type = OrderType.MARKET
        order = OrderData(
            accountid=self.userid,
            symbol=symbol,
            exchange=exchange,
            orderid=orderid,
            sys_orderid=orderid,
            direction=DIRECTION_SOPT2VT[data["Direction"]],
            offset=OFFSET_SOPT2VT[data["CombOffsetFlag"]],
            price=data["LimitPrice"],
            type=order_type,
            volume=data["VolumeTotalOriginal"],
            status=Status.REJECTED,
            gateway_name=self.gateway_name
        )
        self.gateway.on_order(order)

        self.gateway.write_error(f"交易委托失败:{symbol} {order.direction.value} {order.offset.value} {order.price}, {order.volume}", error)

    def onRspOrderAction(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        self.gateway.write_error("交易撤单失败", error)

    def onRspQueryMaxOrderVolume(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        pass

    def onRspSettlementInfoConfirm(self, data: dict, error: dict, reqid: int, last: bool):
        """
        Callback of settlment info confimation.
        """
        self.gateway.write_log("结算信息确认成功")

        self.reqid += 1
        self.reqQryInstrument({}, self.reqid)

    def onRspQryInvestorPosition(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        if not data:
            return

        #self.gateway.write_log(print_dict(data))

        # Get buffered position object
        key = f"{data['InstrumentID'], data['PosiDirection']}"
        position = self.positions.get(key, None)
        if not position:
            position = PositionData(
                accountid=self.userid,
                symbol=data["InstrumentID"],
                name=symbol_name_map[data["InstrumentID"]],
                exchange=symbol_exchange_map[data["InstrumentID"]],
                direction=DIRECTION_SOPT2VT[data["PosiDirection"]],
                gateway_name=self.gateway_name
            )
            self.positions[key] = position

        # For SHFE position data update
        if position.exchange == Exchange.SHFE:
            if data["YdPosition"] and not data["TodayPosition"]:
                position.yd_volume = data["Position"]
        # For other exchange position data update
        else:
            position.yd_volume = data["Position"] - data["TodayPosition"]

        # Get contract size (spread contract has no size value)
        size = symbol_size_map.get(position.symbol, 0)

        # Calculate previous position cost
        cost = position.price * position.volume * size

        # Update new position volume
        position.volume += data["Position"]
        if data["PositionProfit"] == 0:
            position.pnl += data["PositionCost"] - data["OpenCost"]
        else:
            position.pnl += data["PositionProfit"]

        # Calculate average position price
        if position.volume and size:
            cost += data["PositionCost"]
            position.price = cost / (position.volume * size)

        # Get frozen volume
        if position.direction == Direction.LONG:
            position.frozen += data["ShortFrozen"]
        else:
            position.frozen += data["LongFrozen"]

        position.cur_price = self.gateway.prices.get(position.vt_symbol, None)
        if position.cur_price is None:
            position.cur_price = position.price
            self.gateway.subscribe(SubscribeRequest(symbol=position.symbol, exchange=position.exchange))

        if last:
            self.long_option_cost = None
            self.short_option_cost = None
            for position in self.positions.values():
                if position.symbol in option_name_map:
                    # 重新累计多头期权动态权益
                    if position.direction == Direction.LONG:
                        if self.long_option_cost is None:
                            self.long_option_cost = position.cur_price * position.volume * symbol_size_map.get(position.symbol, 0)
                        else:
                            self.long_option_cost += position.cur_price * position.volume * symbol_size_map.get(position.symbol, 0)

                    # 重新累计空头期权动态权益
                    if position.direction == Direction.SHORT:
                        if self.short_option_cost is None:
                            self.short_option_cost = position.cur_price * position.volume * symbol_size_map.get(position.symbol, 0)
                        else:
                            self.short_option_cost += position.cur_price * position.volume * symbol_size_map.get(position.symbol, 0)

                self.gateway.on_position(position)

            self.positions.clear()

    def onRspQryTradingAccount(self, data: dict, error: dict, reqid: int, last: bool):
        """"""

        balance = float(data["Balance"])

        # 资金差额（权利金，正数，是卖call或卖put，收入权利金; 负数，是买call、买put，付出权利金）
        cash_in = data.get('CashIn')
        #balance -= cash_in

        if self.long_option_cost is not None:
            balance += self.long_option_cost
        if self.short_option_cost is not None:
            balance -= self.short_option_cost
        account = AccountData(
            accountid=data["AccountID"],
            balance=balance,
            frozen=data["FrozenMargin"] + data["FrozenCash"] + data["FrozenCommission"],
            gateway_name=self.gateway_name
        )

        #self.gateway.write_log(print_dict(data))

        account.available = data["Available"]
        account.commission = round(float(data['Commission']), 7) + round(float(data['SpecProductCommission']), 7)
        account.margin = round(float(data['CurrMargin']), 7)
        account.close_profit = round(float(data['CloseProfit']), 7) + round(float(data['SpecProductCloseProfit']), 7)
        account.holding_profit = round(float(data['PositionProfit']), 7) + round(float(data['SpecProductPositionProfit']), 7)

        account.trading_day = str(data.get('TradingDay', datetime.now().strftime('%Y-%m-%d')))
        if '-' not in account.trading_day and len(account.trading_day) == 8:
            account.trading_day = '-'.join(
                [
                    account.trading_day[0:4],
                    account.trading_day[4:6],
                    account.trading_day[6:8]
                ]
            )

        self.gateway.on_account(account)

    def onRspQryInstrument(self, data: dict, error: dict, reqid: int, last: bool):
        """
        Callback of instrument query.
        """
        product = PRODUCT_SOPT2VT.get(data["ProductClass"], None)

        if product:
            contract = ContractData(
                symbol=data["InstrumentID"],
                exchange=EXCHANGE_SOPT2VT[data["ExchangeID"]],
                name=data["InstrumentName"].strip(),
                product=product,
                size=data["VolumeMultiple"],
                pricetick=data["PriceTick"],
                gateway_name=self.gateway_name
            )

            # For option only
            if contract.product == Product.OPTION:
                contract.option_portfolio = data["UnderlyingInstrID"] + "_O"
                contract.option_underlying = (
                    data["UnderlyingInstrID"]
                    + "-"
                    + str(data["DeliveryYear"])
                    + str(data["DeliveryMonth"]).rjust(2, "0")
                )
                contract.option_type = OPTIONTYPE_SOPT2VT.get(data["OptionsType"], None)
                contract.option_strike = data["StrikePrice"]
                #contract.option_index = str(data["StrikePrice"])
                contract.option_expiry = datetime.strptime(data["ExpireDate"], "%Y%m%d")
                contract.option_index = get_option_index(
                    contract.option_strike, data["InstrumentCode"]
                )
                option_name_map[contract.symbol] = contract.name

            self.gateway.on_contract(contract)

            symbol_exchange_map[contract.symbol] = contract.exchange
            symbol_name_map[contract.symbol] = contract.name
            symbol_size_map[contract.symbol] = contract.size

        if last:
            self.gateway.write_log("合约信息查询成功")

            for data in self.order_data:
                self.onRtnOrder(data)
            self.order_data.clear()

            for data in self.trade_data:
                self.onRtnTrade(data)
            self.trade_data.clear()

    def onRtnOrder(self, data: dict):
        """
        Callback of order status update.
        """
        symbol = data["InstrumentID"]
        exchange = symbol_exchange_map.get(symbol, "")
        if not exchange:
            self.order_data.append(data)
            return

        frontid = data["FrontID"]
        sessionid = data["SessionID"]
        order_ref = data["OrderRef"]
        orderid = f"{frontid}_{sessionid}_{order_ref}"

        timestamp = f"{data['InsertDate']} {data['InsertTime']}"
        dt = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S")
        #dt = CHINA_TZ.localize(dt)

        order = OrderData(
            accountid=self.userid,
            symbol=symbol,
            exchange=exchange,
            orderid=orderid,
            sys_orderid=orderid,
            type=ORDERTYPE_SOPT2VT[data["OrderPriceType"]],
            direction=DIRECTION_SOPT2VT[data["Direction"]],
            offset=OFFSET_SOPT2VT[data["CombOffsetFlag"]],
            price=data["LimitPrice"],
            volume=data["VolumeTotalOriginal"],
            traded=data["VolumeTraded"],
            status=STATUS_SOPT2VT[data["OrderStatus"]],
            datetime=dt,
            cancel_time=data["CancelTime"],
            gateway_name=self.gateway_name
        )
        self.gateway.on_order(order)

        self.sysid_orderid_map[data["OrderSysID"]] = orderid

    def onRtnTrade(self, data: dict):
        """
        Callback of trade status update.
        """
        symbol = data["InstrumentID"]
        exchange = symbol_exchange_map.get(symbol, "")
        if not exchange:
            self.trade_data.append(data)
            return

        orderid = self.sysid_orderid_map[data["OrderSysID"]]

        timestamp = f"{data['TradeDate']} {data['TradeTime']}"
        dt = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S")
        dt = CHINA_TZ.localize(dt)

        trade = TradeData(
            accountid=self.userid,
            symbol=symbol,
            exchange=exchange,
            orderid=orderid,
            sys_orderid=orderid,
            tradeid=data["TradeID"],
            direction=DIRECTION_SOPT2VT[data["Direction"]],
            offset=OFFSET_SOPT2VT[data["OffsetFlag"]],
            price=data["Price"],
            volume=data["Volume"],
            datetime=dt,
            gateway_name=self.gateway_name
        )
        self.gateway.on_trade(trade)

    def connect(
        self,
        address: str,
        userid: str,
        password: str,
        brokerid: int,
        auth_code: str,
        appid: str,
        product_info
    ):
        """
        Start connection to server.
        """
        self.userid = userid
        self.password = password
        self.brokerid = brokerid
        self.auth_code = auth_code
        self.appid = appid
        self.product_info = product_info

        if not self.connect_status:
            path = get_folder_path(self.gateway_name.lower())
            self.createFtdcTraderApi(str(path) + "\\Td")

            self.subscribePrivateTopic(0)
            self.subscribePublicTopic(0)

            self.registerFront(address)
            self.init()

            self.connect_status = True
        else:
            self.authenticate()

    def authenticate(self):
        """
        Authenticate with auth_code and appid.
        """
        req = {
            "UserID": self.userid,
            "BrokerID": self.brokerid,
            "AuthCode": self.auth_code,
            "AppID": self.appid
        }

        if self.product_info:
            req["UserProductInfo"] = self.product_info

        self.reqid += 1
        self.reqAuthenticate(req, self.reqid)

    def login(self):
        """
        Login onto server.
        """
        if self.login_failed:
            return

        req = {
            "UserID": self.userid,
            "Password": self.password,
            "BrokerID": self.brokerid,
            "AppID": self.appid
        }

        if self.product_info:
            req["UserProductInfo"] = self.product_info

        self.reqid += 1
        self.reqUserLogin(req, self.reqid)

    def send_order(self, req: OrderRequest):
        """
        Send new order.
        """
        self.order_ref += 1

        sopt_req = {
            "InstrumentID": req.symbol,
            "ExchangeID": req.exchange.value,
            "LimitPrice": req.price,
            "VolumeTotalOriginal": int(req.volume),
            "OrderPriceType": ORDERTYPE_VT2SOPT.get(req.type, ""),
            "Direction": DIRECTION_VT2SOPT.get(req.direction, ""),
            "CombOffsetFlag": OFFSET_VT2SOPT.get(req.offset, ""),
            "OrderRef": str(self.order_ref),
            "InvestorID": self.userid,
            "UserID": self.userid,
            "BrokerID": self.brokerid,
            "CombHedgeFlag": THOST_FTDC_HF_Speculation,
            "ContingentCondition": THOST_FTDC_CC_Immediately,
            "ForceCloseReason": THOST_FTDC_FCC_NotForceClose,
            "IsAutoSuspend": 0,
            "TimeCondition": THOST_FTDC_TC_GFD,
            "VolumeCondition": THOST_FTDC_VC_AV,
            "MinVolume": 1
        }

        if req.type == OrderType.FAK:
            sopt_req["OrderPriceType"] = THOST_FTDC_OPT_LimitPrice
            sopt_req["TimeCondition"] = THOST_FTDC_TC_IOC
            sopt_req["VolumeCondition"] = THOST_FTDC_VC_AV
        elif req.type == OrderType.FOK:
            sopt_req["OrderPriceType"] = THOST_FTDC_OPT_LimitPrice
            sopt_req["TimeCondition"] = THOST_FTDC_TC_IOC
            sopt_req["VolumeCondition"] = THOST_FTDC_VC_CV

        self.reqid += 1
        self.reqOrderInsert(sopt_req, self.reqid)

        orderid = f"{self.frontid}_{self.sessionid}_{self.order_ref}"
        order = req.create_order_data(orderid, self.gateway_name)
        order.accountid = self.userid
        order.vt_accountid = f"{self.gateway_name}.{self.userid}"
        self.gateway.on_order(order)

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest):
        """
        Cancel existing order.
        """
        frontid, sessionid, order_ref = req.orderid.split("_")

        sopt_req = {
            "InstrumentID": req.symbol,
            "Exchange": req.exchange,
            "OrderRef": order_ref,
            "FrontID": int(frontid),
            "SessionID": int(sessionid),
            "ActionFlag": THOST_FTDC_AF_Delete,
            "BrokerID": self.brokerid,
            "InvestorID": self.userid
        }

        self.reqid += 1
        self.reqOrderAction(sopt_req, self.reqid)

    def query_account(self):
        """
        Query account balance data.
        """
        self.reqid += 1
        self.reqQryTradingAccount({}, self.reqid)

    def query_position(self):
        """
        Query position holding data.
        """
        if not symbol_exchange_map:
            return

        req = {
            "BrokerID": self.brokerid,
            "InvestorID": self.userid
        }

        self.reqid += 1
        self.reqQryInvestorPosition(req, self.reqid)

    def close(self):
        """"""
        if self.connect_status:
            self.exit()


def get_option_index(strike_price: float, exchange_instrument_id: str) -> str:
    """"""
    exchange_instrument_id = exchange_instrument_id.replace(" ", "")

    if "M" in exchange_instrument_id:
        n = exchange_instrument_id.index("M")
    elif "A" in exchange_instrument_id:
        n = exchange_instrument_id.index("A")
    elif "B" in exchange_instrument_id:
        n = exchange_instrument_id.index("B")
    else:
        return str(strike_price)

    index = exchange_instrument_id[n:]
    option_index = f"{strike_price:.3f}-{index}"

    return option_index


class TickCombiner(object):
    """
    Tick合成类
    """

    def __init__(self, gateway, setting):
        self.gateway = gateway
        self.gateway_name = self.gateway.gateway_name
        self.gateway.write_log(u'创建tick合成类:{}'.format(setting))

        self.symbol = setting.get('symbol', None)
        self.leg1_symbol = setting.get('leg1_symbol', None)
        self.leg2_symbol = setting.get('leg2_symbol', None)
        self.leg1_ratio = setting.get('leg1_ratio', 1)  # 腿1的数量配比
        self.leg2_ratio = setting.get('leg2_ratio', 1)  # 腿2的数量配比
        self.price_tick = setting.get('price_tick', 1)  # 合成价差加比后的最小跳动
        # 价差
        self.is_spread = setting.get('is_spread', False)
        # 价比
        self.is_ratio = setting.get('is_ratio', False)

        self.last_leg1_tick = None
        self.last_leg2_tick = None

        # 价差日内最高/最低价
        self.spread_high = None
        self.spread_low = None

        # 价比日内最高/最低价
        self.ratio_high = None
        self.ratio_low = None

        # 当前交易日
        self.trading_day = None

        if self.is_ratio and self.is_spread:
            self.gateway.write_error(u'{}参数有误，不能同时做价差/加比.setting:{}'.format(self.symbol, setting))
            return

        self.gateway.write_log(u'初始化{}合成器成功'.format(self.symbol))
        if self.is_spread:
            self.gateway.write_log(
                u'leg1:{} * {} - leg2:{} * {}'.format(self.leg1_symbol, self.leg1_ratio, self.leg2_symbol,
                                                      self.leg2_ratio))
        if self.is_ratio:
            self.gateway.write_log(
                u'leg1:{} * {} / leg2:{} * {}'.format(self.leg1_symbol, self.leg1_ratio, self.leg2_symbol,
                                                      self.leg2_ratio))

    def on_tick(self, tick):
        """OnTick处理"""
        combinable = False

        if tick.symbol == self.leg1_symbol:
            # leg1合约
            self.last_leg1_tick = tick
            if self.last_leg2_tick is not None:
                if self.last_leg1_tick.datetime.replace(microsecond=0) == self.last_leg2_tick.datetime.replace(
                        microsecond=0):
                    combinable = True

        elif tick.symbol == self.leg2_symbol:
            # leg2合约
            self.last_leg2_tick = tick
            if self.last_leg1_tick is not None:
                if self.last_leg2_tick.datetime.replace(microsecond=0) == self.last_leg1_tick.datetime.replace(
                        microsecond=0):
                    combinable = True

        # 不能合并
        if not combinable:
            return

        if not self.is_ratio and not self.is_spread:
            return

        # 以下情况，基本为单腿涨跌停，不合成价差/价格比 Tick
        if (self.last_leg1_tick.ask_price_1 == 0 or self.last_leg1_tick.bid_price_1 == self.last_leg1_tick.limit_up) \
                and self.last_leg1_tick.ask_volume_1 == 0:
            self.gateway.write_log(
                u'leg1:{0}涨停{1}，不合成价差Tick'.format(self.last_leg1_tick.vt_symbol, self.last_leg1_tick.bid_price_1))
            return
        if (self.last_leg1_tick.bid_price_1 == 0 or self.last_leg1_tick.ask_price_1 == self.last_leg1_tick.limit_down) \
                and self.last_leg1_tick.bid_volume_1 == 0:
            self.gateway.write_log(
                u'leg1:{0}跌停{1}，不合成价差Tick'.format(self.last_leg1_tick.vt_symbol, self.last_leg1_tick.ask_price_1))
            return
        if (self.last_leg2_tick.ask_price_1 == 0 or self.last_leg2_tick.bid_price_1 == self.last_leg2_tick.limit_up) \
                and self.last_leg2_tick.ask_volume_1 == 0:
            self.gateway.write_log(
                u'leg2:{0}涨停{1}，不合成价差Tick'.format(self.last_leg2_tick.vt_symbol, self.last_leg2_tick.bid_price_1))
            return
        if (self.last_leg2_tick.bid_price_1 == 0 or self.last_leg2_tick.ask_price_1 == self.last_leg2_tick.limit_down) \
                and self.last_leg2_tick.bid_volume_1 == 0:
            self.gateway.write_log(
                u'leg2:{0}跌停{1}，不合成价差Tick'.format(self.last_leg2_tick.vt_symbol, self.last_leg2_tick.ask_price_1))
            return

        if self.trading_day != tick.trading_day:
            self.trading_day = tick.trading_day
            self.spread_high = None
            self.spread_low = None
            self.ratio_high = None
            self.ratio_low = None

        if self.is_spread:
            spread_tick = TickData(gateway_name=self.gateway_name,
                                   symbol=self.symbol,
                                   exchange=Exchange.SPD,
                                   datetime=tick.datetime)

            spread_tick.trading_day = tick.trading_day
            spread_tick.date = tick.date
            spread_tick.time = tick.time

            # 叫卖价差=leg1.ask_price_1 * 配比 - leg2.bid_price_1 * 配比，volume为两者最小
            spread_tick.ask_price_1 = round_to(target=self.price_tick,
                                               value=self.last_leg1_tick.ask_price_1 * self.leg1_ratio - self.last_leg2_tick.bid_price_1 * self.leg2_ratio)
            spread_tick.ask_volume_1 = min(self.last_leg1_tick.ask_volume_1, self.last_leg2_tick.bid_volume_1)

            # 叫买价差=leg1.bid_price_1 * 配比 - leg2.ask_price_1 * 配比，volume为两者最小
            spread_tick.bid_price_1 = round_to(target=self.price_tick,
                                               value=self.last_leg1_tick.bid_price_1 * self.leg1_ratio - self.last_leg2_tick.ask_price_1 * self.leg2_ratio)
            spread_tick.bid_volume_1 = min(self.last_leg1_tick.bid_volume_1, self.last_leg2_tick.ask_volume_1)

            # 最新价
            spread_tick.last_price = round_to(target=self.price_tick,
                                              value=(spread_tick.ask_price_1 + spread_tick.bid_price_1) / 2)
            # 昨收盘价
            if self.last_leg2_tick.pre_close > 0 and self.last_leg1_tick.pre_close > 0:
                spread_tick.pre_close = round_to(target=self.price_tick,
                                                 value=self.last_leg1_tick.pre_close * self.leg1_ratio - self.last_leg2_tick.pre_close * self.leg2_ratio)
            # 开盘价
            if self.last_leg2_tick.open_price > 0 and self.last_leg1_tick.open_price > 0:
                spread_tick.open_price = round_to(target=self.price_tick,
                                                  value=self.last_leg1_tick.open_price * self.leg1_ratio - self.last_leg2_tick.open_price * self.leg2_ratio)
            # 最高价
            if self.spread_high:
                self.spread_high = max(self.spread_high, spread_tick.ask_price_1)
            else:
                self.spread_high = spread_tick.ask_price_1
            spread_tick.high_price = self.spread_high

            # 最低价
            if self.spread_low:
                self.spread_low = min(self.spread_low, spread_tick.bid_price_1)
            else:
                self.spread_low = spread_tick.bid_price_1

            spread_tick.low_price = self.spread_low

            self.gateway.on_tick(spread_tick)

        if self.is_ratio:
            ratio_tick = TickData(
                gateway_name=self.gateway_name,
                symbol=self.symbol,
                exchange=Exchange.SPD,
                datetime=tick.datetime
            )

            ratio_tick.trading_day = tick.trading_day
            ratio_tick.date = tick.date
            ratio_tick.time = tick.time

            # 比率tick = (腿1 * 腿1 手数 / 腿2价格 * 腿2手数) 百分比
            ratio_tick.ask_price_1 = 100 * self.last_leg1_tick.ask_price_1 * self.leg1_ratio \
                                     / (self.last_leg2_tick.bid_price_1 * self.leg2_ratio)  # noqa
            ratio_tick.ask_price_1 = round_to(
                target=self.price_tick,
                value=ratio_tick.ask_price_1
            )

            ratio_tick.ask_volume_1 = min(self.last_leg1_tick.ask_volume_1, self.last_leg2_tick.bid_volume_1)
            ratio_tick.bid_price_1 = 100 * self.last_leg1_tick.bid_price_1 * self.leg1_ratio \
                                     / (self.last_leg2_tick.ask_price_1 * self.leg2_ratio)  # noqa
            ratio_tick.bid_price_1 = round_to(
                target=self.price_tick,
                value=ratio_tick.bid_price_1
            )

            ratio_tick.bid_volume_1 = min(self.last_leg1_tick.bid_volume_1, self.last_leg2_tick.ask_volume_1)
            ratio_tick.last_price = (ratio_tick.ask_price_1 + ratio_tick.bid_price_1) / 2
            ratio_tick.last_price = round_to(
                target=self.price_tick,
                value=ratio_tick.last_price
            )

            # 昨收盘价
            if self.last_leg2_tick.pre_close > 0 and self.last_leg1_tick.pre_close > 0:
                ratio_tick.pre_close = 100 * self.last_leg1_tick.pre_close * self.leg1_ratio / (
                        self.last_leg2_tick.pre_close * self.leg2_ratio)  # noqa
                ratio_tick.pre_close = round_to(
                    target=self.price_tick,
                    value=ratio_tick.pre_close
                )

            # 开盘价
            if self.last_leg2_tick.open_price > 0 and self.last_leg1_tick.open_price > 0:
                ratio_tick.open_price = 100 * self.last_leg1_tick.open_price * self.leg1_ratio / (
                        self.last_leg2_tick.open_price * self.leg2_ratio)  # noqa
                ratio_tick.open_price = round_to(
                    target=self.price_tick,
                    value=ratio_tick.open_price
                )

            # 最高价
            if self.ratio_high:
                self.ratio_high = max(self.ratio_high, ratio_tick.ask_price_1)
            else:
                self.ratio_high = ratio_tick.ask_price_1
            ratio_tick.high_price = self.spread_high

            # 最低价
            if self.ratio_low:
                self.ratio_low = min(self.ratio_low, ratio_tick.bid_price_1)
            else:
                self.ratio_low = ratio_tick.bid_price_1

            ratio_tick.low_price = self.spread_low

            self.gateway.on_tick(ratio_tick)
