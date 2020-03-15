""""""
import os
import sys
import uuid
import bz2
import pickle
import traceback
import zlib
import json
from abc import ABC
from copy import copy
from typing import Any, Callable
from logging import INFO, ERROR
from datetime import datetime
from vnpy.trader.constant import Interval, Direction, Offset, Status, OrderType
from vnpy.trader.object import BarData, TickData, OrderData, TradeData
from vnpy.trader.utility import virtual, append_data, extract_vt_symbol, get_underlying_symbol

from .base import StopOrder, EngineType
from vnpy.component.cta_grid_trade import CtaGrid, CtaGridTrade, LOCK_GRID
from vnpy.component.cta_position import CtaPosition
from vnpy.component.cta_policy import CtaPolicy  # noqa


class CtaTemplate(ABC):
    """CTA策略模板"""

    author = ""
    parameters = []
    variables = []

    # 保存委托单编号和相关委托单的字典
    # key为委托单编号
    # value为该合约相关的委托单
    active_orders = {}

    def __init__(
            self,
            cta_engine: Any,
            strategy_name: str,
            vt_symbol: str,
            setting: dict,
    ):
        """"""
        self.cta_engine = cta_engine
        self.strategy_name = strategy_name
        self.vt_symbol = vt_symbol

        self.inited = False  # 是否初始化完毕
        self.trading = False  # 是否开始交易
        self.pos = 0  # 持仓/仓差
        self.entrust = 0  # 是否正在委托, 0, 无委托 , 1, 委托方向是LONG， -1, 委托方向是SHORT

        self.tick_dict = {}  # 记录所有on_tick传入最新tick

        # Copy a new variables list here to avoid duplicate insert when multiple
        # strategy instances are created with the same strategy class.
        self.variables = copy(self.variables)
        self.variables.insert(0, "inited")
        self.variables.insert(1, "trading")
        self.variables.insert(2, "pos")
        self.variables.insert(3, "entrust")

    def update_setting(self, setting: dict):
        """
        Update strategy parameter wtih value in setting dict.
        """
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    @classmethod
    def get_class_parameters(cls):
        """
        Get default parameters dict of strategy class.
        """
        class_parameters = {}
        for name in cls.parameters:
            class_parameters[name] = getattr(cls, name)
        return class_parameters

    def get_parameters(self):
        """
        Get strategy parameters dict.
        """
        strategy_parameters = {}
        for name in self.parameters:
            strategy_parameters[name] = getattr(self, name)
        return strategy_parameters

    def get_variables(self):
        """
        Get strategy variables dict.
        """
        strategy_variables = {}
        for name in self.variables:
            strategy_variables[name] = getattr(self, name)
        return strategy_variables

    def get_data(self):
        """
        Get strategy data.
        """
        strategy_data = {
            "strategy_name": self.strategy_name,
            "vt_symbol": self.vt_symbol,
            "class_name": self.__class__.__name__,
            "author": self.author,
            "parameters": self.get_parameters(),
            "variables": self.get_variables(),
        }
        return strategy_data

    def get_positions(self):
        """ 返回持仓数量"""
        pos_list = []
        if self.pos > 0:
            pos_list.append({
                "vt_symbol": self.vt_symbol,
                "direction": "long",
                "volume": self.pos
            })
        elif self.pos < 0:
            pos_list.append({
                "vt_symbol": self.vt_symbol,
                "direction": "short",
                "volume": abs(self.pos)
            })
        return pos_list

    @virtual
    def on_timer(self):
        pass

    @virtual
    def on_init(self):
        """
        Callback when strategy is inited.
        """
        pass

    @virtual
    def on_start(self):
        """
        Callback when strategy is started.
        """
        pass

    @virtual
    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        pass

    @virtual
    def on_tick(self, tick: TickData):
        """
        Callback of new tick data update.
        """
        pass

    @virtual
    def on_bar(self, bar: BarData):
        """
        Callback of new bar data update.
        """
        pass

    @virtual
    def on_trade(self, trade: TradeData):
        """
        Callback of new trade data update.
        """
        pass

    @virtual
    def on_order(self, order: OrderData):
        """
        Callback of new order data update.
        """
        pass

    @virtual
    def on_stop_order(self, stop_order: StopOrder):
        """
        Callback of stop order update.
        """
        pass

    def buy(self, price: float, volume: float, stop: bool = False,
            vt_symbol: str = '', order_type: OrderType = OrderType.LIMIT,
            order_time: datetime = None, grid: CtaGrid = None):
        """
        Send buy order to open a long position.
        """
        if OrderType in [OrderType.FAK, OrderType.FOK]:
            if self.is_upper_limit(vt_symbol):
                self.write_error(u'涨停价不做FAK/FOK委托')
                return []
        return self.send_order(vt_symbol=vt_symbol,
                               direction=Direction.LONG,
                               offset=Offset.OPEN,
                               price=price,
                               volume=volume,
                               stop=stop,
                               order_type=order_type,
                               order_time=order_time,
                               grid=grid)

    def sell(self, price: float, volume: float, stop: bool = False,
             vt_symbol: str = '', order_type: OrderType = OrderType.LIMIT,
             order_time: datetime = None, grid: CtaGrid = None):
        """
        Send sell order to close a long position.
        """
        if OrderType in [OrderType.FAK, OrderType.FOK]:
            if self.is_lower_limit(vt_symbol):
                self.write_error(u'跌停价不做FAK/FOK sell委托')
                return []
        return self.send_order(vt_symbol=vt_symbol,
                               direction=Direction.SHORT,
                               offset=Offset.CLOSE,
                               price=price,
                               volume=volume,
                               stop=stop,
                               order_type=order_type,
                               order_time=order_time,
                               grid=grid)

    def short(self, price: float, volume: float, stop: bool = False,
              vt_symbol: str = '', order_type: OrderType = OrderType.LIMIT,
              order_time: datetime = None, grid: CtaGrid = None):
        """
        Send short order to open as short position.
        """
        if OrderType in [OrderType.FAK, OrderType.FOK]:
            if self.is_lower_limit(vt_symbol):
                self.write_error(u'跌停价不做FAK/FOK short委托')
                return []
        return self.send_order(vt_symbol=vt_symbol,
                               direction=Direction.SHORT,
                               offset=Offset.OPEN,
                               price=price,
                               volume=volume,
                               stop=stop,
                               order_type=order_type,
                               order_time=order_time,
                               grid=grid)

    def cover(self, price: float, volume: float, stop: bool = False,
              vt_symbol: str = '', order_type: OrderType = OrderType.LIMIT,
              order_time: datetime = None, grid: CtaGrid = None):
        """
        Send cover order to close a short position.
        """
        if OrderType in [OrderType.FAK, OrderType.FOK]:
            if self.is_upper_limit(vt_symbol):
                self.write_error(u'涨停价不做FAK/FOK cover委托')
                return []
        return self.send_order(vt_symbol=vt_symbol,
                               direction=Direction.LONG,
                               offset=Offset.CLOSE,
                               price=price,
                               volume=volume,
                               stop=stop,
                               order_type=order_type,
                               order_time=order_time,
                               grid=grid)

    def send_order(
            self,
            vt_symbol: str,
            direction: Direction,
            offset: Offset,
            price: float,
            volume: float,
            stop: bool = False,
            order_type: OrderType = OrderType.LIMIT,
            order_time: datetime = None,
            grid: CtaGrid = None
    ):
        """
        Send a new order.
        """
        # 兼容cta_strategy的模板，缺省不指定vt_symbol时，使用策略配置的vt_symbol
        if vt_symbol == '':
            vt_symbol = self.vt_symbol

        if not self.trading:
            return []

        vt_orderids = self.cta_engine.send_order(
            strategy=self,
            vt_symbol=vt_symbol,
            direction=direction,
            offset=offset,
            price=price,
            volume=volume,
            stop=stop,
            order_type=order_type
        )

        if order_time is None:
            order_time = datetime.now()

        for vt_orderid in vt_orderids:
            d = {
                'direction': direction,
                'offset': offset,
                'vt_symbol': vt_symbol,
                'price': price,
                'volume': volume,
                'order_type': order_type,
                'traded': 0,
                'order_time': order_time,
                'status': Status.SUBMITTING
            }
            if grid:
                d.update({'grid': grid})
                grid.order_ids.append(vt_orderid)
            self.active_orders.update({vt_orderid: d})
        if direction == Direction.LONG:
            self.entrust = 1
        elif direction == Direction.SHORT:
            self.entrust = -1
        return vt_orderids

    def cancel_order(self, vt_orderid: str):
        """
        Cancel an existing order.
        """
        if self.trading:
            return self.cta_engine.cancel_order(self, vt_orderid)

        return False

    def cancel_all(self):
        """
        Cancel all orders sent by strategy.
        """
        if self.trading:
            self.cta_engine.cancel_all(self)

    def is_upper_limit(self, symbol):
        """是否涨停"""
        tick = self.tick_dict.get(symbol, None)
        if tick is None or tick.limit_up is None or tick.limit_up == 0:
            return False
        if tick.bid_price_1 == tick.limit_up:
            return True

    def is_lower_limit(self, symbol):
        """是否跌停"""
        tick = self.tick_dict.get(symbol, None)
        if tick is None or tick.limit_down is None or tick.limit_down == 0:
            return False
        if tick.ask_price_1 == tick.limit_down:
            return True

    def write_log(self, msg: str, level: int = INFO):
        """
        Write a log message.
        """
        self.cta_engine.write_log(msg=msg, strategy_name=self.strategy_name, level=level)

    def write_error(self, msg: str):
        """write error log message"""
        self.write_log(msg=msg, level=ERROR)

    def get_engine_type(self):
        """
        Return whether the cta_engine is backtesting or live trading.
        """
        return self.cta_engine.get_engine_type()

    def load_bar(
            self,
            days: int,
            interval: Interval = Interval.MINUTE,
            callback: Callable = None,
    ):
        """
        Load historical bar data for initializing strategy.
        """
        if not callback:
            callback = self.on_bar

        self.cta_engine.load_bar(self.vt_symbol, days, interval, callback)

    def load_tick(self, days: int):
        """
        Load historical tick data for initializing strategy.
        """
        self.cta_engine.load_tick(self.vt_symbol, days, self.on_tick)

    def put_event(self):
        """
        Put an strategy data event for ui update.
        """
        if self.inited:
            self.cta_engine.put_strategy_event(self)

    def send_email(self, msg):
        """
        Send email to default receiver.
        """
        if self.inited:
            self.cta_engine.send_email(msg, self)

    def sync_data(self):
        """
        Sync strategy variables value into disk storage.
        """
        if self.trading:
            self.cta_engine.sync_strategy_data(self)




class CtaFutureTemplate(CtaTemplate):
    """
    合约期货模板
    """

    price_tick = 1  # 商品的最小价格跳动
    symbol_size = 10  # 商品得合约乘数
    margin_rate = 0.1  # 商品的保证金
    volumn_tick = 1 # 商品最小成交数量

    # 委托类型
    order_type = OrderType.LIMIT
    cancel_seconds = 120  # 撤单时间(秒)

    # 资金相关
    max_invest_rate = 0.1  # 最大仓位(0~1)
    max_invest_margin = 0  # 资金上限 0，不限制
    max_invest_pos = 0  # 单向头寸数量上限 0，不限制

    # 是否回测状态
    backtesting = False

    # 逻辑过程日志
    dist_fieldnames = ['datetime', 'symbol', 'volume', 'price',
                       'operation', 'signal', 'stop_price', 'target_price',
                       'long_pos', 'short_pos']

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        self.position = None  # 仓位组件
        self.policy = None  # 事务执行组件
        self.gt = None  # 网格交易组件
        self.klines = {}  # K线组件字典: kline_name: kline

        self.cur_datetime = None  # 当前Tick时间
        self.cur_tick = None  # 最新的合约tick( vt_symbol)
        self.cur_price = None  # 当前价（主力合约 vt_symbol)

        self.last_minute = None  # 最后的分钟,用于on_tick内每分钟处理的逻辑

        super().__init__(
            cta_engine, strategy_name, vt_symbol, setting
        )

        # 增加仓位管理模块
        self.position = CtaPosition(strategy=self)
        self.position.maxPos = sys.maxsize
        # 增加网格持久化模块
        self.gt = CtaGridTrade(strategy=self)

        if 'backtesting' not in self.parameters:
            self.parameters.append('backtesting')

    def update_setting(self, setting: dict):
        """
        Update strategy parameter wtih value in setting dict.
        """
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

        self.price_tick = self.cta_engine.get_price_tick(self.vt_symbol)
        self.symbol_size = self.cta_engine.get_size(self.vt_symbol)
        self.margin_rate = self.cta_engine.get_margin_rate(self.vt_symbol)
        self.volumn_tick = self.cta_engine.get_volume_tick(self.vt_symbol)

    def save_klines_to_cache(self, kline_names: list = []):
        """
        保存K线数据到缓存
        :param kline_names: 一般为self.klines的keys
        :return:
        """
        if len(kline_names) == 0:
            kline_names = list(self.klines.keys())

        # 获取保存路径
        save_path = self.cta_engine.get_data_path()
        # 保存缓存的文件名
        file_name = os.path.abspath(os.path.join(save_path, f'{self.strategy_name}_klines.pkb2'))
        with bz2.BZ2File(file_name, 'wb') as f:
            klines = {}
            for kline_name in kline_names:
                kline = self.klines.get(kline_name, None)
                # if kline:
                #    kline.strategy = None
                #    kline.cb_on_bar = None
                klines.update({kline_name: kline})
            pickle.dump(klines, f)

    def load_klines_from_cache(self, kline_names: list = []):
        """
        从缓存加载K线数据
        :param kline_names:
        :return:
        """
        if len(kline_names) == 0:
            kline_names = list(self.klines.keys())

        save_path = self.cta_engine.get_data_path()
        file_name = os.path.abspath(os.path.join(save_path, f'{self.strategy_name}_klines.pkb2'))
        try:
            last_bar_dt = None
            with bz2.BZ2File(file_name, 'rb') as f:
                klines = pickle.load(f)
                # 逐一恢复K线
                for kline_name in kline_names:
                    # 缓存的k线实例
                    cache_kline = klines.get(kline_name, None)
                    # 当前策略实例的K线实例
                    strategy_kline = self.klines.get(kline_name, None)

                    if cache_kline and strategy_kline:
                        # 临时保存当前的回调函数
                        cb_on_bar = strategy_kline.cb_on_bar
                        # 缓存实例数据 =》 当前实例数据
                        strategy_kline.__dict__.update(cache_kline.__dict__)

                        # 所有K线的最后时间
                        if last_bar_dt and strategy_kline.cur_datetime:
                            last_bar_dt = max(last_bar_dt, strategy_kline.cur_datetime)
                        else:
                            last_bar_dt = strategy_kline.cur_datetime

                        # 重新绑定k线策略与on_bar回调函数
                        strategy_kline.strategy = self
                        strategy_kline.cb_on_bar = cb_on_bar

                        self.write_log(f'恢复{kline_name}缓存数据,最新bar结束时间:{last_bar_dt}')

                self.write_log(u'加载缓存k线数据完毕')
                return last_bar_dt
        except Exception as ex:
            self.write_error(f'加载缓存K线数据失败:{str(ex)}')
        return None

    def get_klines_snapshot(self):
        """返回当前klines的切片数据"""
        try:
            d = {
                'strategy': self.strategy_name,
                'datetime': datetime.now()}
            klines = {}
            for kline_name in sorted(self.klines.keys()):
                klines.update({kline_name: self.klines.get(kline_name).get_data()})
            kline_names = list(klines.keys())
            binary_data = zlib.compress(pickle.dumps(klines))
            d.update({'kline_names': kline_names, 'klines': binary_data, 'zlib': True})
            return d
        except Exception as ex:
            self.write_error(f'获取klines切片数据失败:{str(ex)}')
            return {}

    def init_position(self):
        """
        初始化Positin
        使用网格的持久化，获取开仓状态的多空单，更新
        :return:
        """
        self.write_log(u'init_position(),初始化持仓')
        if len(self.gt.up_grids) <= 0:
            self.position.short_pos = 0
            # 加载已开仓的空单数据，网格JSON
            short_grids = self.gt.load(direction=Direction.SHORT, open_status_filter=[True])
            if len(short_grids) == 0:
                self.write_log(u'没有持久化的空单数据')
                self.gt.up_grids = []

            else:
                self.gt.up_grids = short_grids
                for sg in short_grids:
                    if len(sg.order_ids) > 0 or sg.order_status:
                        self.write_log(f'重置委托状态:{sg.order_status},清除委托单：{sg.order_ids}')
                        sg.order_status = False
                        sg.order_ids = []

                    self.write_log(u'加载持仓空单[{},价格:{},数量:{}手,开仓时间:{}'
                                   .format(self.vt_symbol, sg.open_price,
                                           sg.volume, sg.open_time))
                    self.position.short_pos -= sg.volume

                self.write_log(u'持久化空单，共持仓:{}手'.format(abs(self.position.short_pos)))

        if len(self.gt.dn_grids) <= 0:
            # 加载已开仓的多数据，网格JSON
            self.position.long_pos = 0
            long_grids = self.gt.load(direction=Direction.LONG, open_status_filter=[True])
            if len(long_grids) == 0:
                self.write_log(u'没有持久化的多单数据')
                self.gt.dn_grids = []
            else:
                self.gt.dn_grids = long_grids
                for lg in long_grids:

                    if len(lg.order_ids) > 0 or lg.order_status:
                        self.write_log(f'重置委托状态:{lg.order_status},清除委托单：{lg.order_ids}')
                        lg.order_status = False
                        lg.order_ids = []

                    self.write_log(u'加载持仓多单[{},价格:{},数量:{}手, 开仓时间:{}'
                                   .format(self.vt_symbol, lg.open_price, lg.volume, lg.open_time))
                    self.position.long_pos += lg.volume

                self.write_log(f'持久化多单，共持仓:{self.position.long_pos}手')

        self.position.pos = self.position.long_pos + self.position.short_pos

        self.write_log(u'{}加载持久化数据完成，多单:{}，空单:{},共:{}手'
                       .format(self.strategy_name,
                               self.position.long_pos,
                               abs(self.position.short_pos),
                               self.position.pos))
        self.pos = self.position.pos
        self.gt.save()
        self.display_grids()

    def get_positions(self):
        """
        获取策略当前持仓(重构，使用主力合约）
        :return: [{'vt_symbol':symbol,'direction':direction,'volume':volume]
        """
        if not self.position:
            return []
        pos_list = []

        if self.position.long_pos > 0:
            for g in self.gt.get_opened_grids(direction=Direction.LONG):
                pos_list.append({'vt_symbol': self.vt_symbol,
                                 'direction': 'long',
                                 'volume': g.volume - g.traded_volume,
                                 'price': g.open_price})

        if abs(self.position.short_pos) > 0:
            for g in self.gt.get_opened_grids(direction=Direction.SHORT):
                pos_list.append({'vt_symbol': self.vt_symbol,
                                 'direction': 'short',
                                 'volume': abs(g.volume - g.traded_volume),
                                 'price': g.open_price})

        if self.cur_datetime and (datetime.now() - self.cur_datetime).total_seconds() < 10:
            self.write_log(u'当前持仓:{}'.format(pos_list))
        return pos_list

    def tns_cancel_logic(self, dt, force=False):
        "撤单逻辑"""
        if len(self.active_orders) < 1:
            self.entrust = 0
            return

        for vt_orderid in list(self.active_orders.keys()):
            order_info = self.active_orders.get(vt_orderid)
            if order_info.get('status', None) in [Status.CANCELLED, Status.REJECTED]:
                self.active_orders.pop(vt_orderid, None)
                continue

            order_time = order_info.get('order_time')
            over_ms = (dt - order_time).total_seconds()
            if (over_ms > self.cancel_seconds) \
                    or force:  # 超过设置的时间还未成交
                self.write_log(f'{dt}, 超时{over_ms}秒未成交，取消委托单：{order_info}')

                if self.cancel_order(vt_orderid):
                    order_info.update({'status': Status.CANCELLING})
                else:
                    order_info.update({'status': Status.CANCELLED})

        if len(self.active_orders) < 1:
            self.entrust = 0

    def display_grids(self):
        """更新网格显示信息"""
        if not self.inited:
            return

        up_grids_info = self.gt.to_str(direction=Direction.SHORT)
        if len(self.gt.up_grids) > 0:
            self.write_log(up_grids_info)

        dn_grids_info = self.gt.to_str(direction=Direction.LONG)
        if len(self.gt.dn_grids) > 0:
            self.write_log(dn_grids_info)

    def display_tns(self):
        """显示事务的过程记录=》 log"""
        if not self.inited:
            return
        self.write_log(u'{} 当前 {}价格：{}'
                       .format(self.cur_datetime, self.vt_symbol, self.cur_price))
        if hasattr(self, 'policy'):
            policy = getattr(self, 'policy')
            op = getattr(policy, 'to_json', None)
            if callable(op):
                self.write_log(u'当前Policy:{}'.format(json.dumps(policy.to_json(), indent=2, ensure_ascii=False)))

    def save_dist(self, dist_data):
        """
        保存策略逻辑过程记录=》 csv文件按
        :param dist_data:
        :return:
        """
        if self.backtesting:
            save_path = self.cta_engine.get_logs_path()
        else:
            save_path = self.cta_engine.get_data_path()
        try:
            if self.position and 'long_pos' not in dist_data:
                dist_data.update({'long_pos': self.position.long_pos})
            if self.position and 'short_pos' not in dist_data:
                dist_data.update({'short_pos': self.position.short_pos})

            file_name = os.path.abspath(os.path.join(save_path, f'{self.strategy_name}_dist.csv'))
            append_data(file_name=file_name, dict_data=dist_data, field_names=self.dist_fieldnames)
        except Exception as ex:
            self.write_error(u'save_dist 异常:{} {}'.format(str(ex), traceback.format_exc()))

    def save_tns(self, tns_data):
        """
        保存多空事务记录=》csv文件,便于后续分析
        :param tns_data:
        :return:
        """
        if self.backtesting:
            save_path = self.cta_engine.get_logs_path()
        else:
            save_path = self.cta_engine.get_data_path()

        try:
            file_name = os.path.abspath(os.path.join(save_path, f'{self.strategy_name}_tns.csv'))
            append_data(file_name=file_name, dict_data=tns_data)
        except Exception as ex:
            self.write_error(u'save_tns 异常:{} {}'.format(str(ex), traceback.format_exc()))

    def send_wechat(self, msg: str):
        """实盘时才发送微信"""
        if self.backtesting:
            return
        self.cta_engine.send_wechat(msg=msg, strategy=self)


