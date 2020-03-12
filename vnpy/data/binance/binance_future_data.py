# 币安合约数据

from typing import Dict, List, Any
from datetime import datetime, timedelta
from vnpy.api.rest.rest_client import RestClient
from vnpy.trader.object import (
    Interval,
    Exchange,
    BarData,
    HistoryRequest
)

BINANCE_INTERVALS = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]

INTERVAL_VT2BINANCEF: Dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1h",
    Interval.DAILY: "1d",
}

TIMEDELTA_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}

REST_HOST: str = "https://fapi.binance.com"

class BinanceFutureData(RestClient):

    def __init__(self, parent=None):
        super().__init__()
        self.parent = parent
        self.init(url_base=REST_HOST)

    def write_log(self, msg):
        """日志"""
        if self.parent and hasattr(self.parent, 'write_log'):
            func = getattr(self.parent, 'write_log')
            func(msg)
        else:
            print(msg)

    def get_interval(self, interval, interval_num):
        """ =》K线间隔"""
        t = interval[-1]
        b_interval = f'{interval_num}{t}'
        if b_interval not in BINANCE_INTERVALS:
            return interval
        else:
            return b_interval

    def get_bars(self,
                req: HistoryRequest,
                return_dict=True,
                ) -> List[Any]:
        """获取历史kline"""
        bars = []
        limit = 1000
        start_time = int(datetime.timestamp(req.start))
        b_interval = self.get_interval(INTERVAL_VT2BINANCEF[req.interval], req.interval_num)
        while True:
            # Create query params
            params = {
                "symbol": req.symbol,
                "interval": b_interval,
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
                "/fapi/v1/klines",
                data={},
                params=params
            )

            # Break if request failed with other status code
            if resp.status_code // 100 != 2:
                msg = f"获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
                self.write_log(msg)
                break
            else:
                datas = resp.json()
                if not datas:
                    msg = f"获取历史数据为空，开始时间：{start_time}"
                    self.write_log(msg)
                    break

                buf = []
                begin, end = None, None
                for data in datas:
                    dt = datetime.fromtimestamp(data[0] / 1000)  # convert to second
                    if not begin:
                        begin = dt
                    end = dt
                    if return_dict:
                        bar = {
                            "datetime": dt,
                            "symbol": req.symbol,
                            "exchange": req.exchange.value,
                            "vt_symbol": f'{req.symbol}.{req.exchange.value}',
                            "interval": req.interval.value,
                            "volume": float(data[5]),
                            "open_price": float(data[1]),
                            "high_price": float(data[2]),
                            "low_price": float(data[3]),
                            "close_price": float(data[4]),
                            "gateway_name": "",
                            "open_interest": 0,
                            "trading_day": dt.strftime('%Y-%m-%d')
                        }
                    else:
                        bar = BarData(
                            symbol=req.symbol,
                            exchange=req.exchange,
                            datetime=dt,
                            trading_day=dt.strftime('%Y-%m-%d'),
                            interval=req.interval,
                            volume=float(data[5]),
                            open_price=float(data[1]),
                            high_price=float(data[2]),
                            low_price=float(data[3]),
                            close_price=float(data[4]),
                            gateway_name=self.gateway_name
                        )
                    buf.append(bar)

                bars.extend(buf)

                msg = f"获取历史数据成功，{req.symbol} - {b_interval}，{begin} - {end}"
                self.write_log(msg)

                # Break if total data count less than limit (latest date collected)
                if len(datas) < limit:
                    break

                # Update start time
                start_dt = end + TIMEDELTA_MAP[req.interval] * req.interval_num
                start_time = int(datetime.timestamp(start_dt))

        return bars

    def export_to(self, bars, file_name):
        """导出bar到文件"""
        if len(bars) == 0:
            self.write_log('not data in bars')
            return

        import pandas as pd
        df = pd.DataFrame(bars)
        df = df.set_index('datetime')
        df.index.name = 'datetime'
        df.to_csv(file_name, index=True)
        self.write_log('保存成功')
