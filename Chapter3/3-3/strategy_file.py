# import backtrader as bt
# import calendar
import datetime
import backtrader as bt
import pandas as pd
import calendar
from datetime import datetime
import empyrical as ep
import pyfolio as pf
import itertools
import warnings
warnings.filterwarnings('ignore')

def option_expiration(date):
    day = 21 - (calendar.weekday(date.year, date.month, 1) + 4) % 7
    return datetime(date.year, date.month, day)  # type: ignore

class MA_Volume_Strategy(bt.Strategy):
    params = (
        ('ma_short', 5),
        ('ma_medium', 20),
        ('ma_long', 60),
        ('stop_loss_pct', 0.02),
        ('take_profit_pct', 0.02),
    )

    def log(self, txt, dt=None):
        '''日誌記錄函數'''
        dt = dt or self.datas[0].datetime.datetime(0)
        print(f"{dt.isoformat()}, {txt}")

    def __init__(self):
        # 收盤價
        self.dataclose = self.datas[0].close

        # 成交量
        self.datavolume = self.datas[0].volume

        # 移動平均線
        self.ma_short = bt.indicators.SMA(self.dataclose, period=self.params.ma_short)
        self.ma_medium = bt.indicators.SMA(self.dataclose, period=self.params.ma_medium)
        self.ma_long = bt.indicators.SMA(self.dataclose, period=self.params.ma_long)

        # 成交價移動平均線
        self.vol_ma_short = bt.indicators.SMA(self.datavolume, period=self.params.ma_short)
        self.vol_ma_long = bt.indicators.SMA(self.datavolume, period=self.params.ma_long)

        self.order = None

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return 
        if order.status in [order.Complete]:
            if order.isbuy():
                self.log(f"""BUY EXECUTED, Price: {order.executed.price:.2f},
                         Cost: {order.executed.value:.2f},
                         Comm: {order.executed.comm:.2f}""")
                self.buycomm = order.executed.comm
            else:
                self.sellprice = order.executed.price
                self.log(f"""SELL EXECUTED, Price: {order.executed.price:.2f},
                         Cost: {order.executed.value:.2f},
                         Comm: {order.executed.comm:.2f}""")
            self.bar_executed = len(self)
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return 
        self.log(f"OPERATION PROFIT, GROSS{trade.pnl:.2f}, NET{trade.pnlcomm:.2f}")

    def next(self):
        if self.order:
            return 
        
        position_size = self.getposition().size
        position_price = self.getposition().price

        status = None
        if (
            option_expiration(self.datas[0].datetime.datetime(0))
            == self.datas[0].datetime.datetime(0).day
        ):
            if self.datas[0].datetime.datetime(0).hour >= 13:
                status = "end"
                if position_size != 0:
                    self.close()
                    self.log("Expired and Create Close Order")

            if status != "end":
                if not position_size:
                    # 多頭進場條件
                    if (self.dataclose[0] > self.ma_short[0] and
                        self.dataclose[0] > self.ma_medium[0] and 
                        self.dataclose[0] > self.ma_long[0] and 
                        self.vol_ma_short[0] > self.vol_ma_long[0]):
                        self.order = self.buy()
                        self.log("創建買單")
                    # 空頭進場條件
                    elif (self.dataclose[0] < self.ma_short[0] and
                          self.dataclose[0] < self.ma_medium[0] and 
                          self.dataclose[0] < self.ma_long[0] and 
                          self.vol_ma_short[0] < self.vol_ma_long[0]):
                        self.order = self.sell()
                        self.log("創建賣單")
                else:
                    # 已有持倉，檢查出場條件
                    if position_size > 0:
                        stop_loss_price = position_price * (1 - self.params.stop_loss_pct)
                        taks_profit_price = position_price * (1 + self.params.take_profit_pct)
                        # 多頭持倉
                        if self.dataclose[0] <= stop_loss_price:
                            self.oreder = self.close()
                            self.log("平多單 - 止損")
                        elif self.dataclose[0] >= taks_profit_price:
                            self.order = self.close()
                            self.log("平多單 - 停利")

                    elif position_size < 0:
                        stop_loss_price = position_price * (1 + self.params.stop_loss_pct)
                        taks_profit_price = position_price * (1 - self.params.take_profit_pct)
                        # 空頭持倉
                        if self.dataclose[0] <= taks_profit_price:
                            self.order = self.close()
                            self.log("平空單 - 停利")
                        elif self.dataclose[0] >= stop_loss_price:
                            self.order = self.close()
                            self.log("平空單 - 止損")