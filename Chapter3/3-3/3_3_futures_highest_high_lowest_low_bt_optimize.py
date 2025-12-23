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

import collections
import collections.abc

# --- 這是您需要新增的程式碼 ---
# 檢查並手動將 Iterable, Mapping 等屬性加回 collections 模組
if not hasattr(collections, 'Iterable'):
    collections.Iterable = collections.abc.Iterable
if not hasattr(collections, 'Mapping'):
    collections.Mapping = collections.abc.Mapping
if not hasattr(collections, 'MutableMapping'):
    collections.MutableMapping = collections.abc.MutableMapping
if not hasattr(collections, 'MutableSequence'):
    collections.MutableSequence = collections.abc.MutableSequence
if not hasattr(collections, 'Sequence'):
    collections.Sequence = collections.abc.Sequence

def option_expiration(date):
    day = 21 - (calendar.weekday(date.year, date.month, 1) + 4) % 7
    return datetime(date.year, date.month, day)  

class High_Low_Strategy(bt.Strategy):
    params = (
        ('period', 18),            # 回溯週期長度
        ('stop_loss_pct', 0.02),   # 停損百分比
        ('exit_pct', 0.02),        # 出場百分比
    )

    def log(self, txt, dt=None):
        '''日誌記錄函數'''
        dt = dt or self.datas[0].datetime.datetime(0)
        print(f"{dt.isoformat()}, {txt}")

    def __init__(self):
        self.datahigh = self.datas[0].high
        self.datalow = self.datas[0].low
        self.dataclose = self.datas[0].close

        # 計算過去 18 根K線的最高價和最低價 (不包含當前K線)
        self.highest_prev = bt.indicators.Highest(self.datahigh(-1), period=self.params.period)
        self.lowest_prev = bt.indicators.Lowest(self.datalow(-1), period=self.params.period)

        self.order = None

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f"""BUY EXECUTED, Price: {order.executed.price:.2f},
                         Cost: {order.executed.value:.2f},
                         ComM: {order.executed.comm:.2f}""")
                self.buycomm = order.executed.comm
            else:
                self.sellprice = order.executed.price
                self.log(f"""SELL EXECUTED, Price: {order.executed.price:.2f},
                         Cost: {order.executed.value:.2f},
                         ComM: {order.executed.comm:.2f}""")
            self.bar_executed = len(self)
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log(f"""OPERATION PROFIT, GROSS {trade.pnl:.2f}, NET {trade.pnlcomm:.2f}""")

    def next(self):
        """
        這是一個標準且邏輯清晰的「海龜交易法則」或「通道突破」策略。

        * 通道: 由過去 period K線的最高價 (highest_prev) 和最低價 (lowest_prev) 組成。

        進場條件 (無持倉時)
        * 做多 (Buy): 當前收盤價 > 前一根K線的通道上軌 (self.highest_prev[-1])。
            * 意義: 價格突破了近期的壓力區，代表上升趨勢可能啟動。
        * 做空 (Sell): 當前收盤價 < 前一根K線的通道下軌 (self.lowest_prev[-1])。
            * 意義: 價格跌破了近期的支撐區，代表下降趨勢可能啟動。

        出場條件 (持有多單時)
        您現在設定了兩種出場機制，它們會各自獨立檢查，確保策略的穩健性：

        1. 追蹤停損 (Trailing Stop) / 趨勢反轉出場:
            * 條件: if self.dataclose[0] < self.highest_prev[-1]:
            * 意義: 價格無法維持在突破後的強勢區，反而跌回了突破前的壓力線 (現在的支撐線)
                以下。這通常被視為突破失敗或趨勢反轉的信號，因此需要出場。這是策略主要的停利和趨勢跟隨機制。

        2. 固定停損 (Fixed Stop-Loss) / 風險控制:
            * 條件: stop_loss_price = entry_price * (1 - self.params.stop_loss_pct)
            * 意義: 這是一個絕對的防守底線。無論趨勢指標如何變化，只要虧損達到了您預設的 stop_loss_pct (例如
                2%)，就立即無條件離場。它的核心目的是控制單筆交易的最大虧損，防止發生預期外的巨大風險。
        """
        if self.order:
            return # 正在等待訂單執行

        status = None
        position_size = self.getposition().size
        # print(f"position_size: {position_size}")

        if (
            option_expiration(self.datas[0].datetime.date(0)).day
            == self.datas[0].datetime.datetime(0).day
        ):
            if self.datas[0].datetime.datetime(0).day >= 13:
                status = "end"
                if position_size != 0:
                    self.close()
                    self.log("Expired and Create Close Order")
        
        # 進場條件
        if status != "end": # 如果不是到期日
            if not  position_size:
                if self.datahigh[0] > self.highest_prev[0]:
                    self.order = self.buy()
                    self.log("創建買單")
                elif self.datalow[0] < self.lowest_prev[0]:
                    self.order = self.sell()
                    self.log("創建賣單")
            else:
                # 取得目前持倉的成本價
                entry_price = self.getposition().price
                # 計算出場價和止損價
                if position_size > 0:  # 多頭持倉
                    # exit_price = entry_price * (1 + self.params.exit_pct)
                    # stop_loss_price = entry_price * (1 - self.params.stop_loss_pct)
                    exit_price = self.lowest_prev[0] + (self.dataclose[0] * self.params.exit_pct)
                    stop_loss_price = entry_price - (self.dataclose[0] * self.params.stop_loss_pct) 

                    # 出場條件
                    if self.dataclose[0] >= exit_price:
                        self.order = self.close()
                        self.log("平多單 - 出場條件達成")
                    # 停損條件
                    elif self.dataclose[0] <= stop_loss_price:
                        self.order = self.close()
                        self.log("平多倉 - 止損條件達成")

                elif position_size < 0:  # 空頭持倉
                    exit_price = self.highest_prev[0] - (self.dataclose[0] * self.params.exit_pct)
                    stop_loss_price = entry_price + (self.dataclose[0] * self.params.stop_loss_pct)

                    # 出場條件
                    if self.datalow[0] <= exit_price:
                        self.order = self.close()
                        self.log("平空單 - 出場條件達成")
                    # 停損條件
                    elif self.dataclose[0] >= stop_loss_price:
                        self.order = self.close()
                        self.log("平空單 - 止損條件達成")

cerebro = bt.Cerebro()
# /Users/houguanyu/Documents/code/python/stock/Quant/PythonQuantrading/Chapter3/3-3
df = pd.read_csv('/Users/houguanyu/Documents/code/python/stock/Quant/PythonQuantrading/Chapter3/3-3/TXF_30.csv')
df = df.dropna()
df['Date'] = pd.to_datetime(df['Date'])
df.index= df['Date']
df = df.between_time('08:45','13:45')
data_feed = bt.feeds.PandasData(
    dataname=df,
    name='TXF',
    datetime=0,
    high=2,
    low=3,
    open=1,
    close=4,
    volume=5,
    plot=False
)

cerebro.adddata(data_feed, name='TXF')

# 參數優化範圍
period_values = [3, 5, 10, 15, 18, 25, 50, 90, 150]
stop_loss_pct_values = [0.01, 0.02, 0.03, 0.04, 0.05]
exit_pct_value = [0.01, 0.02, 0.03, 0.04, 0.05]

# 新增策略的排列組合
cerebro.optstrategy(High_Low_Strategy,
                    period=period_values,
                    stop_loss_pct=stop_loss_pct_values,
                    exit_pct=exit_pct_value
                    )

# 設定初始資金和交易成本
cerebro.broker.setcash(300000.0)
cerebro.broker.setcommission(commission=200, margin=167000, mult=200)

# 新增PyFolio分析器
cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')

results = cerebro.run(maxcpus=1)

# 儲存結果為 Excel 檔案
output = []
for result in results:
    strat = result[0]
    pyfoliozer = strat.analyzers.getbyname('pyfolio')
    returns, positions, transactions, gross_lev = pyfoliozer.get_pf_items()

    cum_return = ep.cum_returns_final(returns)
    sharpe_ratio = ep.sharpe_ratio(returns)
    mdd = ep.max_drawdown(returns)

    output.append({
        'period': strat.params.period,
        'stop_loss_pct': strat.params.stop_loss_pct,
        'exit_pct': strat.params.exit_pct,
        'cum_return': cum_return,
        'sharpe_ratio': sharpe_ratio,
        'max_drawdown': mdd
    })


df_output = pd.DataFrame(output)
df_output.to_excel('futures_highest_high_lowest_low_bt_optimize_results.xlsx', index=False)
print('Optimization results saved to futures_highest_high_lowest_low_bt_optimize_results.xlsx')

                 