#%%
# 載入需要的套件
import json
import os
import sys

from alphalens.tears import create_full_tear_sheet
from alphalens.utils import get_clean_factor_and_forward_returns

utils_folder_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(utils_folder_path)
# 載入 Chapter1 資料夾中的 utils.py 模組，並命名為 chap1_utils
import Chapter1.utils as chap1_utils

# 使用 Finlab API token 登入 Finalb 平台，取得資料訪問權限。
chap1_utils.finlab_login()

#%%
analysis_period_start_date = "2017-05-16"
analysis_period_end_date = "2021-05-15"

#%%
"""
Part1. 使用「營業利益」這個因子來做示範
"""

#%%
# 排除指定產業（金融業、金融保險業、存托憑證、建材營造）的股票
# 並排除上市日期晚於 2017-01-03 的股票
top_N_stocks = chap1_utils.get_top_stocks_by_market_value(
    excluded_industry=[
        "金融業",
        "金融保險業",
        "存托憑證",
        "建材營造"
    ],
    pre_list_date="2017-01-03"
)

print(f"股票數量: {len(top_N_stocks)}")  # should be 757

#%% 
# 獲取指定股票代碼列表在給定日期範圍內的每日收盤價資料
# 對應到財報資料時間 2017-Q1~2020-Q4
close_price_date = chap1_utils.get_daily_close_prices_data(
    stock_symbols=top_N_stocks,
    start_date=analysis_period_start_date,
    end_date=analysis_period_end_date,
)
close_price_date.head()
close_price_date.tail()
print(f"股票代碼(欄位名稱): {close_price_date.columns}")
print(f"日期(索引): {close_price_date.index}")

#%%
# 獲取指定因子（營業利益）的資料，並根據每日的交易日將因子資料擴展成日頻資料。
factor_data = chap1_utils.get_factor_data(
    stock_symbols=top_N_stocks,
    factor_name="營業利益",
    trading_days=sorted(list(close_price_date.index))
)
factor_data = factor_data.dropna()
factor_data.head()  
factor_data.tail()
print(f"列出欄位名稱{factor_data.columns}")
print(f"列出索引名稱(日期, 股票代碼): {factor_data.index}")
print(f"列出所有日期: {factor_data.index.get_level_values(0)}")
print(f"列出所有股票代碼: {factor_data.index.get_level_values(1)}")
# %%
