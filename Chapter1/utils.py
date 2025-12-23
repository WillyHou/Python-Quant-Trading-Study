import os
from datetime import datetime
from typing import List, Tuple
from dotenv import load_dotenv
import finlab
from finlab import data
import yfinance as yf
import pandas as pd
from pandas.core.indexes.datetimes import DatetimeIndex

def finlab_login() -> None:
    """
    使用 Finlab API token 登入 Finlab 
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 載入 .env檔案中定義的變數
    load_dotenv(f"{current_dir}/.env")
    api_token = os.getenv("FINLABTOKEN")
    # 使用 API Token 登入 Finlab 量化平台
    finlab.login(api_token=api_token)

def get_top_stocks_by_market_value(
    excluded_industry: List[str],
    pre_list_date: str,
    top_n: int = None # type: ignore
):
    """
    篩選市值前 N 大的上市公司股票代號，並以列表形式回傳。篩選過程包括以下條件：
    1. 排除特定產業的公司(excluded_industry)。
    2. 僅篩選上市日期早於特定日期(pre_list_date)的公司。
    3. 選擇是值前 N 大的公司

    :params excluded_industry: 需要排除的特定產業類別列表
    :params pre_list_date: 上市日期須早於此特定日期
    :params top_n: 市值前 N 大的公司
    """
    # 從 Finlab 取得公司基本資訊表，內容包括公司股票代號、公司名稱、上市日期和產業類別
    company_info = data.get("company_basic_info")[
        ["stock_id", "公司名稱", "上市日期", "產業類別", "市場別"]
    ]

    # 如果有特定要排除的類別，則過濾掉這些產業的公司
    if excluded_industry:
        company_info = company_info[~company_info["產業類別"].isin(excluded_industry)]
    # 如果有設定上市日期條件，則過濾掉上市日期晚於指定日期的公司
    if pre_list_date:
        company_info = company_info[company_info["市場別"] == "sii"]
        company_info = company_info[company_info["上市日期"] < pre_list_date]
    # 如果有設定 top_n，則選取市值前 N 大的公司股票代碼
    if top_n:
        # 從 Finlab 取得最新的個股是值數據表，並重設索引名稱為 market_value
        market_value = pd.DataFrame(data.get("etl:market_value"))
        market_value = market_value[market_value.index == pre_list_date]
        market_value = market_value.reset_index().melt(
            id_vars="date", var_name="stock_id", value_name="market_value"
        )
        # 將市值數據表與公司資訊表根據股票代碼欄位(stock_id)進行合併，
        # 並根據市值欄位(market_value)將公司由大到小排序
        company_info = pd.merge(market_value, company_info, on="stock_id").sort_values(
            by="market_value", ascending=False
        )
        return company_info.head(top_n)["stock_id"].tolist()
    else:
        return company_info["stock_id"].tolist()

# print(
#     get_top_stocks_by_market_value(
#         excluded_industry=["建材營造"],
#         pre_list_date="2017-01-03",
#         top_n=100,
#     )
# )

def get_daily_close_prices_data(
    stock_symbols: List[str],
    start_date: str = "",
    end_date: str = "",
    is_tw_stock: bool = True
) -> pd.DataFrame:
    """
    獲取指定股票清單(stock_symbols)在給定日期範圍內(start_date~end_date)每日收盤價資料。
    Args:
        stock_symbols: 股票代碼列表
        start_date: 起始日期, "YYYY-MM-DD"
        end_date: 結束日期
        is_tw_stock: stock_symbols 是否是台灣股票

    Returns:
        pd.DataFrame: 
            每日股票收盤價資料表,
            索引是日期(DatetimeIndex格式),
            欄位名稱包含股票代號,
    """
    # 如果是台灣股票，則在每個股票代碼後加上 ".TW"
    if is_tw_stock:
        stock_symbols = [
            f"{symbol}.TW" if ".TW" not in symbol else symbol
            for symbol in stock_symbols
        ]
    # 從 YFinance 下載指定股票在給定日期範圍內的數據，並取出收盤價欄位(Close)的資料
    full_data = yf.download(stock_symbols, start=start_date, end=end_date)

    if full_data is None:
        raise ValueError("yfinance 返回 None，請檢查網路連接或股票代碼")
    if full_data.empty:
        raise ValueError("獲取的數據為空，請檢查股票代碼或日期範圍")
    # 取出收盤價資料
    stock_data = full_data["Close"]

    # 如果只有一支股票，將其轉為 DataFrame 並設定欄位名稱為該股票代碼
    if isinstance(stock_data, pd.Series):
        stock_data = stock_data.to_frame()
        stock_data.columns = stock_symbols
    # 使用向前填補方法處理資料中的缺失值
    stock_data = stock_data.ffill()
    # 將欄位名稱中的 ".TW" 移除，只保留股票代碼
    stock_data.columns = stock_data.columns.str.replace(".TW", "", regex=False)
    return stock_data

# print(
#     get_daily_close_prices_data(
#         stock_symbols=["2330", "1101"],
#         start_date="2022-01-01",
#         end_date="2022-01-08",
#         is_tw_stock=True,
#     )
# )

def get_factor_data(
    stock_symbols: List[str],
    factor_name: str,
    trading_days: List[DatetimeIndex] = None # type: ignore
) -> pd.DataFrame:
    """
    從 Finlab 獲取指定股票清單(stock_symbols)的單個因子(factor_name)資料，
    並根據需求擴展至交易日頻率資料或是回傳原始季頻率資料。
    如果沒有指定交易日(trading_days)，則回傳原始季頻率資料。
    Args:
        stock_symbols: 股票代碼列表
        factor_name: 因子名稱
        trading_days: 如果有指定日期，就會將資料的頻率從季頻率擴充成此交易日頻率
    Returns:
        pd.DataFrame:
            有指定trading_days, 回傳多索引資料表,索引是datatime和asset, 欄位包含value(因子值)。
            未指定trading_days, 回傳原始 Finlab 因子資料表,索引是datetime, 欄位包含股票代號。
    """
    # 從 Finlab 獲取指定因子資料表，並藉由加上 .deadline() 將索引格式轉為財報截止日
    factor_data = data.get(f"fundamental_features:{factor_name}").deadline() # type: ignore
    # 如果指定了股票代碼列表，則篩選出特定股票的因子資料
    if stock_symbols:
        # 找出 stock_symbols 中確實存在於 factor_data 欄位裡的股票
        available_stocks = [s for s in stock_symbols if s in factor_data.columns]
        
        if not available_stocks:
            # 如果沒有任何一個指定的股票有數據，則拋出錯誤
            raise ValueError(f"指定的股票清單中，沒有任何股票有 '{factor_name}' 的數據。無法進行數據獲取")
        
        # factor_data = factor_data[stock_symbols]
        factor_data = factor_data[available_stocks]
    # 如果指定了交易日，則將「季度頻率」的因子資料擴展至「交易日頻率」的資料，
    # 否則回傳原始資料
    if trading_days is not None:
        factor_data = factor_data.reset_index()
        factor_data = extend_factor_data(
            factor_data=factor_data, trading_days=trading_days
        )
        # 使用 melt 轉換資料格式
        factor_data = factor_data.melt(
            id_vars="index", var_name="asset", value_name="value"
        )
        # 重命名欄位名稱，且根據日期、股票代碼進行排序，最後設定多重索引 datatime 和 asset
        factor_data = (factor_data.rename(columns={"index": "datetime"})
                       .sort_values(by=["datetime", "asset"])
                       .set_index(["datetime", "asset"]))
    return factor_data

def extend_factor_data(
    factor_data: pd.DataFrame,
    trading_days: List[DatetimeIndex] # pd.DataFrame
):
    """
    將因子資料(factor_data)擴展至交易日頻率(trading_days)資料，使用向前填補的方式補值
    Args:
        factor_data: 
            為擴充前的因子資料表，
            欄位名稱包含index(日期欄位名稱)和股票代碼
        trading_days:
            填補後的因子資料表，
            欄位名稱包含index(日期欄位名稱)和股瞟代碼
    """
    # 將交易日列表轉換為 DataFrame 格式，索引為指定的交易日列表
    trading_days_df = pd.DataFrame(trading_days, columns=["index"])
    # 將交易日資料與因子資料進行合併，以交易日資料有的日期為主
    extended_data = pd.merge(trading_days_df, factor_data, on="index", how="outer")
    extended_data = extended_data.ffill()
    # 最後只回傳在和 trading_days_df 時間重疊的資料
    extended_data = extended_data[
        (extended_data["index"] >= min(trading_days_df["index"]))
        & (extended_data["index"] <= max(trading_days_df["index"]))
    ]
    return extended_data

trading_days = pd.date_range(start="2020-01-01", end="2020-12-01", freq="D")
# print(
#     get_factor_data(
#         stock_symbols=[
#             "2330",
#             "3017"
#         ],
#         factor_name="營業利益",
#         trading_days=trading_days, # type: ignore
#     )
# )

# print(
#     get_factor_data(
#         stock_symbols=[
#             "2330",
#             "1101",
#             # '9103', 
#             # '910322', 
#             # '9105', 
#             # '910861', 
#             # '9110', 
#             # '911608', 
#             # '911622', 
#             # '911868', 
#             # '912000', 
#             # '9136'
#         ],
#         factor_name="營業利益",
#         trading_days=None, # type: ignore
#     )
# )

def convert_quarter_to_dates(quarter: str) -> Tuple[str, str]:
    """
    將季度字串(quarter)轉換為起始和結束日期字串
    ex: 2013-Q1 -> 2013-05-16, 2013-08-14
    Args:
        quarter: 年-季度字串，例如：2013-Q1
    Returns:
        Tuple[str,str]: 季度對應的起始和結束日期字串
    """
    try:
        year_str, qtr = quarter.split("-")
        year = int(year_str)  # 嘗試將年份轉換為整數，以補獲非整數年份
    except (ValueError, IndexError) as exc: # 處理 split 失敗或 int 轉換失敗的情況
        raise ValueError("無效的季度格式。請使用 'YYYY-QX' 格式，例如 '2013-Q1'。") from exc
    if qtr == "Q1":
        return f"{year}-05-16", f"{year}-08-14"
    if qtr == "Q2":
        return f"{year}-08-15", f"{year}-11-14"
    if qtr == "Q3":
        return f"{year}-11-15", f"{int(year) + 1}-03-31"
    if qtr == "Q4":
        return f"{int(year) + 1}-04-01", f"{int(year) + 1}-05-15"
    else: # 捕獲所有不符合 Q1-Q4 的情況
        raise ValueError(f"無效的季度 '{qtr}'。季度必須是 'Q1', 'Q2', 'Q3', 'Q4'。 ")

# print(convert_quarter_to_dates(quarter='2013-Q2'))

def convert_date_to_quarter(date: str) -> str:
    """
    將日期字串(date)轉換為季度字差。
    ex: 2013-05-16 -> 2013-Q1
    yyyy-mm-dd -> yyyy-q
    
    Args:
        date: 日期字串，格式為 YYYY-MM-DD
    Returns:
        str: 對應的季度字串
    """
    # 將字串轉換為日期格式
    try:
        date_obj = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"無效的日期格式 '{date}'。請使用 'YYYY-MM-DD'格式  ") from exc

    year, month, day = (
        date_obj.year,
        date_obj.month,
        date_obj.day,
    )  # 獲取年份、月份和日期
    # 根據日期判斷所屬的季度並回傳相應燈季度字串
    if month == 5 and day >= 16 or month in [6, 7] or (month == 8 and day <= 14):
        return f"{year}-Q1"
    elif month == 8 and day >=15 or month in [9, 10] or (month == 11 and day <= 14):
        return f"{year}-Q2"
    elif month == 11 and day >= 15 or month in [12]:
        return f"{year}-Q3"
    elif (month == 1) or (month == 2) or (month == 3 and day <= 31):
        return f"{year-1}-Q3"
    elif month == 4 or (month ==5 and day <= 15):
        return f"{year-1}-Q4"
    raise ValueError(f"日期 '{date}' 不在任何已定義的季度範圍內。請檢查日期或季度定義。")

# print(convert_date_to_quarter(date="2013-05-16"))
# print(convert_date_to_quarter(date="2013-08-14"))
# print(convert_date_to_quarter(date="2013-08-15"))
# print(convert_date_to_quarter(date="2013-11-14"))
# print(convert_date_to_quarter(date="2013-11-15"))
# print(convert_date_to_quarter(date="2014-03-31"))
# print(convert_date_to_quarter(date="2014-04-01"))
# print(convert_date_to_quarter(date="2014-05-15"))

def rank_stocks_by_factor(
    factor_df: pd.DataFrame,
    positive_corr: bool,
    rank_column: str,
    rank_result_column: str
) -> pd.DataFrame:
    """
    根據某個指定因子的值(rank_column)對股價進行排序，
    遞增或遞減排序方式取決於因子與未來收益的相關性(positive_corr)。
    如果相關性為正，則將股票按因子值由小排到大;如果為負，則按因子值由大到小排序。
    最後，將排序結果新增至原始因子資料表中，且指定排序結果欄位名稱為 rank_result_column。

    Args:
        factor_df: 因子資料表，
                   欄位名稱寒 asset(股票代碼欄位)、datetime(日期欄位)、value(因子值欄位)
        positive_corr: 因子與收益的相關性，正相關為 True, 負相關為 False
        rank_column: 用於排序的欄位名稱
        rank_result_column: 保存排序結果的欄位名稱

    """
    # 複製因子資料表，以避免對原資料進行修改
    ranked_df = factor_df.copy()

    # 將 datetime 欄位設置為索引
    ranked_df = ranked_df.set_index("datetime")
    # 針對每一天的資料，根據指定的因子欄位進行排名
    # 如果因子與收益正相關，則根據因子值由小到大排名
    # 如果因子與收益負相關，則根據因子值由大到小排名
    ranked_df[rank_result_column] = ranked_df.groupby(level="datetime")[
        rank_column].rank(ascending=positive_corr)
    ranked_df = ranked_df.fillna(0)
    ranked_df.reset_index(inplace=True)
    return ranked_df

# trading_days = pd.date_range(start="2020-01-01", end="2020-01-03", freq="D")
# test_factor_df = get_factor_data(
#     stock_symbols=["2330", "6593", "2615", "6285"],
#     factor_name="營業利益",
#     trading_days=trading_days # type: ignore
# )
# test_factor_df = test_factor_df.reset_index()
# a = rank_stocks_by_factor(
#     factor_df=test_factor_df,
#     positive_corr=True,
#     rank_column="value",
#     rank_result_column="rank"
# )
# print(a)

# b = rank_stocks_by_factor(
#     factor_df=test_factor_df,
#     positive_corr=False,
#     rank_column="value",
#     rank_result_column="rank"
# )
# print(b)

def calculate_weighted_rank(
    ranked_dfs: List[pd.DataFrame],
    weights: List[float],
    positive_corr: bool,
    rank_column: str
) -> pd.DataFrame:
    """
    根據多個因子的加權排名計算最終的股票排名
    len(ranked_dfs) 會等於 len(weights)

    Args:
        ranked_dfs; 多個包含因子排名資料表的list
        weights: 對應於各因子權重的list
        positive_corr: 因子與收益相關性的list，正相關為 True, 負相關為 False
        rank_column: 用於排序的欄位名稱

    Returns: 
        pd.DataFrame: 包含加權排序結果的資料表，欄位名稱含 asset (股票代碼)、datetime(日期)和加權排名結果(weighted_rank)
    """
    # 檢查 ranked_dfs 和 weights 的長度是否相同，否則拋出錯誤
    # 也就是有 n 個因子資料就需要有 n 個權重值
    if len(ranked_dfs) != len(weights):
        raise ValueError("ranked_dfs 和 weights 的長度必須相同。")
    # 初始化 combined_ranks 為空的 DataFrame，用來儲存加權後的排名結果
    combined_ranks = pd.DataFrame()
    # 遍歷每個因子排名資料表及其對應的權重
    for i, df in enumerate(ranked_dfs):
        # 將每個因子的排名乘以對應的權重，並存入新的欄位 rank_i
        df[f"rank_{i}"] = df[rank_column] * weights[i]
        if combined_ranks.empty:
            combined_ranks = df[["datetime", "asset", f"rank_{i}"]]
        else:
            # 根據 datetime 和 asset 這兩個欄位將資料進行合併
            combined_ranks = pd.merge(
                combined_ranks,
                df[["datetime", "asset", f"rank_{i}"]],
                on=["datetime", "asset"],
                how="outer",
            )
    # 將合併後的資料中遺失值刪除
    combined_ranks = combined_ranks.dropna()
    # 最後，將所有乘上權重的排名進行每個股票每日的加總，得到最終的排名
    combined_ranks["weighted"] = combined_ranks.filter(like="rank_").sum(axis=1)
    # 根據加權總分計算最終的股票排名
    # 使用 rank_stocks_by_factor 函數對加權排名結果進行排序
    ranked_df = rank_stocks_by_factor(
        factor_df=combined_ranks,
        positive_corr=positive_corr,
        rank_column="weighted",
        rank_result_column="weighted_rank"
    )
    return ranked_df[["datetime", "asset", "weighted_rank"]]

# trading_days = pd.date_range(start="2020-01-01", end="2020-01-02", freq="D")
# test_factor_data = get_factor_data(
#     stock_symbols=["2330", "3017"],
#     factor_name="營業利益",
#     trading_days=trading_days # type: ignore
# )
# # print(test_factor_data)
# test_factor_data = test_factor_data.reset_index()

# test_2factors_data_list = [
#     rank_stocks_by_factor(
#         factor_df=test_factor_data,
#         positive_corr=True,
#         rank_column="value",
#         rank_result_column="rank",
#     ),
#     rank_stocks_by_factor(
#         factor_df=test_factor_data,
#         positive_corr=True,
#         rank_column="value",
#         rank_result_column="rank"
#     )
# ]
# result = calculate_weighted_rank(
#     ranked_dfs=test_2factors_data_list,
#     weights=[1 / 2, 1 / 2],
#     positive_corr=True,
#     rank_column="rank"
# )
# print(result)

def get_daily_OHLCV_data(
    stock_symbols: List[str],
    start_date: str,
    end_date: str,
    is_tw_stock: bool = True
) -> pd.DataFrame:
    """
    取得指定股票(stock_symbols)在給定日期範圍內(start_date~end_date)的每日價量資料。

    Args:
        stock_symbols: 股票代碼list
        start_date: 起始日期
        end_date: 結束日期
        is_tw_stock: stock_symbol 是否是台灣股票

    Returns: 
        pd.DataFrame: 價量的資料集、欄位名稱包含股票代碼、日期、開高低收量
    """
    # 如果是台灣股票，則在股票代碼後加上 ".TW"
    if is_tw_stock:
        stock_symbols = [
            f"{symbol}.TW" if ".TW"  not in symbol else symbol 
            for symbol in stock_symbols
        ]
    # 使用 pd.concat 合併多隻股票的數據
    all_stock_data = pd.concat(
        [
            # 從 YFinance 下載每隻股票在指定日期範圍內的數據
            pd.DataFrame(yf.download(symbol, start=start_date, end=end_date)).droplevel(
                "Ticker", axis=1
            ).assign(asset=symbol.split(".")[0])  # 新增一個 "asset" 的欄位，用來儲存股票代碼
            .reset_index().rename(columns={"Date": "datetime"})  # 重設索引並將日期欄位名稱從 Date 改為 datetime
            .ffill()  # 使用向前填補的方法處理資料中的缺失值
            for symbol in stock_symbols
            
        ]
    )
    all_stock_data.columns.name = None
    all_stock_data = all_stock_data[
        ["Open", "High", "Low", "Close", "Volume", "datetime", "asset"]
    ]
    return all_stock_data.reset_index(drop=True)  # type: ignore

# print(
#     get_daily_OHLCV_data(
#         stock_symbols=["2330"],
#         start_date="2020-01-01",
#         end_date="2020-01-08",
#         is_tw_stock=True
#     )
# )
# print(
#     get_daily_OHLCV_data(
#         stock_symbols=["2330"],
#         start_date="2022-01-01",
#         end_date="2022-01-08",
#         is_tw_stock=True
#     )
# )

def list_factors_by_type(data_type: str) -> List[str]:
    """
    根據資料型態列出所有相關的因子名稱。
    Args:
        data_type: 資料型態, 例如：fundamental_features
    Returns:
        List[str]: 該資料型態下的所有項目列表
    """
    return list(
        data.search(keyword=data_type, display_info=["name", "description", "items"])[
            0
        ]["items"]
    )

# print(list_factors_by_type(data_type="fundamental_features"))
# print(list_factors_by_type(data_type="price"))
# print(list_factors_by_type(data_type="institutional_investors")) # 法人買超賣超數據
