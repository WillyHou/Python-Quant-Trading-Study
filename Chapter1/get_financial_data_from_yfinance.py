import yfinance as yf

# 以取得台積電 2330.TW 的財務報資料為例
# 可以把「2330.TW」換成任意想查詢的公司股票代號
stock = yf.Ticker("2330.TW")

# quarterly_financials = stock.quarterly_financials

# print(quarterly_financials)

print("取得台積電的季度損益表")
print(stock.quarterly_financials)

print("取得台積電的季度資產負債表")
print(stock.quarterly_balance_sheet)

print("取得台積電的季度現金流量表")
print(stock.quarterly_cashflow)

print("取得台積電的年度損益表")
print(stock.financials)

print("取得台積電的年度資產負債表")
print(stock.balance_sheet)

print("取得台積電的年度現金流量表")
print(stock.cashflow)