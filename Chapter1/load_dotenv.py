import os

from dotenv import load_dotenv

# 載入 .env 檔案中的環境變數
load_dotenv()

# 取得並顯示 TOKEN1 的值
TOKEN1 = os.getenv("TOKEN1")
print(f"TOKEN1: {TOKEN1}")