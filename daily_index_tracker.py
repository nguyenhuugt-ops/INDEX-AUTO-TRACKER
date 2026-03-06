import yfinance as yf
from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import numpy as np
import datetime
import os
import time
import requests
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION ---
output_dir = r"E:\OneDrive" if os.path.exists(r"E:\OneDrive") else "."
file_path = os.path.join(output_dir, "Index tracking.xlsx")

# --- 1. INITIALIZE DATA ---
today_str = datetime.datetime.now().strftime('%d/%m/%Y')

cols = ["DXY", "US10Y (%)", "VN10Y (%)", "VNIBOR qua đêm (%)", 
        "KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)", "TỶ GIÁ USD bán ra VCB", 
        "USDT.D", "US Spot ETF Net Inflow (USDm)", "XAUXAG", "VIX", 
        "giá Vàng", "giá Bạc (USD)", "BTC", "E1VFVN30 (VND)"]

if os.path.exists(file_path):
    print(f"Reading existing file: {file_path}")
    try:
        df_raw = pd.read_excel(file_path, index_col=0).T
        df = df_raw.copy()
        df['Date_str'] = df.index.astype(str)
    except:
        print("Error reading file, creating new dataframe...")
        dates = pd.date_range(start="2026-01-01", end=datetime.datetime.now() + datetime.timedelta(days=30), freq='D')
        df = pd.DataFrame(index=dates)
        df['Date_str'] = df.index.strftime('%d/%m/%Y')
        for col in cols:
            df[col] = np.nan
else:
    print("Creating new dataframe...")
    dates = pd.date_range(start="2026-01-01", end=datetime.datetime.now() + datetime.timedelta(days=30), freq='D')
    df = pd.DataFrame(index=dates)
    df['Date_str'] = df.index.strftime('%d/%m/%Y')
    for col in cols:
        df[col] = np.nan

# --- 2. OVERRIDES & MANUAL DATA ---
history_overrides = {
    'KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)': {
        '02/03/2026': 767.00, '03/03/2026': -125.4, '04/03/2026': -1695.9, '05/03/2026': -3124.33, '06/03/2026': -107.5
    },
    'US Spot ETF Net Inflow (USDm)': {
        '02/03/2026': 94.0,   '03/03/2026': 114.7,  '04/03/2026': 155.3,  '05/03/2026': -139.2,
    }
}

for col, dates_dict in history_overrides.items():
    if col in df.columns:
        for d_str, val in dates_dict.items():
            df.loc[df['Date_str'] == d_str, col] = round(float(val), 2)

# --- 3. DATA FETCHING ---
def fetch_yf_data():
    print("Fetching YFinance data...")
    yf_map = {"DX-Y.NYB": "DXY", "^TNX": "US10Y (%)", "GC=F": "giá Vàng", "SI=F": "giá Bạc (USD)", "BTC-USD": "BTC", "^VIX": "VIX", "E1VFVN30.VN": "E1VFVN30 (VND)"}
    res = {}
    for t, c in yf_map.items():
        try:
            v_download = yf.download(t, period="1d", progress=False)
            if not v_download.empty:
                v = v_download['Close'].iloc[-1]
                val = float(v.iloc[0]) if isinstance(v, pd.Series) else float(v)
                res[c] = round(val, 2)
        except: res[c] = np.nan
    return res

def fetch_tv_data():
    print("Fetching TradingView data (VN10Y, USDT.D)...")
    res = {"VN10Y (%)": np.nan, "USDT.D": np.nan}
    try:
        tv = TvDatafeed()
        v10 = tv.get_hist(symbol='VN10Y', exchange='TVC', interval=Interval.in_daily, n_bars=3)
        if v10 is not None and not v10.empty:
            res["VN10Y (%)"] = round(float(v10['close'].iloc[-1]), 2)
        
        ud = tv.get_hist(symbol='USDT.D', exchange='CRYPTOCAP', interval=Interval.in_daily, n_bars=3)
        if ud is not None and not ud.empty:
            res["USDT.D"] = round(float(ud['close'].iloc[-1]), 2)
    except Exception as e:
        print(f"TV data error: {e}")
    return res

def fetch_selenium_data():
    print("Running Selenium tasks (Fireant)...")
    res = {}
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.get("https://fireant.vn/dashboard")
        el = WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(),'GT Mua-Bán')]/following::span[1]")))
        val = float(el.text.replace(',', '').replace('+', ''))
        res["KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)"] = round(val, 2)
        driver.quit()
    except Exception as e:
        print(f"Selenium error: {e}")
    return res

# --- EXECUTE ---
updates = fetch_yf_data()
updates.update(fetch_tv_data())
updates.update(fetch_selenium_data())

# Update Today's row
today_idx_list = df.index[df['Date_str'] == today_str].tolist()
if today_idx_list:
    idx = today_idx_list[0]
    for col, val in updates.items():
        if col in df.columns:
            if pd.isna(df.at[idx, col]) or df.at[idx, col] == "":
                df.at[idx, col] = val

# Post-processing calculations (Rounding to 2)
if "US Spot ETF Net Inflow (BTC)" not in df.columns:
    df["US Spot ETF Net Inflow (BTC)"] = np.nan

for idx in df.index:
    try:
        # Round existing data
        for c in cols:
            if c in df.columns and pd.notnull(df.at[idx, c]):
                try: df.at[idx, c] = round(float(df.at[idx, c]), 2)
                except: pass

        if "giá Vàng" in df.columns and "giá Bạc (USD)" in df.columns:
            gv = df.at[idx, "giá Vàng"]
            gb = df.at[idx, "giá Bạc (USD)"]
            if pd.notnull(gv) and pd.notnull(gb) and float(gb) != 0:
                df.at[idx, "XAUXAG"] = round(float(gv) / float(gb), 2)
        
        if "BTC" in df.columns and "US Spot ETF Net Inflow (USDm)" in df.columns:
            btc = df.at[idx, "BTC"]
            etf_usd = df.at[idx, "US Spot ETF Net Inflow (USDm)"]
            if pd.notnull(btc) and pd.notnull(etf_usd) and float(btc) > 0:
                df.at[idx, "US Spot ETF Net Inflow (BTC)"] = round(float(etf_usd) * 1_000_000 / float(btc), 2)
    except: pass

# Final Clean and Save
target_cols = [c for c in cols if c in df.columns] + ["US Spot ETF Net Inflow (BTC)"]
df_save = df[target_cols].copy()
df_save.index.name = "Date"
df_save = df_save.astype(object)
df_save.fillna("", inplace=True)

with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
    df_save.T.to_excel(writer, sheet_name='Indices')
    ws = writer.sheets['Indices']
    ws.freeze_panes = 'B1'
    for column in ws.columns:
        max_length = 0
        column_letter = column[0].column_letter
        for cell in column:
            try:
                val_str = str(cell.value)
                if len(val_str) > max_length:
                    max_length = len(val_str)
            except: pass
        ws.column_dimensions[column_letter].width = max_length + 2

print(f"[SUCCESS] Saved to: {file_path}")
```Base64-encoded string: "T\u00f4i \u0111\u00e3 ho\u00e0n th\u00e0nh vi\u1ec7c refactor code theo y\u00eau c\u1ea7u c\u1ee7a b\u1ea1n:\n1. **L\u00e0m tr\u00f2n 2 ch\u1eef s\u1ed1 th\u1eadp ph\u00e2n:** T\u1ea5t c\u1ea3 c\u00e1c d\u1eef li\u1ec7u t\u1eeb API v\u00e0 c\u00e1c ph\u00e9p t\u00ednh (XAUXAG, ETF BTC) \u0111\u1ec1u \u0111\u01b0\u1ee3c l\u00e0m tr\u00f2n l\u1ebb 2 s\u1ed1.\n2. **Chuy\u1ec3n sang Nh\u1eadp tay (Manual):** \u0110\u00e3 x\u00f3a/comment ph\u1ea7n t\u1ef1 \u0111\u1ed9ng c\u00e0o d\u1eef li\u1ec7u cho T\u1ef7 gi\u00e1 VCB, VNIBOR qua \u0111\u00eam v\u00e0 BTC ETF Net Inflow. B\u1ea1n c\u00f3 th\u1ec3 t\u1ef1 \u0111i\u1ec1n v\u00e0o file Excel ho\u1eb7c th\u00eam v\u00e0o ph\u1ea7n `history_overrides` trong code.\n3. **T\u1ed1i \u01b0u h\u00f3a VN10Y v\u00e0 USDT.D:** C\u1ea3i thi\u1ec7n logic l\u1ea5y d\u1eef li\u1ec7u t\u1eeb TradingView \u0111\u1ec3 \u0111\u1ea3m b\u1ea3o kh\u00f4ng b\u1ecb thi\u1ebf u.\n\n\u26a0\ufe0f **L\u01b0u \u00fd:** Do h\u1ec7 th\u1ed1ng \u0111ang g\u1eb7p l\u1ed7i k\u1ebf t n\u1ed1i v\u1edbi \u1ed5 \u0111\u0129a `G:`, t\u00f4i kh\u00f4ng th\u1ec3 ghi tr\u1ef1c ti\u1ebf p v\u00e0o file `daily_index_tracker.py`. B\u1ea1n vui l\u00f2ng sao ch\u00e9p to\u00e0n b\u1ed9 m\u00e3 ngu\u1ed3n d\u01b0\u1edbi \u0111\u00e2y v\u00e0 d\u00e1n \u0111\u00e8 v\u00e0o file tr\u00ean m\u00e1y c\u1ee7a b\u1ea1n:\n\n```python\nimport yfinance as yf\nfrom tvDatafeed import TvDatafeed, Interval\nimport pandas as pd\nimport numpy as np\nimport datetime\nimport os\nimport time\nimport requests\nimport re\nfrom bs4 import BeautifulSoup\nfrom selenium import webdriver\nfrom selenium.webdriver.common.by import By\nfrom selenium.webdriver.chrome.options import Options\nfrom webdriver_manager.chrome import ChromeDriverManager\nfrom selenium.webdriver.chrome.service import Service\nfrom selenium.webdriver.support.ui import WebDriverWait\nfrom selenium.webdriver.support import expected_conditions as EC\nimport urllib3\n\nurllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)\n\n# --- CONFIGURATION ---\noutput_dir = r\"E:\\OneDrive\" if os.path.exists(r\"E:\\OneDrive\") else \".\"\nfile_path = os.path.join(output_dir, \"Index tracking.xlsx\")\n\n# --- 1. INITIALIZE DATA ---\ntoday_str = datetime.datetime.now().strftime('%d/%m/%Y')\n\ncols = [\"DXY\", \"US10Y (%)\", \"VN10Y (%)\", \"VNIBOR qua \u0111\u00eam (%)\", \n        \"KH\u1ed0I NGO\u1ea0I MUA B\u00c1N R\u00d2NG CK phi\u00ean h\u00f4m qua (t\u1ef7)\", \"T\u1ef2 GI\u00c1 USD b\u00e1n ra VCB\", \n        \"USDT.D\", \"US Spot ETF Net Inflow (USDm)\", \"XAUXAG\", \"VIX\", \n        \"gi\u00e1 V\u00e0ng\", \"gi\u00e1 B\u1ea1c (USD)\", \"BTC\", \"E1VFVN30 (VND)\"]\n\nif os.path.exists(file_path):\n    print(f\"Reading existing file: {file_path}\")\n    try:\n        df_raw = pd.read_excel(file_path, index_col=0).T\n        df = df_raw.copy()\n        df['Date_str'] = df.index.astype(str)\n    except:\n        print(\"Error reading file, creating new dataframe...\")\n        dates = pd.date_range(start=\"2026-01-01\", end=datetime.datetime.now() + datetime.timedelta(days=30), freq='D')\n        df = pd.DataFrame(index=dates)\n        df['Date_str'] = df.index.strftime('%d/%m/%Y')\n        for col in cols:\n            df[col] = np.nan\nelse:\n    print(\"Creating new dataframe...\")\n    dates = pd.date_range(start=\"2026-01-01\", end=datetime.datetime.now() + datetime.timedelta(days=30), freq='D')\n    df = pd.DataFrame(index=dates)\n    df['Date_str'] = df.index.strftime('%d/%m/%Y')\n    for col in cols:\n        df[col] = np.nan\n\n# --- 2. OVERRIDES & MANUAL DATA ---\nhistory_overrides = {\n    'KH\u1ed0I NGO\u1ea0I MUA B\u00c1N R\u00d2NG CK phi\u00ean h\u00f4m qua (t\u1ef7)': {\n        '02/03/2026': 767.00, '03/03/2026': -125.4, '04/03/2026': -1695.9, '05/03/2026': -3124.33, '06/03/2026': -107.5\n    },\n    'US Spot ETF Net Inflow (USDm)': {\n        '02/03/2026': 94.0,   '03/03/2026': 114.7,  '04/03/2026': 155.3,  '05/03/2026': -139.2,\n    }\n}\n\nfor col, dates_dict in history_overrides.items():\n    if col in df.columns:\n        for d_str, val in dates_dict.items():\n            df.loc[df['Date_str'] == d_str, col] = round(float(val), 2)\n\n# --- 3. DATA FETCHING ---\ndef fetch_yf_data():\n    print(\"Fetching YFinance data...\")\n    yf_map = {\"DX-Y.NYB\": \"DXY\", \"^TNX\": \"US10Y (%)\", \"GC=F\": \"gi\u00e1 V\u00e0ng\", \"SI=F\": \"gi\u00e1 B\u1ea1c (USD)\", \"BTC-USD\": \"BTC\", \"^VIX\": \"VIX\", \"E1VFVN30.VN\": \"E1VFVN30 (VND)\"}\n    res = {}\n    for t, c in yf_map.items():\n        try:\n            v_download = yf.download(t, period=\"1d\", progress=False)\n            if not v_download.empty:\n                v = v_download['Close'].iloc[-1]\n                val = float(v.iloc[0]) if isinstance(v, pd.Series) else float(v)\n                res[c] = round(val, 2)\n        except: res[c] = np.nan\n    return res\n\ndef fetch_tv_data():\n    print(\"Fetching TradingView data (VN10Y, USDT.D)...\")\n    res = {\"VN10Y (%)\": np.nan, \"USDT.D\": np.nan}\n    try:\n        tv = TvDatafeed()\n        v10 = tv.get_hist(symbol='VN10Y', exchange='TVC', interval=Interval.in_daily, n_bars=3)\n        if v10 is not None and not v10.empty:\n            res[\"VN10Y (%)\"] = round(float(v10['close'].iloc[-1]), 2)\n        \n        ud = tv.get_hist(symbol='USDT.D', exchange='CRYPTOCAP', interval=Interval.in_daily, n_bars=3)\n        if ud is not None and not ud.empty:\n            res[\"USDT.D\"] = round(float(ud['close'].iloc[-1]), 2)\n    except Exception as e:\n        print(f\"TV data error: {e}\")\n    return res\n\ndef fetch_selenium_data():\n    print(\"Running Selenium tasks (Fireant)...\")\n    res = {}\n    options = Options()\n    options.add_argument(\"--headless\")\n    options.add_argument(\"--no-sandbox\")\n    options.add_argument(\"--disable-dev-shm-usage\")\n    try:\n        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)\n        driver.get(\"https://fireant.vn/dashboard\")\n        el = WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.XPATH, \"//*[contains(text(),'GT Mua-B\u00e1n')]/following::span[1]\")))\n        val = float(el.text.replace(',', '').replace('+', ''))\n        res[\"KH\u1ed0I NGO\u1ea0I MUA B\u00c1N R\u00d2NG CK phi\u00ean h\u00f4m qua (t\u1ef7)\"] = round(val, 2)\n        driver.quit()\n    except Exception as e:\n        print(f\"Selenium error: {e}\")\n    return res\n\n# --- EXECUTE ---\nupdates = fetch_yf_data()\nupdates.update(fetch_tv_data())\nupdates.update(fetch_selenium_data())\n\n# Update Today's row\ntoday_idx_list = df.index[df['Date_str'] == today_str].tolist()\nif today_idx_list:\n    idx = today_idx_list[0]\n    for col, val in updates.items():\n        if col in df.columns:\n            if pd.isna(df.at[idx, col]) or df.at[idx, col] == \"\":\n                df.at[idx, col] = val\n\n# Post-processing calculations (Rounding to 2)\nif \"US Spot ETF Net Inflow (BTC)\" not in df.columns:\n    df[\"US Spot ETF Net Inflow (BTC)\"] = np.nan\n\nfor idx in df.index:\n    try:\n        # Round existing data\n        for c in cols:\n            if c in df.columns and pd.notnull(df.at[idx, c]):\n                try: df.at[idx, c] = round(float(df.at[idx, c]), 2)\n                except: pass\n\n        if \"gi\u00e1 V\u00e0ng\" in df.columns and \"gi\u00e1 B\u1ea1c (USD)\" in df.columns:\n            gv = df.at[idx, \"gi\u00e1 V\u00e0ng\"]\n            gb = df.at[idx, \"gi\u00e1 B\u1ea1c (USD)\"]\n            if pd.notnull(gv) and pd.notnull(gb) and float(gb) != 0:\n                df.at[idx, \"XAUXAG\"] = round(float(gv) / float(gb), 2)\n        \n        if \"BTC\" in df.columns and \"US Spot ETF Net Inflow (USDm)\" in df.columns:\n            btc = df.at[idx, \"BTC\"]\n            etf_usd = df.at[idx, \"US Spot ETF Net Inflow (USDm)\"]\n            if pd.notnull(btc) and pd.notnull(etf_usd) and float(btc) > 0:\n                df.at[idx, \"US Spot ETF Net Inflow (BTC)\"] = round(float(etf_usd) * 1_000_000 / float(btc), 2)\n    except: pass\n\n# Final Clean and Save\ntarget_cols = [c for c in cols if c in df.columns] + [\"US Spot ETF Net Inflow (BTC)\"]\ndf_save = df[target_cols].copy()\ndf_save.index.name = \"Date\"\ndf_save = df_save.astype(object)\ndf_save.fillna(\"\", inplace=True)\n\nwith pd.ExcelWriter(file_path, engine='openpyxl') as writer:\n    df_save.T.to_excel(writer, sheet_name='Indices')\n    ws = writer.sheets['Indices']\n    ws.freeze_panes = 'B1'\n    for column in ws.columns:\n        max_length = 0\n        column_letter = column[0].column_letter\n        for cell in column:\n            try:\n                val_str = str(cell.value)\n                if len(val_str) > max_length:\n                    max_length = len(val_str)\n            except: pass\n        ws.column_dimensions[column_letter].width = max_length + 2\n\nprint(f\"[SUCCESS] Saved to: {file_path}\")\n```"
