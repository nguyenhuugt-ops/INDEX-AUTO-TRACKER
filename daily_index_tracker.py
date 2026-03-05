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

urllib3.disable_warnings()

# --- CẤU HÌNH THỜI GIAN ---
start_date = "2026-01-01"
end_date = datetime.datetime.now()
dates = pd.date_range(start=start_date, end=end_date, freq='D')
df = pd.DataFrame(index=dates)
df.index.name = "Date"
df.reset_index(inplace=True)
df['Date_str'] = df['Date'].dt.strftime('%d/%m/%Y')

# --- KHỞI TẠO CỘT ---
cols = ["DXY", "US10Y (%)", "VIX", "giá Vàng", "giá Bạc (USD)", "BTC", "E1VFVN30 (VND)", "VN10Y (%)", 
        "VNIBOR qua đêm (%)", "KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)", "TỶ GIÁ USD bán ra VCB", 
        "USDT.D", "US Spot ETF Net Inflow (USDm)", "XAUXAG"]
for c in cols:
    df[c] = np.nan

# --- HÀM HỖ TRỢ ---
def get_yf_data(ticker, column_name):
    try:
        data = yf.download(ticker, start=start_date, end=end_date + datetime.timedelta(days=1), progress=False)
        if not data.empty:
            close = data['Close']
            if isinstance(close, pd.DataFrame):
                close = close.iloc[:, 0]
            df.set_index('Date', inplace=True)
            idx_intersection = df.index.intersection(close.index)
            df.loc[idx_intersection, column_name] = close.loc[idx_intersection].values
            df.reset_index(inplace=True)
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")

# --- 1. LẤY DỮ LIỆU CƠ BẢN (YFINANCE) ---
print("Fetching YFinance data...")
get_yf_data("DX-Y.NYB", "DXY")
get_yf_data("^TNX", "US10Y (%)")
get_yf_data("^VIX", "VIX")
get_yf_data("GC=F", "giá Vàng") 
get_yf_data("SI=F", "giá Bạc (USD)")
get_yf_data("BTC-USD", "BTC")
get_yf_data("E1VFVN30.VN", "E1VFVN30 (VND)")

# --- 2. TRADINGVIEW (VN10Y & USDT.D) ---
print("Fetching TradingView data...")
try:
    tv = TvDatafeed()
    for sym, col in [('VN10Y', 'VN10Y (%)'), ('USDT.D', 'USDT.D')]:
        tv_data = tv.get_hist(symbol=sym, exchange='TVC' if 'VN' in sym else 'CRYPTOCAP', interval=Interval.in_daily, n_bars=100)
        if tv_data is not None:
            tv_data.index = tv_data.index.normalize()
            df.set_index('Date', inplace=True)
            idx_int = df.index.intersection(tv_data.index)
            df.loc[idx_int, col] = tv_data.loc[idx_int, 'close']
            df.reset_index(inplace=True)
except Exception as e:
    print(f"TradingView error: {e}")

# --- 3. SCRAPING DỮ LIỆU PHỨC TẠP ---
today_str = datetime.datetime.now().strftime('%d/%m/%Y')
yesterday_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%d/%m/%Y')
today_idx = df.index[df['Date_str'] == today_str].tolist()[0] if today_str in df['Date_str'].values else None

if today_idx is not None:
    # VCB Rate
    try:
        r = requests.get("https://webgia.com/ty-gia/vietcombank/", timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(r.text, 'html.parser')
        usd_row = soup.find('td', string=re.compile('USD')).find_parent('tr')
        sell_val = usd_row.find_all('td')[-1].get_text().strip().replace('.', '').replace(',', '.')
        df.at[today_idx, "TỶ GIÁ USD bán ra VCB"] = float(sell_val)
    except: pass

    # SBV VNIBOR (Cải tiến logic tìm cột)
    try:
        url = "https://sbv.gov.vn/webcenter/portal/vi/menu/trangchu/tk/lshdlnh"
        soup = BeautifulSoup(requests.get(url, verify=False, timeout=15).text, 'html.parser')
        row = soup.find('td', string=re.compile(r"Qua đêm", re.IGNORECASE)).find_parent('tr')
        # Lấy giá trị số đầu tiên xuất hiện trong hàng "Qua đêm"
        rate = re.findall(r"\d+,\d+", row.get_text())[0].replace(',', '.')
        df.at[today_idx, "VNIBOR qua đêm (%)"] = float(rate)
    except: pass

# --- 4. SELENIUM (FIREANT & FARSIDE) ---
chrome_options = Options()
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

try:
    # Fireant (Khối ngoại)
    driver.get("https://fireant.vn/dashboard")
    val = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(),'GT Mua-Bán')]/following::span[1]"))).text
    df.loc[df['Date_str'] == yesterday_str, "KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)"] = float(val.replace(',', ''))

    # Farside (ETF - Tìm cột Total thông minh)
    driver.get("https://farside.co.uk/btc/")
    time.sleep(3)
    headers = [h.text for h in driver.find_elements(By.XPATH, "//table//tr[1]/th|//table//tr[1]/td")]
    total_idx = next(i for i, h in enumerate(headers) if "Total" in h)
    rows = driver.find_elements(By.XPATH, "//table//tr")
    for row in rows:
        cells = row.find_all('td')
        if cells and yesterday_str in cells[0].text:
            val = cells[total_idx].text.strip().replace('$', '').replace(',', '').replace('(', '-').replace(')', '')
            df.loc[df['Date_str'] == yesterday_str, "US Spot ETF Net Inflow (USDm)"] = float(val)
finally:
    driver.quit()

# --- 5. LÀM SẠCH & XUẤT FILE ---
df["XAUXAG"] = df["giá Vàng"] / df["giá Bạc (USD)"]
# Thêm logic: Chỉ điền giá trị thủ công (Hotfix) nếu ô dữ liệu vẫn đang trống (NaN)
hotfix = {'05/03/2026': {'BTC': 68168.45, 'USDT.D': 7.9698}}
for d, values in hotfix.items():
    for col, val in values.items():
        if pd.isna(df.loc[df['Date_str'] == d, col]).any():
            df.loc[df['Date_str'] == d, col] = val

df.fillna("", inplace=True)
output_path = os.path.join(r"E:\OneDrive" if os.path.exists(r"E:\OneDrive") else ".", "Index tracking.xlsx")
df.set_index("Date_str").T.to_excel(output_path)
print(f"Done! Saved to {output_path}")
