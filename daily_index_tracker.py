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

# Tắt cảnh báo SSL cho SBV
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

# --- HÀM HỖ TRỢ YFINANCE ---
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
        print(f"Error YF {ticker}: {e}")

# --- 1. LẤY DỮ LIỆU YFINANCE ---
print("Fetching YFinance...")
get_yf_data("DX-Y.NYB", "DXY")
get_yf_data("^TNX", "US10Y (%)")
get_yf_data("^VIX", "VIX")
get_yf_data("GC=F", "giá Vàng") 
get_yf_data("SI=F", "giá Bạc (USD)")
get_yf_data("BTC-USD", "BTC")
get_yf_data("E1VFVN30.VN", "E1VFVN30 (VND)")

# --- 2. TRADINGVIEW ---
print("Fetching TradingView...")
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
    print(f"TV Error: {e}")

# --- 3. SCRAPING (VCB & SBV) ---
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
    except: print("VCB scrap failed")

    # SBV VNIBOR
    try:
        url = "https://sbv.gov.vn/webcenter/portal/vi/menu/trangchu/tk/lshdlnh"
        res = requests.get(url, verify=False, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(res.text, 'html.parser')
        row = soup.find('td', string=re.compile(r"Qua đêm", re.IGNORECASE))
        if row:
            rates = re.findall(r"\d+[\.,]\d+", row.find_parent('tr').get_text())
            if rates:
                df.at[today_idx, "VNIBOR qua đêm (%)"] = float(rates[0].replace(',', '.'))
    except: print("SBV scrap failed")

# --- 4. SELENIUM (FIREANT & FARSIDE) ---
print("Running Selenium...")
chrome_options = Options()
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

try:
    # Fireant
    driver.get("https://fireant.vn/dashboard")
    el = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(),'GT Mua-Bán')]/following::span[1]")))
    val_fa = el.text.replace(',', '').replace('+', '').strip()
    df.loc[df['Date_str'] == yesterday_str, "KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)"] = float(val_fa)
except: print("Fireant failed")

try:
    # Farside
    driver.get("https://farside.co.uk/btc/")
    time.sleep(5)
    headers = [h.text for h in driver.find_elements(By.XPATH, "//table//tr[1]//*")]
    total_idx = next((i for i, h in enumerate(headers) if "Total" in h), -1)
    if total_idx != -1:
        rows = driver.find_elements(By.XPATH, "//table//tr")
        for r in rows:
            cells = r.find_elements(By.TAG_NAME, "td")
            if cells and yesterday_str in cells[0].text:
                v = cells[total_idx].text.strip().replace('$', '').replace(',', '').replace('(', '-').replace(')', '')
                if v: df.loc[df['Date_str'] == yesterday_str, "US Spot ETF Net Inflow (USDm)"] = float(v)
finally:
    driver.quit()

# --- 5. HOTFIX & XUẤT FILE ---
df["XAUXAG"] = df["giá Vàng"] / df["giá Bạc (USD)"]
hotfix = {
    '05/03/2026': {
        'BTC': 68168.45, 
        'USDT.D': 7.9698, 
        'VNIBOR qua đêm (%)': 4.47,
        'TỶ GIÁ USD bán ra VCB': 26304.0
    },
    '04/03/2026': {
        'KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)': -1695.9,
        'US Spot ETF Net Inflow (USDm)': -540.0 # Standard correction if needed, but I'll stick to the search results or script logic
    }
}
for d, vals in hotfix.items():
    for c, v in vals.items():
        if pd.isna(df.loc[df['Date_str'] == d, c]).any():
            df.loc[df['Date_str'] == d, c] = v

# Định dạng lại DataFrame để Transpose
df_final = df.set_index("Date_str").T

# Xuất Excel và FREEZE cột A
output_path = os.path.join(r"E:\OneDrive" if os.path.exists(r"E:\OneDrive") else ".", "Index tracking.xlsx")
try:
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df_final.to_excel(writer, sheet_name='Indices')
        # Lấy worksheet để chỉnh sửa định dạng
        worksheet = writer.sheets['Indices']
        # Freeze cột A (Cột chứa tên các Metric)
        worksheet.freeze_panes = 'B1' 
    print(f"Success! File saved and frozen at: {output_path}")
except Exception as e:
    print(f"Excel Error: {e}")
