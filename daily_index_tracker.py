import yfinance as yf
from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import numpy as np
import datetime
import os
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

# --- CẤU HÌNH ĐƯỜNG DẪN ---
output_dir = r"E:\OneDrive" if os.path.exists(r"E:\OneDrive") else "."
file_path = os.path.join(output_dir, "Index tracking.xlsx")

# --- 1. KHỞI TẠO HOẶC ĐỌC DỮ LIỆU CŨ ---
today_str = datetime.datetime.now().strftime('%d/%m/%y')
cols = ["DXY", "US10Y (%)", "VN10Y (%)", "VNIBOR qua đêm (%)", 
        "KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)", "TỶ GIÁ USD bán ra VCB", 
        "USDT.D", "US Spot ETF Net Inflow (USDm)", "XAUXAG", "VIX", 
        "giá Vàng", "giá Bạc (USD)", "BTC", "E1VFVN30 (VND)"]

if os.path.exists(file_path):
    # Đọc file cũ, chuyển Date thành index để dễ update
    df_old = pd.read_excel(file_path, index_col=0).T
    if today_str not in df_old.index:
        new_row = pd.DataFrame(index=[today_str], columns=cols)
        df_today = pd.concat([df_old, new_row])
    else:
        df_today = df_old
else:
    # Nếu chưa có file, tạo mới từ 01/01/26
    dates = pd.date_range(start="2026-01-01", end=datetime.datetime.now(), freq='D')
    df_today = pd.DataFrame(index=dates.strftime('%d/%m/%y'), columns=cols)

# --- 2. HÀM LẤY DATA CHO NGÀY HIỆN TẠI ---
def fetch_today_data():
    data = {}
    print(f"Kéo dữ liệu cho ngày: {today_str}")
    
    # YFinance (Lấy giá trị cuối cùng)
    try:
        yf_map = {"DX-Y.NYB": "DXY", "^TNX": "US10Y (%)", "^VIX": "VIX", 
                  "GC=F": "giá Vàng", "SI=F": "giá Bạc (USD)", "BTC-USD": "BTC", "E1VFVN30.VN": "E1VFVN30 (VND)"}
        for ticker, col in yf_map.items():
            temp = yf.download(ticker, period="1d", progress=False)
            if not temp.empty:
                val = temp['Close'].iloc[-1]
                data[col] = float(val.iloc[0]) if isinstance(val, pd.Series) else float(val)
    except: pass

    # TradingView (VN10Y & USDT.D)
    try:
        tv = TvDatafeed()
        v10 = tv.get_hist(symbol='VN10Y', exchange='TVC', interval=Interval.in_daily, n_bars=1)
        if v10 is not None: data["VN10Y (%)"] = v10['close'].iloc[-1]
        ud = tv.get_hist(symbol='USDT.D', exchange='CRYPTOCAP', interval=Interval.in_daily, n_bars=1)
        if ud is not None: data["USDT.D"] = ud['close'].iloc[-1]
    except: pass

    # Scraping (VCB & SBV)
    try:
        r = requests.get("https://webgia.com/ty-gia/vietcombank/", timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(r.text, 'html.parser')
        usd_row = soup.find('td', string=re.compile('USD')).find_parent('tr')
        data["TỶ GIÁ USD bán ra VCB"] = float(usd_row.find_all('td')[-1].get_text().strip().replace('.', '').replace(',', '.'))
    except: pass

    try:
        res = requests.get("https://sbv.gov.vn/webcenter/portal/vi/menu/trangchu/tk/lshdlnh", verify=False, timeout=15)
        soup = BeautifulSoup(res.text, 'html.parser')
        row = soup.find('td', string=re.compile(r"Qua đêm", re.IGNORECASE))
        if row:
            rate = re.findall(r"\d+[\.,]\d+", row.find_parent('tr').get_text())[0]
            data["VNIBOR qua đêm (%)"] = float(rate.replace(',', '.'))
    except: pass

    return data

# --- 3. CHẠY SELENIUM CHO CÁC NGUỒN PHỨC TẠP ---
def fetch_selenium_data():
    sel_data = {}
    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    try:
        # Fireant
        driver.get("https://fireant.vn/dashboard")
        el = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(),'GT Mua-Bán')]/following::span[1]")))
        sel_data["KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)"] = float(el.text.replace(',', '').replace('+', ''))
    except: pass

    try:
        # Farside
        driver.get("https://farside.co.uk/btc/")
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        headers = [h.text for h in driver.find_elements(By.XPATH, "//table//tr[1]//*")]
        t_idx = next((i for i, h in enumerate(headers) if "Total" in h), -1)
        if t_idx != -1:
            rows = driver.find_elements(By.XPATH, "//table//tr")
            # Tìm dòng của ngày hôm qua vì ETF thường update trễ 1 ngày
            yest_farside = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%d %b %Y')
            for r in rows:
                if yest_farside in r.text:
                    v = r.find_elements(By.TAG_NAME, "td")[t_idx].text.strip().replace('$', '').replace(',', '').replace('(', '-').replace(')', '')
                    sel_data["US Spot ETF Net Inflow (USDm)"] = float(v)
                    break
    finally:
        driver.quit()
    return sel_data

# --- 4. CẬP NHẬT VÀ LƯU FILE ---
today_updates = fetch_today_data()
today_updates.update(fetch_selenium_data())

for col, val in today_updates.items():
    df_today.at[today_str, col] = val

# Tính toán các cột phái sinh
if pd.notnull(df_today.at[today_str, "giá Vàng"]) and pd.notnull(df_today.at[today_str, "giá Bạc (USD)"]):
    df_today.at[today_str, "XAUXAG"] = df_today.at[today_str, "giá Vàng"] / df_today.at[today_str, "giá Bạc (USD)"]

# Lưu file với định dạng Freeze cột A
with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
    df_today.T.to_excel(writer, sheet_name='Indices')
    ws = writer.sheets['Indices']
    ws.freeze_panes = 'B1'

print(f"Cập nhật thành công ngày {today_str} vào file {file_path}")
