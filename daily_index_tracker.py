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

# --- HÀM HỖ TRỢ ---
def get_yf_data(ticker, column_name):
    try:
        data = yf.download(ticker, start=start_date, end=end_date + datetime.timedelta(days=1), progress=False)
        if isinstance(data.columns, pd.MultiIndex):
            if ticker in data['Close']:
                close = data['Close'][ticker]
            else:
                close = data['Close'].iloc[:, 0]
        else:
            close = data['Close']
        df.set_index('Date', inplace=True)
        idx_intersection = df.index.intersection(close.index)
        df.loc[idx_intersection, column_name] = close.loc[idx_intersection]
        df.reset_index(inplace=True)
    except Exception as e:
        df[column_name] = np.nan
        print(f"Error fetching {ticker}: {e}")

def get_binance_btc_price():
    """Lấy giá BTC thực từ Binance API (Backup cho yfinance)"""
    try:
        url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
        r = requests.get(url, timeout=5)
        data = r.json()
        return float(data['price'])
    except Exception as e:
        print(f"Binance API Error: {e}")
        return None

# --- KHỞI TẠO CỘT ---
cols = ["DXY", "US10Y (%)", "VIX", "giá Vàng", "giá Bạc (USD)", "BTC", "E1VFVN30 (VND)", "VN10Y (%)", 
        "VNIBOR qua đêm (%)", "KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)", "TỶ GIÁ USD bán ra VCB", 
        "USDT.D", "US Spot ETF Net Inflow (USDm)", "XAUXAG"]
for c in cols:
    df[c] = np.nan

# --- LẤY DỮ LIỆU CƠ BẢN TỪ YFINANCE ---
print("Fetching YFinance data...")
get_yf_data("DX-Y.NYB", "DXY")
get_yf_data("^TNX", "US10Y (%)")
get_yf_data("^VIX", "VIX")
get_yf_data("GC=F", "giá Vàng") 
get_yf_data("SI=F", "giá Bạc (USD)")
get_yf_data("BTC-USD", "BTC")
get_yf_data("E1VFVN30.VN", "E1VFVN30 (VND)")

# Tính tỷ lệ Vàng/Bạc
if "giá Vàng" in df.columns and "giá Bạc (USD)" in df.columns:
    gold = pd.to_numeric(df["giá Vàng"], errors='coerce')
    silver = pd.to_numeric(df["giá Bạc (USD)"], errors='coerce')
    df["XAUXAG"] = gold / silver

# --- TRADINGVIEW DATAFEED (VN10Y & USDT.D) ---
print("Fetching TradingView data...")
for attempt in range(3):
    try:
        tv = TvDatafeed()
        # VN10Y
        tv_data = tv.get_hist(symbol='VN10Y', exchange='TVC', interval=Interval.in_daily, n_bars=100)
        if tv_data is not None and not tv_data.empty:
            tv_data.index = tv_data.index.normalize()
            df.set_index('Date', inplace=True)
            idx_intersection = df.index.intersection(tv_data.index)
            df.loc[idx_intersection, "VN10Y (%)"] = tv_data.loc[idx_intersection, 'close']
            df.reset_index(inplace=True)
            
        # USDT.D
        tv_usdt = tv.get_hist(symbol='USDT.D', exchange='CRYPTOCAP', interval=Interval.in_daily, n_bars=100)
        if tv_usdt is not None and not tv_usdt.empty:
            tv_usdt.index = tv_usdt.index.normalize()
            df.set_index('Date', inplace=True)
            idx_intersection2 = df.index.intersection(tv_usdt.index)
            df.loc[idx_intersection2, "USDT.D"] = tv_usdt.loc[idx_intersection2, 'close']
            df.reset_index(inplace=True)
        break 
    except Exception as e:
        print(f"tvDatafeed error (attempt {attempt}): {e}")
        time.sleep(3)

# --- XÁC ĐỊNH INDEX NGÀY HÔM NAY VÀ HÔM QUA ---
today_str = datetime.datetime.now().strftime('%d/%m/%Y')
yesterday_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%d/%m/%Y')

today_idx_list = df.index[df['Date_str'] == today_str].tolist()
today_idx = today_idx_list[0] if today_idx_list else None

yesterday_idx_list = df.index[df['Date_str'] == yesterday_str].tolist()
yesterday_idx = yesterday_idx_list[0] if yesterday_idx_list else None

# --- CÁC NGUỒN DỮ LIỆU PHỨC TẠP (Web Scraping / API Backup) ---

if today_idx is not None:
    # 1. BTC REAL-TIME FIX (Binance Backup)
    # Nếu yfinance trả về NaN cho hôm nay (do chưa đóng nến), dùng Binance
    current_btc = df.at[today_idx, "BTC"]
    if pd.isna(current_btc) or current_btc == 0:
        binance_price = get_binance_btc_price()
        if binance_price:
            df.at[today_idx, "BTC"] = binance_price
            print(f"Fetched BTC from Binance: {binance_price}")

    # 2. TỶ GIÁ VCB (XML -> Webgia fallback)
    print("Fetching VCB Rate...")
    try:
        vcb_url = "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx"
        response = requests.get(vcb_url, timeout=10)
        soup = BeautifulSoup(response.content, 'xml')
        found = False
        for exrate in soup.find_all('Exrate'):
            if exrate.get('CurrencyCode') == 'USD':
                sell_str = exrate.get('Sell', '').replace(',', '')
                if sell_str:
                    df.at[today_idx, "TỶ GIÁ USD bán ra VCB"] = float(sell_str)
                    found = True
                break
        if not found:
            raise Exception("XML empty")
    except Exception:
        # Fallback Webgia
        try:
            wg_url = "https://webgia.com/ty-gia/vietcombank/"
            r = requests.get(wg_url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
            soup2 = BeautifulSoup(r.text, 'html.parser')
            rows = soup2.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if cells and 'USD' in cells[0].get_text():
                    sell_val = cells[-1].get_text().strip().replace('.', '').replace(',', '.')
                    df.at[today_idx, "TỶ GIÁ USD bán ra VCB"] = float(sell_val)
                    break
        except Exception as e2:
            print(f"VCB Fallback error: {e2}")

    # 3. VNIBOR QUA ĐÊM (SBV - Logic cải tiến)
    print("Fetching SBV VNIBOR...")
    try:
        url = "https://sbv.gov.vn/webcenter/portal/vi/menu/trangchu/tk/lshdlnh"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(url, verify=False, timeout=15, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Tìm ngày áp dụng trên web để map đúng ngày
        date_node = soup.find(string=re.compile("Ngày áp dụng"))
        sbv_date_str = None
        if date_node:
            match = re.search(r"(\d{2}/\d{2}/\d{4})", date_node)
            if match:
                sbv_date_str = match.group(1)

        # Tìm dòng chứa lãi suất "Qua đêm" chuẩn xác
        rate_val = None
        row = soup.find('td', string=re.compile(r"Qua đêm", re.IGNORECASE))
        if row:
            # Tìm ô giá trị (thường là ô kế tiếp trong cùng hàng tr)
            parent_tr = row.find_parent('tr')
            cols = parent_tr.find_all('td')
            # Lãi suất thường nằm ở cột thứ 2 hoặc 3
            for col in cols:
                text = col.get_text().strip().replace(',', '.')
                if re.match(r"^\d+(\.\d+)?$", text): # Là số
                    rate_val = float(text)
                    break
        
        if rate_val is not None:
            if sbv_date_str:
                # Gán vào đúng ngày web hiển thị
                match_idx = df.index[df['Date_str'] == sbv_date_str].tolist()
                if match_idx:
                    df.at[match_idx[0], "VNIBOR qua đêm (%)"] = rate_val
                    print(f"SBV Rate ({sbv_date_str}): {rate_val}")
            else:
                # Không tìm thấy ngày -> Gán tạm vào hôm nay
                df.at[today_idx, "VNIBOR qua đêm (%)"] = rate_val
                print(f"SBV Rate (Assumed Today): {rate_val}")

    except Exception as e:
        print(f"SBV Error: {e}")

    # 4. SELENIUM: KHỐI NGOẠI & FARSIDE
    print("Running Selenium tasks...")
    driver = None
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--log-level=3")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # A. KHỐI NGOẠI (Fireant)
        if yesterday_idx is not None:
            try:
                driver.get("https://fireant.vn/dashboard")
                time.sleep(5)
                # Logic lấy số liệu (giữ nguyên hoặc tinh chỉnh nếu cần)
                elements = driver.find_elements(By.XPATH, "//*[contains(text(),'GT Mua-Bán')]/following::*[1]")
                if elements:
                    val_text = elements[0].text.replace('tỷ', '').replace(',', '').replace('+', '').strip()
                    if val_text:
                        df.at[yesterday_idx, "KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)"] = float(val_text)
                        print(f"Fireant (Yesterday): {val_text}")
            except Exception as e:
                print(f"Fireant error: {e}")

        # B. US SPOT ETF (Farside)
        # Check cả hôm qua và hôm kia đề phòng lệch múi giờ
        check_days = []
        if yesterday_idx is not None: check_days.append(yesterday_idx)
        
        try:
            driver.get("https://farside.co.uk/btc/")
            time.sleep(3)
            # Tìm dòng TOTAL chuẩn xác
            totals = driver.find_elements(By.XPATH, "//tr[td[1][contains(translate(text(), 'TOTAL', 'total'), 'total')]]/td")
            if totals:
                val_text = totals[-1].text.strip()
                if val_text:
                    val_num = float(val_text.replace('(', '-').replace(')', '').replace('$', '').replace(',', ''))
                    # Mặc định gán vào ngày hôm qua (T-1)
                    if yesterday_idx is not None:
                        df.at[yesterday_idx, "US Spot ETF Net Inflow (USDm)"] = val_num
                        print(f"Farside ETF: {val_num}")
        except Exception as e:
            print(f"Farside error: {e}")
            
    except Exception as e:
        print(f"Selenium Error: {e}")
    finally:
        if driver:
            driver.quit()

# --- OVERRIDES: CÁC GIÁ TRỊ CỐ ĐỊNH & HOTFIX ---

# 1. Hotfix cho ngày hôm nay (04/03/2026) theo dữ liệu ngài yêu cầu
hotfix_today = {
    'Date_str': '04/03/2026',
    'VNIBOR qua đêm (%)': 4.47,     # Giả định giữ nguyên nếu SBV chưa update
    'USDT.D': 7.9698,               # Dữ liệu sáng
    'BTC': 68168.45,                # Giá sáng nay
    'TỶ GIÁ USD bán ra VCB': 26298  # Giá sáng nay
}

# Áp dụng Hotfix
idx_today_fix = df.index[df['Date_str'] == hotfix_today['Date_str']].tolist()
if idx_today_fix:
    idx = idx_today_fix[0]
    for col, val in hotfix_today.items():
        if col != 'Date_str':
            # Chỉ ghi đè nếu đang rỗng hoặc NaN
            current = df.at[idx, col]
            if pd.isna(current) or current == "":
                df.at[idx, col] = val

# 2. Các dữ liệu lịch sử (Hardcode cũ)
overrides = {
    '02/03/2026': {'VIX': 21.44, 'VNIBOR qua đêm (%)': 4.47, 'BTC': 65695.44, 'TỶ GIÁ USD bán ra VCB': 26289},
}
for d_str, vals in overrides.items():
    match = df.index[df['Date_str'] == d_str].tolist()
    if match:
        idx = match[0]
        for c, v in vals.items():
            df.at[idx, c] = v

# --- XỬ LÝ DỮ LIỆU CUỐI CÙNG ---

# Tính cột ETF Inflow (BTC)
df["US Spot ETF Net Inflow (BTC)"] = np.nan
btc_col = pd.to_numeric(df["BTC"], errors='coerce')
etf_usd_col = pd.to_numeric(df["US Spot ETF Net Inflow (USDm)"], errors='coerce')
valid = btc_col.notna() & etf_usd_col.notna() & (btc_col > 0)
df.loc[valid, "US Spot ETF Net Inflow (BTC)"] = (etf_usd_col[valid] * 1_000_000 / btc_col[valid]).round(2)

# Làm sạch data để xuất Excel
df['Date'] = df['Date_str']
df = df.astype(object)
df.fillna("", inplace=True)
df.replace([np.nan, "NaN", "nan"], "", inplace=True)

# Sắp xếp cột
cols_order = ["Date", "DXY", "US10Y (%)", "VN10Y (%)", "VNIBOR qua đêm (%)",
              "KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)", "TỶ GIÁ USD bán ra VCB",
              "USDT.D", "US Spot ETF Net Inflow (BTC)", "XAUXAG", "VIX",
              "giá Vàng", "giá Bạc (USD)", "BTC", "E1VFVN30 (VND)"]
cols_order = [c for c in cols_order if c in df.columns]
df = df[cols_order]

# Transpose để xem theo Hàng ngang (Ngày là cột)
df_t = df.set_index("Date").T

# --- XUẤT FILE ---
print("\n--- DATA PREVIEW (LAST 2 DAYS) ---")
print(df.tail(2).to_string(index=False))

output_dir = r"E:\OneDrive"
if not os.path.exists(output_dir):
    try:
        os.makedirs(output_dir)
    except:
        output_dir = "." # Fallback to current folder
        
output_path = os.path.join(output_dir, "Index tracking.xlsx")

try:
    df_t.to_excel(output_path)
    print(f"\n[SUCCESS] Saved to: {output_path}")
except Exception as e:
    print(f"\n[ERROR] Could not save Excel: {e}")
    temp_path = "Index_tracking_temp.xlsx"
    df_t.to_excel(temp_path)
    print(f"[INFO] Saved to temp file instead: {temp_path}")
