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

start_date = "2026-01-01"
end_date = datetime.datetime.now()

dates = pd.date_range(start=start_date, end=end_date, freq='D')
df = pd.DataFrame(index=dates)
df.index.name = "Date"
df.reset_index(inplace=True)
df['Date_str'] = df['Date'].dt.strftime('%d/%m/%Y')

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
        # map by date index
        idx_intersection = df.index.intersection(close.index)
        df.loc[idx_intersection, column_name] = close.loc[idx_intersection]
        df.reset_index(inplace=True)
    except Exception as e:
        df[column_name] = np.nan
        print(f"Error fetching {ticker}: {e}")

df["DXY"] = np.nan
df["US10Y (%)"] = np.nan
df["VIX"] = np.nan
df["giá Vàng"] = np.nan
df["giá Bạc (USD)"] = np.nan
df["BTC"] = np.nan
df["E1VFVN30 (VND)"] = np.nan
df["VN10Y (%)"] = np.nan

get_yf_data("DX-Y.NYB", "DXY")
get_yf_data("^TNX", "US10Y (%)")
get_yf_data("^VIX", "VIX")
get_yf_data("GC=F", "giá Vàng") 
get_yf_data("SI=F", "giá Bạc (USD)")

if "giá Vàng" in df.columns and "giá Bạc (USD)" in df.columns:
    # ensure numeric before dividing
    gold = pd.to_numeric(df["giá Vàng"], errors='coerce')
    silver = pd.to_numeric(df["giá Bạc (USD)"], errors='coerce')
    df["XAUXAG"] = gold / silver
else:
    df["XAUXAG"] = np.nan

get_yf_data("BTC-USD", "BTC")
get_yf_data("E1VFVN30.VN", "E1VFVN30 (VND)")

# fetch VN10Y AND USDT.D from tvDatafeed
for attempt in range(3):
    try:
        tv = TvDatafeed()
        tv_data = tv.get_hist(symbol='VN10Y', exchange='TVC', interval=Interval.in_daily, n_bars=100)
        if tv_data is not None and not tv_data.empty:
            tv_data.index = tv_data.index.normalize()
            df.set_index('Date', inplace=True)
            idx_intersection = df.index.intersection(tv_data.index)
            df.loc[idx_intersection, "VN10Y (%)"] = tv_data.loc[idx_intersection, 'close']
            df.reset_index(inplace=True)
            
        tv_usdt = tv.get_hist(symbol='USDT.D', exchange='CRYPTOCAP', interval=Interval.in_daily, n_bars=100)
        if tv_usdt is not None and not tv_usdt.empty:
            tv_usdt.index = tv_usdt.index.normalize()
            df.set_index('Date', inplace=True)
            idx_intersection2 = df.index.intersection(tv_usdt.index)
            df.loc[idx_intersection2, "USDT.D"] = tv_usdt.loc[idx_intersection2, 'close']
            df.reset_index(inplace=True)
        break # Success
    except Exception as e:
        print(f"tvDatafeed error (attempt {attempt}): {e}")
        time.sleep(3)

# Fallback for VN10Y if TradingView API fails (IP Block from Nologin)
vn10y_past = {
    '20/02/2026': 4.249, '23/02/2026': 4.246, '24/02/2026': 4.251,
    '25/02/2026': 4.251, '26/02/2026': 4.251
}
for d_str, val in vn10y_past.items():
    if pd.isna(df.loc[df['Date_str'] == d_str, "VN10Y (%)"].values[0]) or df.loc[df['Date_str'] == d_str, "VN10Y (%)"].values[0] == "":
        df.loc[df['Date_str'] == d_str, "VN10Y (%)"] = val

df["USDT.D"] = df.get("USDT.D", np.nan)
df["VNIBOR qua đêm (%)"] = np.nan
df["KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)"] = np.nan
df["TỶ GIÁ USD bán ra VCB"] = np.nan
df["US Spot ETF Net Inflow"] = np.nan

# Hardcode Khối ngoại 10 days
khoi_ngoai = {
    '10/02/2026': -255.43,
    '11/02/2026': 761.05,
    '12/02/2026': 2086.43,
    '13/02/2026': 314.59,
    '14/02/2026': 196.17,
    '23/02/2026': -1107.49,
    '24/02/2026': 319.22,
    '25/02/2026': -1061.00,
    '26/02/2026': -1228.14
}

for d_str, val in khoi_ngoai.items():
    df.loc[df['Date_str'] == d_str, "KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)"] = val


today_str = datetime.datetime.now().strftime('%d/%m/%Y')
today_idx = df.index[df['Date_str'] == today_str].tolist()
if today_idx:
    today_idx = today_idx[0]
    
    # 2. VCB XML (Chỉ hôm nay) - sẽ ffill cho quá khứ nếu trống
    try:
        url = "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx"
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.content, 'xml')
        for exrate in soup.find_all('Exrate'):
            if exrate.get('CurrencyCode') == 'USD':
                df.at[today_idx, "TỶ GIÁ USD bán ra VCB"] = float(exrate.get('Sell').replace(',', ''))
    except Exception as e:
        print(f"VCB error: {e}")

    # 3. SBV VNIBOR
    try:
        url = "https://sbv.gov.vn/webcenter/portal/vi/menu/trangchu/tk/lshdlnh"
        response = requests.get(url, verify=False, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Tìm ngày áp dụng
        date_text_nodes = soup.find_all(string=re.compile("Ngày áp dụng"))
        sbv_date = None
        for node in date_text_nodes:
            match = re.search(r"(\d{2}/\d{2}/\d{4})", node)
            if match:
                sbv_date = match.group(1)
                break
                
        # Tìm lãi suất
        tds = soup.find_all('td')
        rate = None
        for i, td in enumerate(tds):
            if "Qua đêm" in td.get_text():
                if i + 1 < len(tds):
                    val_str = tds[i+1].get_text().strip().replace(',', '.')
                    rate = float(val_str)
                break
                
        if sbv_date and rate is not None:
            df.loc[df['Date_str'] == sbv_date, "VNIBOR qua đêm (%)"] = rate
        elif rate is not None:
            df.at[today_idx, "VNIBOR qua đêm (%)"] = rate
            
        # Hardcode past VNIBOR
        vnibor_past = {
            '09/02/2026': 9.19, '10/02/2026': 8.51, '11/02/2026': 4.28,
            '12/02/2026': 4.28, '13/02/2026': 4.28, '23/02/2026': 6.39,
            '24/02/2026': 4.47, '25/02/2026': 4.47, '26/02/2026': 4.47
        }
        for d_str, val in vnibor_past.items():
            df.loc[df['Date_str'] == d_str, "VNIBOR qua đêm (%)"] = val

    except Exception as e:
        print(f"SBV error: {e}")

    # 4. TỶ GIÁ LỊCH SỬ (yfinance VND=X để backfill VCB)
    try:
         vnd_data = yf.download("VND=X", start=start_date, end=end_date + datetime.timedelta(days=1), progress=False)
         if not vnd_data.empty:
             vnd_close = vnd_data['Close']
             if isinstance(vnd_data.columns, pd.MultiIndex):
                 vnd_close = vnd_data['Close']['VND=X']
             df.set_index('Date', inplace=True)
             idx_intersection = df.index.intersection(vnd_close.index)
             for idx in idx_intersection:
                 if pd.isna(df.loc[idx, "TỶ GIÁ USD bán ra VCB"]):
                     df.loc[idx, "TỶ GIÁ USD bán ra VCB"] = vnd_close.loc[idx]
             df.reset_index(inplace=True)
    except Exception as e:
         print(f"VND=X backfill error: {e}")


    # SELENIUM for Fireant and Farside ETF
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--log-level=3")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # KHỐI NGOẠI FIREANT
        try:
            driver.get("https://fireant.vn/dashboard")
            time.sleep(5)
            rows = driver.find_elements(By.XPATH, "//div[contains(text(), 'GT Mua-Bán')]/following-sibling::div")
            if rows:
                val_text = rows[0].text.replace('tỷ', '').replace(',', '').strip()
                if val_text:
                    df.at[today_idx, "KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)"] = float(val_text)
        except Exception as e:
            print(f"Fireant error: {e}")
            
        # 4. US SPOT ETF NET INFLOW (Farside)
        try:
            driver.get("https://farside.co.uk/btc/")
            time.sleep(3)
            totals = driver.find_elements(By.XPATH, "//th[text()='Total']/ancestor::table//tr[last()]/td")
            if totals:
                val = totals[-1].text.strip()
                if val:
                    df.at[today_idx, "US Spot ETF Net Inflow"] = float(val.replace('$', '').replace(',', ''))
        except Exception as e:
            print(f"Farside error: {e}")
            
        driver.quit()
    except Exception as e:
        print(f"Selenium setup error: {e}")

    # Hardcode Farside past 10 days
    farside_past = {
        '09/02/2026': 598.0, '10/02/2026': 4469.0, '11/02/2026': -3792.0,
        '12/02/2026': -4970.0, '13/02/2026': -1610.0, '17/02/2026': 69.65,
        '18/02/2026': -1853.0, '19/02/2026': -1636.0, '20/02/2026': -2455.0,
        '21/02/2026': 1294.0, '24/02/2026': -3092.0, '25/02/2026': 3556.0
    }
    for d_str, val in farside_past.items():
        df.loc[df['Date_str'] == d_str, "US Spot ETF Net Inflow"] = val

# Replace explicit NAs/nans with empties
df.replace({np.nan: ""}, inplace=True)

df['Date'] = df['Date_str']

# Thay thế các ô trống bằng "" 
df = df.astype(object)
df.fillna("", inplace=True)
df.replace([np.nan, "NaN", "nan"], "", inplace=True)

cols_order = ["Date", "DXY", "US10Y (%)", "VN10Y (%)", "VNIBOR qua đêm (%)", 
              "KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)", "TỶ GIÁ USD bán ra VCB", 
              "USDT.D", "US Spot ETF Net Inflow", "XAUXAG", "VIX", 
              "giá Vàng", "giá Bạc (USD)", "BTC", "E1VFVN30 (VND)"]

cols_order = [c for c in cols_order if c in df.columns]
df = df[cols_order]
df_t = df.set_index("Date").T

output_dir = r"E:\OneDrive"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
output_path = os.path.join(output_dir, "Index tracking.xlsx")

try:
    df_t.to_excel(output_path)
    print(f"Saved successfully to {output_path}")
except Exception as e:
    print(f"Failed to save Excel: {e}")
