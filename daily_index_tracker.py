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

# Hardcoded overrides for known yfinance data errors
# (yfinance ^VIX sometimes returns wrong data - correct with real values)
vix_overrides = {
    '02/03/2026': 19.86,
}
for d_str, val in vix_overrides.items():
    matches = df.loc[df['Date_str'] == d_str]
    if not matches.empty:
        df.loc[df['Date_str'] == d_str, 'VIX'] = val

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
    '25/02/2026': 4.251, '26/02/2026': 4.251,
    '27/02/2026': 4.256, '28/02/2026': 4.254,
    '02/03/2026': 4.253,
}
for d_str, val in vn10y_past.items():
    if pd.isna(df.loc[df['Date_str'] == d_str, "VN10Y (%)"].values[0]) or df.loc[df['Date_str'] == d_str, "VN10Y (%)"].values[0] == "":
        df.loc[df['Date_str'] == d_str, "VN10Y (%)"] = val

df["USDT.D"] = df.get("USDT.D", np.nan)
df["VNIBOR qua đêm (%)"] = np.nan
df["KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)"] = np.nan
df["TỶ GIÁ USD bán ra VCB"] = np.nan
df["US Spot ETF Net Inflow (USDm)"] = np.nan  # raw USD millions from Farside

# Note: Overrides are now applied at the end of the script to ensure accuracy.


today_str = datetime.datetime.now().strftime('%d/%m/%Y')
today_idx = df.index[df['Date_str'] == today_str].tolist()
if today_idx:
    today_idx = today_idx[0]
    
    # 2. VCB Exchange Rate - try XML first, fallback to webgia.com
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
                    print(f"VCB XML rate: {sell_str}")
                    found = True
                break
        if not found:
            raise Exception("USD not found in VCB XML")
    except Exception as e:
        print(f"VCB XML error: {e} - trying webgia.com fallback")
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
                    print(f"VCB webgia fallback: {sell_val}")
                    break
        except Exception as e2:
            print(f"VCB webgia error: {e2}")

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
            '24/02/2026': 4.47, '25/02/2026': 4.47, '26/02/2026': 4.47,
            '27/02/2026': 4.47, '28/02/2026': 4.47, '02/03/2026': 4.47,
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
        
        # KHỐI NGOẠI FIREANT - Lấy GT Mua-Bán (Giá Trị, not Khối Lượng)
        try:
            driver.get("https://fireant.vn/dashboard")
            time.sleep(6)
            # Click 'Nước ngoài' tab
            try:
                tab = driver.find_element(By.XPATH, "//div[contains(text(),'Nước ngoài')]")
                tab.click()
                time.sleep(2)
            except:
                pass
            # Read GT Mua-Bán value directly from dashboard text
            # The panel shows: GT Mua, GT Bán, GT Mua-Bán
            try:
                # Try to get GT Mua-Bán text - look for element after 'GT Mua-Bán' label
                elements = driver.find_elements(By.XPATH, "//div[contains(@class,'text') and contains(text(),'GT Mua-Bán')]/following-sibling::div[1]")
                if not elements:
                    elements = driver.find_elements(By.XPATH, "//*[contains(text(),'GT Mua-Bán')]/following::*[1]")
                if elements:
                    val_text = elements[0].text.replace('tỷ', '').replace(',', '').replace('+', '').strip()
                    if val_text:
                        df.at[today_idx, "KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)"] = float(val_text)
                        print(f"Fireant GT Mua-Bán: {val_text}")
            except Exception as e:
                print(f"Fireant GT parse error: {e}")
        except Exception as e:
            print(f"Fireant error: {e}")
            
        # 4. US SPOT ETF NET INFLOW (Farside) - store in USD millions, convert to BTC later
        try:
            driver.get("https://farside.co.uk/btc/")
            time.sleep(3)
            totals = driver.find_elements(By.XPATH, "//th[text()='Total']/ancestor::table//tr[last()]/td")
            if totals:
                val = totals[-1].text.strip()
                if val:
                    df.at[today_idx, "US Spot ETF Net Inflow (USDm)"] = float(val.replace('$', '').replace(',', ''))
        except Exception as e:
            print(f"Farside error: {e}")
            
        driver.quit()
    except Exception as e:
        print(f"Selenium setup error: {e}")

    # Note: Overrides applied at the end.


# =============================================================================
# FINAL OVERRIDES - "Chuẩn tuyệt đối" verified data
# =============================================================================

# 1. Foreign Investor Net Value (Khoi Ngoai) - verified from CafeF articles & Fireant tooltips
khoi_ngoai_final = {
    '10/02/2026': 761.05, '11/02/2026': 2086.43, '12/02/2026': 341.59,
    '13/02/2026': 196.00, '23/02/2026': -1107.00, '24/02/2026': 319.00,
    '25/02/2026': -1061.33, '26/02/2026': -3146.81, '27/02/2026': 182.00,
    '02/03/2026': 766.50,
}
for d_str, val in khoi_ngoai_final.items():
    df.loc[df['Date_str'] == d_str, "KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)"] = val

# 2. US Spot ETF Net Inflow (Farside) - verified from farside.co.uk (USD millions)
farside_final = {
    '11/02/2026': -276.3, '12/02/2026': -410.2, '13/02/2026': 15.1,
    '17/02/2026': -104.9, '18/02/2026': -133.3, '19/02/2026': -165.8,
    '20/02/2026': 88.1,   '23/02/2026': -203.8, '24/02/2026': 257.7,
    '25/02/2026': 506.6,  '26/02/2026': 254.4,  '27/02/2026': -27.5,
}
for d_str, val in farside_final.items():
    df.loc[df['Date_str'] == d_str, "US Spot ETF Net Inflow (USDm)"] = val

# 3. VCB Exchange Rate Fix
df.loc[df['Date_str'] == '02/03/2026', "TỶ GIÁ USD bán ra VCB"] = 26289.0

# 4. Other Historical Constants
vnibor_final = {
    '09/02/2026': 9.19, '10/02/2026': 8.51, '11/02/2026': 4.28,
    '12/02/2026': 4.28, '13/02/2026': 4.28, '23/02/2026': 6.39,
    '24/02/2026': 4.47, '25/02/2026': 4.47, '26/02/2026': 4.47,
    '27/02/2026': 4.47, '28/02/2026': 4.47, '02/03/2026': 4.47,
}
for d_str, val in vnibor_final.items():
    df.loc[df['Date_str'] == d_str, "VNIBOR qua đêm (%)"] = val

# =============================================================================

# ─── Convert ETF inflow from USD millions → BTC units ───────────────────────
# Formula: BTC = USD_millions * 1_000_000 / BTC_price_that_day
df["US Spot ETF Net Inflow (BTC)"] = np.nan
btc_col = pd.to_numeric(df["BTC"], errors='coerce')
etf_usd_col = pd.to_numeric(df["US Spot ETF Net Inflow (USDm)"], errors='coerce')
valid = btc_col.notna() & etf_usd_col.notna() & (btc_col > 0)
df.loc[valid, "US Spot ETF Net Inflow (BTC)"] = (
    etf_usd_col[valid] * 1_000_000 / btc_col[valid]
).round(2)
print("ETF BTC conversion done.")

# Replace explicit NAs/nans with empties
df.replace({np.nan: ""}, inplace=True)

df['Date'] = df['Date_str']

# Thay thế các ô trống bằng "" 
df = df.astype(object)
df.fillna("", inplace=True)
df.replace([np.nan, "NaN", "nan"], "", inplace=True)

cols_order = ["Date", "DXY", "US10Y (%)", "VN10Y (%)", "VNIBOR qua đêm (%)",
              "KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)", "TỶ GIÁ USD bán ra VCB",
              "USDT.D", "US Spot ETF Net Inflow (BTC)", "XAUXAG", "VIX",
              "giá Vàng", "giá Bạc (USD)", "BTC", "E1VFVN30 (VND)"]

cols_order = [c for c in cols_order if c in df.columns]
df = df[cols_order]
df_t = df.set_index("Date").T

output_dir = r"E:\OneDrive"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
output_path = os.path.join(output_dir, "Index tracking.xlsx")

# Print latest row for easy summary
print("\n--- LATEST DATA (Today) ---")
print(df.tail(1).to_string(index=False))
print("---------------------------\n")

try:
    df_t.to_excel(output_path)
    print(f"Saved successfully to {output_path}")
except Exception as e:
    print(f"Failed to save Excel (might be open): {e}")
    alt_path = output_path.replace(".xlsx", "_temp.xlsx")
    try:
        df_t.to_excel(alt_path)
        print(f"Saved instead to {alt_path}")
    except:
        print("Failed to save even to alt path.")
