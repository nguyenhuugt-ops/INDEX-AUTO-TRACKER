import yfinance as yf
from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import numpy as np
import datetime
import os
import time
import requests
import re
import xml.etree.ElementTree as ET
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
def get_config():
    output_dir = r"E:\OneDrive" if os.path.exists(r"E:\OneDrive") else "."
    file_path = os.path.join(output_dir, "Index tracking.xlsx")
    return output_dir, file_path

# --- 1. INITIALIZE DATA ---
def initialize_df(file_path, cols):
    if os.path.exists(file_path):
        print(f"Reading existing file: {file_path}")
        try:
            # Read excel, don't parse index immediately to handle mapping correctly
            df_raw = pd.read_excel(file_path, index_col=0)
            df = df_raw.T
            # Force index to be datetime with dayfirst support
            df.index = pd.to_datetime(df.index, dayfirst=True, errors='coerce')
            # Normalize to date only (remove time)
            df.index = df.index.normalize()
            # Filter valid dates and remove duplicates which cause crashes
            df = df[df.index.notnull()]
            df = df[~df.index.duplicated(keep='last')]
            
            # Filter duplicate columns (indicator rows)
            df = df.loc[:, ~df.columns.duplicated(keep='last')]
            
            df['Date_str'] = df.index.strftime('%d/%m/%Y')
            
            # Ensure all required columns exist
            for col in cols:
                if col not in df.columns:
                    df[col] = np.nan
            return df
        except Exception as e:
            print(f"Error reading file: {e}, creating new dataframe...")
    
    print("Creating new dataframe...")
    dates = pd.date_range(start="2026-01-01", end=datetime.datetime.now() + datetime.timedelta(days=30), freq='D')
    df = pd.DataFrame(index=dates)
    df['Date_str'] = df.index.strftime('%d/%m/%Y')
    for col in cols:
        df[col] = np.nan
    return df

# --- 2. OVERRIDES & MANUAL DATA ---
def apply_overrides(df):
    history_overrides = {
        'US Spot ETF Net Inflow (USDm)': {
            '02/03/2026': 94.0,   '03/03/2026': 114.7,  '04/03/2026': 155.3,  '05/03/2026': -139.2,
        }
    }
    for col, dates_dict in history_overrides.items():
        if col in df.columns:
            for d_str, val in dates_dict.items():
                date_mask = df['Date_str'] == d_str
                if date_mask.any():
                    df.loc[date_mask, col] = round(float(val), 2)
    return df

# --- 3. DATA FETCHING ---
def fetch_china_pmi():
    print("Fetching China PMI from DBnomics...")
    res = {"China PMI": np.nan}
    try:
        url = "https://api.db.nomics.world/v22/series/TradingEconomics/CHIPMIM?observations=1"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            val = data['series']['docs'][0]['value'][-1]
            if val is not None:
                res["China PMI"] = round(float(val), 2)
    except Exception as e:
        print(f"China PMI error: {e}")
    return res

def fetch_vndirect_foreign():
    print("Fetching E1VFVN30 foreign net flow from Fireant...")
    res = {}
    url = "https://restv2.fireant.vn/symbols/E1VFVN30/historical-foreign?startDate=2026-02-01&endDate=2026-12-31"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Origin': 'https://fireant.vn',
        'Referer': 'https://fireant.vn/'
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            for item in data:
                dt_str = datetime.datetime.strptime(item['date'][:10], '%Y-%m-%d').strftime('%d/%m/%Y')
                net_val = item.get('buyVal', 0) - item.get('sellVal', 0)
                res[dt_str] = round(net_val / 1e9, 2)
    except Exception as e:
        print(f"Fireant Error: {e}")
    return res

def fetch_yf_data(date_obj=None):
    print(f"Fetching YFinance data{' for ' + date_obj.strftime('%Y-%m-%d') if date_obj else ''}...")
    yf_map = {
        "DX-Y.NYB": "DXY", 
        "^TNX": "US10Y (%)", 
        "GC=F": "giá Vàng", 
        "SI=F": "giá Bạc (USD)", 
        "BTC-USD": "BTC", 
        "^VIX": "VIX", 
        "E1VFVN30.VN": "E1VFVN30 (VND)",
        "CL=F": "US OIL (WTI)"
    }
    res = {c: np.nan for c in yf_map.values()}
    
    for t, c in yf_map.items():
        try:
            if date_obj:
                start = (date_obj - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
                end = (date_obj + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                v_download = yf.download(t, start=start, end=end, progress=False)
                if not v_download.empty:
                    target_dt = pd.Timestamp(date_obj).normalize()
                    v_download.index = v_download.index.tz_localize(None).normalize()
                    if target_dt in v_download.index:
                        v = v_download.loc[target_dt, 'Close']
                    else:
                        v = v_download['Close'].iloc[-1]
                    val = float(v.iloc[0]) if isinstance(v, pd.Series) else float(v)
                    res[c] = round(val, 2)
            else:
                v_download = yf.download(t, period="1d", progress=False)
                if not v_download.empty:
                    v = v_download['Close'].iloc[-1]
                    val = float(v.iloc[0]) if isinstance(v, pd.Series) else float(v)
                    res[c] = round(val, 2)
        except: pass
    return res

def fetch_tv_data(date_obj=None):
    print(f"Fetching TradingView data (VN10Y, USDT.D, China PMI, EIA, Core PCE)...")
    res = {"VN10Y (%)": np.nan, "USDT.D": np.nan, "EIA Inventories (USDm)": np.nan, "Core PCE (%)": np.nan}
    try:
        tv = TvDatafeed()
        symbols = {
            "VN10Y (%)": ('VN10Y', 'TVC'),
            "USDT.D": ('USDT.D', 'CRYPTOCAP'),
            "EIA Inventories (USDm)": ('USCOSC', 'ECONOMICS'),
            "Core PCE (%)": ('USCPCEPIAC', 'ECONOMICS')
        }
        
        for col, (sym, exch) in symbols.items():
            try:
                hist = tv.get_hist(symbol=sym, exchange=exch, interval=Interval.in_daily, n_bars=10)
                if hist is not None and not hist.empty:
                    if date_obj:
                        target_dt = pd.Timestamp(date_obj).normalize()
                        hist.index = pd.to_datetime(hist.index).normalize()
                        if target_dt in hist.index:
                            res[col] = round(float(hist.loc[target_dt, 'close']), 2)
                        else:
                            past_data = hist[hist.index <= target_dt]
                            if not past_data.empty:
                                res[col] = round(float(past_data['close'].iloc[-1]), 2)
                    else:
                        res[col] = round(float(hist['close'].iloc[-1]), 2)
            except: pass
    except Exception as e:
        print(f"TV data error: {e}")
    return res

def fetch_vcb_rate(date_obj=None):
    print(f"Fetching VCB exchange rate...")
    url = "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx"
    res = {"TỶ GIÁ USD bán ra VCB": np.nan}
    try:
        resp = requests.get(url, verify=False, timeout=10)
        if resp.status_code == 200:
            root = ET.fromstring(resp.content)
            for exrate in root.findall('Exrate'):
                if exrate.get('CurrencyCode') == 'USD':
                    res["TỶ GIÁ USD bán ra VCB"] = float(exrate.get('Sell').replace(',', ''))
                    break
    except Exception as e:
        print(f"VCB error: {e}")
    return res

def fetch_vnibor(date_obj=None):
    print(f"Fetching VNIBOR from SBV...")
    url = "https://sbv.gov.vn/vi/lãi-suất1" 
    res = {"VNIBOR qua đêm (%)": np.nan}
    try:
        resp = requests.get(url, verify=False, timeout=15)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, 'html.parser')
            td_label = soup.find('td', string=re.compile(r'Qua đêm', re.I))
            if td_label:
                td_value = td_label.find_next_sibling('td')
                if td_value:
                    val_str = td_value.get_text(strip=True).replace(',', '.')
                    res["VNIBOR qua đêm (%)"] = float(val_str)
    except Exception as e:
        print(f"VNIBOR error: {e}")
    return res

def fetch_coinglass_etf():
    print("Fetching Coinglass ETF data...")
    res = {}
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.get("https://www.coinglass.com/etf/bitcoin")
        wait = WebDriverWait(driver, 30)
        rows = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "tr.ant-table-row")))
        for row in rows:
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 10:
                    date_str_raw = cells[0].text.strip()
                    total_str = cells[9].text.strip()
                    dt = datetime.datetime.strptime(date_str_raw, "%Y-%m-%d")
                    target_dt_str = dt.strftime("%d/%m/%Y")
                    val = 0.0
                    total_str = total_str.replace('+', '').replace(',', '')
                    if 'K' in total_str:
                        val = float(total_str.replace('K', '')) * 1000
                    elif 'M' in total_str:
                        val = float(total_str.replace('M', '')) * 1000000
                    else:
                        val = float(total_str)
                    res[target_dt_str] = round(val, 2)
            except: pass
        driver.quit()
    except Exception as e:
        print(f"Coinglass error: {e}")
    return res

def run_tracker(output_path=None):
    output_dir, file_path = get_config()
    if output_path:
        file_path = output_path
    
    cols = ["DXY", "US10Y (%)", "VN10Y (%)", "VNIBOR qua đêm (%)", 
            "KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)", "TỶ GIÁ USD bán ra VCB", 
            "USDT.D", "US Spot ETF Net Inflow (USDm)", "XAUXAG", "VIX", 
            "giá Vàng", "giá Bạc (USD)", "BTC", "E1VFVN30 (VND)",
            "US OIL (WTI)", "China PMI", "EIA Inventories (USDm)", "Core PCE (%)",
            "US Spot ETF Net Inflow (BTC)"]
    
    df = initialize_df(file_path, cols)
    df = apply_overrides(df)
    
    coinglass_data = fetch_coinglass_etf()
    vnd_foreign = fetch_vndirect_foreign()
    
    today = datetime.datetime.now().date()
    start_backfill = today - datetime.timedelta(days=14)
    target_dates = pd.date_range(start=start_backfill, end=today).date.tolist()
    
    for target_date in target_dates:
        target_dt = pd.to_datetime(target_date)
        dt_str = target_date.strftime('%d/%m/%Y')
        
        needs_update = False
        if target_dt in df.index:
            row = df.loc[target_dt]
            if pd.isna(row.get('DXY')) or pd.isna(row.get('BTC')):
                needs_update = True
        else:
            needs_update = True
            
        if needs_update:
            print(f"\n--- Updating data for {dt_str} ---")
            updates = fetch_yf_data(target_date if target_date != today else None)
            updates.update(fetch_tv_data(target_date if target_date != today else None))
            
            if target_date == today:
                updates.update(fetch_vcb_rate())
                updates.update(fetch_vnibor())
                
            # Use dbnomics for China PMI
            if 'pmi_val' not in locals():
                rpmi = fetch_china_pmi()
                pmi_val = rpmi.get("China PMI", np.nan)
            updates["China PMI"] = pmi_val
            
            if dt_str in coinglass_data:
                updates["US Spot ETF Net Inflow (BTC)"] = coinglass_data[dt_str]
                
            if dt_str in vnd_foreign:
                updates["KHỐI NGOẠI MUA BÁN RÒNG CK phiên hôm qua (tỷ)"] = vnd_foreign[dt_str]
                
            # Apply updates
            if target_dt not in df.index:
                df.loc[target_dt] = np.nan
                df.at[target_dt, 'Date_str'] = dt_str
                
            for col, val in updates.items():
                if col in df.columns:
                    current_val = df.at[target_dt, col]
                    # Handle Series if duplicate index somehow persists
                    if isinstance(current_val, pd.Series):
                        current_val = current_val.iloc[-1]
                        
                    # For BTC price, always update if it's the backfill and we found it
                    if pd.isna(current_val) or current_val == "" or (col == "BTC" and not pd.isna(val)):
                        df.at[target_dt, col] = val

    if "US Spot ETF Net Inflow (BTC)" not in df.columns:
        df["US Spot ETF Net Inflow (BTC)"] = np.nan

    for idx in df.index:
        try:
            btc = df.at[idx, "BTC"]
            etf_usd = df.at[idx, "US Spot ETF Net Inflow (USDm)"]
            etf_btc = df.at[idx, "US Spot ETF Net Inflow (BTC)"]
            if pd.isna(etf_btc) or etf_btc == "":
                if pd.notnull(btc) and pd.notnull(etf_usd) and float(btc) > 0:
                    df.at[idx, "US Spot ETF Net Inflow (BTC)"] = round(float(etf_usd) * 1_000_000 / float(btc), 2)
            for c in cols:
                if c in df.columns and pd.notnull(df.at[idx, c]):
                    try: df.at[idx, c] = round(float(df.at[idx, c]), 2)
                    except: pass
            if "giá Vàng" in df.columns and "giá Bạc (USD)" in df.columns:
                gv = df.at[idx, "giá Vàng"]
                gb = df.at[idx, "giá Bạc (USD)"]
                if pd.notnull(gv) and pd.notnull(gb) and float(gb) != 0:
                    df.at[idx, "XAUXAG"] = round(float(gv) / float(gb), 2)
        except: pass

    df.sort_index(inplace=True)
    target_cols = [c for c in cols if c in df.columns]
    
    # Drop rows that are completely empty across target_cols (Deduplicate garbage future dates)
    df.replace("", np.nan, inplace=True)
    df.dropna(subset=target_cols, how='all', inplace=True)
    
    # Drop completely future dates which are artifacts of past Month-Day swap bugs
    df = df[df.index <= pd.Timestamp(today)]
    
    df_save = df[target_cols].copy()
    df_save.index.name = "Date"
    # Ensure index is datetime before formatting
    if not isinstance(df_save.index, pd.DatetimeIndex):
        df_save.index = pd.to_datetime(df_save.index, dayfirst=True)
    df_save.index = df_save.index.strftime('%d/%m/%Y')
    df_save = df_save.astype(object)
    df_save.fillna("", inplace=True)

    try:
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
    except Exception as e:
        print(f"[ERROR] Failed to save to {file_path}: {e}")
        local_path = "Index_tracking_local.xlsx"
        df_save.T.to_excel(local_path)
        print(f"[RECOVERY] Saved to backup: {local_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", help="Path to output Excel file")
    args = parser.parse_args()
    run_tracker(output_path=args.output)
