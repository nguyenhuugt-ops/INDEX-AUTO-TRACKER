import requests
import re

def get_vn10y():
    print("Testing VN10Y from Investing.com...")
    url = "https://vn.investing.com/rates-bonds/vietnam-10-year-bond-yield"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        print("Status:", resp.status_code)
        # Search for the yield value (it usually looks like a 3-4 digit percentage in a specific div)
        # Regex for current value on Investing.com bond page
        match = re.search(r'data-test="last-price">([\d,.]+)<', resp.text)
        if match:
            print("Value:", match.group(1))
        else:
            # Fallback regex
            match = re.search(r'instrument-price-last">([\d,.]+)<', resp.text)
            if match:
                print("Fallback Value:", match.group(1))
            else:
                print("Not found in text chunk.")
    except Exception as e:
        print("Error:", e)

def get_china_pmi():
    print("\nTesting China PMI from TradingEconomics...")
    url = "https://tradingeconomics.com/china/manufacturing-pmi"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        print("Status:", resp.status_code)
        # Look for the current value in the table
        match = re.search(r'<td>(\d+\.\d+)</td>', resp.text)
        if match:
            print("Value:", match.group(1))
        else:
            print("Not found in text chunk.")
    except Exception as e:
        print("Error:", e)

def get_fireant_foreign_whole():
    print("\nTesting Fireant Whole Market Foreign...")
    # Fireant uses this for whole market foreign investment
    url = "https://restv2.fireant.vn/market/foreign-investment?type=1"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        print("Status:", resp.status_code)
        if resp.status_code == 200:
            data = resp.json()
            for item in data[:3]:
                net = (item.get('buyValue', 0) - item.get('sellValue', 0)) / 1e9
                print(f"{item['date'][:10]}: {net:.2f} tỷ")
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    get_vn10y()
    get_china_pmi()
    get_fireant_foreign_whole()
