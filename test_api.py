import requests
import json
import urllib.request
import ssl

def check_dbnomics():
    print("Testing DBnomics China PMI...")
    url = 'https://api.db.nomics.world/v22/series/TradingEconomics/CHIPMIM?observations=1'
    try:
        resp = requests.get(url, timeout=5)
        print("Status:", resp.status_code)
        if resp.status_code == 200:
            print("Value:", resp.json()['series']['docs'][0]['value'][-1])
    except Exception as e:
        print("Error:", e)

def check_fireant_market():
    print("\nTesting Fireant Market VNINDEX...")
    url = 'https://restv2.fireant.vn/symbols/VNINDEX/historical-foreign?startDate=2026-02-01&endDate=2026-03-31'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Origin': 'https://fireant.vn',
        'Referer': 'https://fireant.vn/'
    }
    try:
        # ctx = ssl.create_default_context()
        # ctx.check_hostname = False
        # ctx.verify_mode = ssl.CERT_NONE
        # req = urllib.request.Request(url, headers=headers)
        # with urllib.request.urlopen(req, context=ctx, timeout=5) as response:
        #     data = json.loads(response.read().decode())
        resp = requests.get(url, headers=headers, timeout=5)
        print("Status:", resp.status_code)
        if resp.status_code == 200:
            data = resp.json()
            for d in data[:3]:
                net = d.get('buyVal', 0) - d.get('sellVal', 0)
                print(d['date'], "Net:", net / 1e9)
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    check_dbnomics()
    check_fireant_market()
