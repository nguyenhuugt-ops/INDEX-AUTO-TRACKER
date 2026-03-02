# INDEX-AUTO-TRACKER

Automated daily tracking of financial indices, exchange rates, and crypto ETF inflows.

## Features
- **Foreign Investment Tracking**: Fetches exact net buying/selling values from Fireant and CafeF (HOSE/HSX).
- **Exchange Rates**: Real-time USD/VND rates from Vietcombank (VCB) with fallback sources.
- **Crypto ETF Inflows**: Captures US Spot BTC ETF net inflows from Farside and converts them to BTC units.
- **Global Macro Indices**: DXY, US10Y, VN10Y, VIX, Gold, Silver, and more via Yahoo Finance and TradingView.
- **Automated Reporting**: Generates a formatted Excel report daily.

## Setup
1. Install dependencies:
   ```bash
   pip install yfinance pandas numpy requests beautifulsoup4 selenium webdriver-manager tvDatafeed
   ```
2. Run the tracker:
   ```bash
   python daily_index_tracker.py
   ```

## Data Sources
- **Fireant / CafeF**: VN Stocks & Foreign Investor data.
- **Vietcombank**: Tỷ giá USD.
- **Farside**: Bitcoin ETF flows.
- **Yahoo Finance**: Global indices.
- **SBV (NHNN)**: VNIBOR rates.
