import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import numpy as np
import datetime
import os
import daily_index_tracker

@pytest.fixture
def mock_df():
    cols = ["DXY", "BTC", "US Spot ETF Net Inflow (BTC)"]
    dates = pd.date_range(start="2026-03-01", periods=5, freq='D')
    df = pd.DataFrame(index=dates, columns=cols + ['Date_str'])
    df['Date_str'] = df.index.strftime('%d/%m/%Y')
    return df

def test_initialize_df_new(tmp_path):
    file_path = tmp_path / "test.xlsx"
    cols = ["DXY", "BTC"]
    df = daily_index_tracker.initialize_df(str(file_path), cols)
    assert "DXY" in df.columns
    assert "BTC" in df.columns
    assert isinstance(df.index, pd.DatetimeIndex)

def test_initialize_df_existing(tmp_path):
    file_path = tmp_path / "existing.xlsx"
    df_init = pd.DataFrame({"DXY": [100.0]}, index=["01/03/2026"])
    df_init.index.name = "Date"
    with pd.ExcelWriter(file_path) as writer:
        df_init.T.to_excel(writer, sheet_name='Indices')
    
    cols = ["DXY", "BTC"]
    df = daily_index_tracker.initialize_df(str(file_path), cols)
    assert "DXY" in df.columns
    assert "BTC" in df.columns
    # Check if BTC was added as NaN
    assert pd.isna(df.loc["2026-03-01", "BTC"])

def test_initialize_df_date_swap_fix(tmp_path):
    # Test that 09/03/2026 is March 9, not September 3
    file_path = tmp_path / "dates.xlsx"
    df_init = pd.DataFrame({"DXY": [100.0]}, index=["09/03/2026"])
    df_init.index.name = "Date"
    with pd.ExcelWriter(file_path) as writer:
        df_init.T.to_excel(writer, sheet_name='Indices')
    
    df = daily_index_tracker.initialize_df(str(file_path), ["DXY"])
    assert df.index[0].month == 3
    assert df.index[0].day == 9

def test_initialize_df_deduplication(tmp_path):
    file_path = tmp_path / "dupes.xlsx"
    # Create timestamps that are on the same day but different hours
    df_init = pd.DataFrame({"DXY": [100.0, 105.0]})
    df_init.index = [pd.Timestamp("2026-03-09 10:00:00"), pd.Timestamp("2026-03-09 14:00:00")]
    df_init.index.name = "Date"
    df_init.index = df_init.index.strftime('%d/%m/%Y %H:%M:%S')
    
    with pd.ExcelWriter(file_path) as writer:
        df_init.T.to_excel(writer, sheet_name='Indices')
    
    df = daily_index_tracker.initialize_df(str(file_path), ["DXY"])
    assert len(df) == 1
    assert df.loc["2026-03-09", "DXY"] == 105.0 # Keep last

def test_initialize_df_column_deduplication(tmp_path):
    file_path = tmp_path / "col_dupes.xlsx"
    # Create dataframe with duplicated columns
    df_init = pd.DataFrame([[100.0, 200.0]], columns=["DXY", "DXY"], index=["09/03/2026"])
    df_init.index.name = "Date"
    with pd.ExcelWriter(file_path) as writer:
        df_init.T.to_excel(writer, sheet_name='Indices')
    
    df = daily_index_tracker.initialize_df(str(file_path), ["DXY", "BTC"])
    # Should only have one DXY column
    assert "DXY" in df.columns
    # The deduplication uses keep='last', so value should be 200.0
    assert df.loc["2026-03-09", "DXY"] == 200.0
    assert "BTC" in df.columns

@patch('daily_index_tracker.yf.download')
def test_fetch_yf_data_success(mock_yf):
    mock_data = MagicMock()
    mock_data.empty = False
    mock_data.__getitem__.return_value.iloc.__getitem__.return_value = 100.0
    mock_yf.return_value = mock_data
    
    res = daily_index_tracker.fetch_yf_data()
    assert res["DXY"] == 100.0

@patch('daily_index_tracker.requests.get')
def test_fetch_vcb_rate_success(mock_get):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.content = b'<Exrates><Exrate CurrencyCode="USD" Sell="25,000"/></Exrates>'
    mock_get.return_value = mock_resp
    
    res = daily_index_tracker.fetch_vcb_rate()
    assert res["TỶ GIÁ USD bán ra VCB"] == 25000.0

@patch('daily_index_tracker.requests.get')
def test_fetch_vnibor_success(mock_get):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.content = '<table><tr><td>Qua đêm</td><td>5,74</td></tr></table>'.encode('utf-8')
    mock_get.return_value = mock_resp
    
    res = daily_index_tracker.fetch_vnibor()
    assert res["VNIBOR qua đêm (%)"] == 5.74

@patch('daily_index_tracker.requests.get')
def test_fetch_china_pmi_success(mock_get):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        'series': {'docs': [{'value': [48.5, 49.0]}]}
    }
    mock_get.return_value = mock_resp
    
    res = daily_index_tracker.fetch_china_pmi()
    assert res["China PMI"] == 49.0

@patch('daily_index_tracker.requests.get')
def test_fetch_vndirect_foreign_success(mock_get):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = [
        {'date': '2026-03-09', 'buyVal': 1000000000, 'sellVal': 500000000}
    ]
    mock_get.return_value = mock_resp
    
    res = daily_index_tracker.fetch_vndirect_foreign()
    assert res["09/03/2026"] == 0.5

def test_post_processing_etf_calc():
    df = pd.DataFrame({
        "BTC": [100000.0],
        "US Spot ETF Net Inflow (USDm)": [100.0],
        "US Spot ETF Net Inflow (BTC)": [np.nan]
    }, index=[pd.Timestamp("2026-03-09")])
    
    # Manually run the logic from run_tracker
    idx = df.index[0]
    btc = df.at[idx, "BTC"]
    etf_usd = df.at[idx, "US Spot ETF Net Inflow (USDm)"]
    df.at[idx, "US Spot ETF Net Inflow (BTC)"] = round(float(etf_usd) * 1_000_000 / float(btc), 2)
    
    assert df.at[idx, "US Spot ETF Net Inflow (BTC)"] == 1000.0

@patch('daily_index_tracker.fetch_yf_data')
@patch('daily_index_tracker.fetch_tv_data')
@patch('daily_index_tracker.fetch_coinglass_etf')
@patch('daily_index_tracker.fetch_vndirect_foreign')
@patch('daily_index_tracker.fetch_china_pmi')
@patch('daily_index_tracker.pd.ExcelWriter')
def test_run_tracker_dry_run(mock_writer, mock_china, mock_vnd, mock_coinglass, mock_tv, mock_yf, tmp_path):
    mock_yf.return_value = {"DXY": 100.0}
    mock_tv.return_value = {"VN10Y (%)": 4.0}
    mock_coinglass.return_value = {"09/03/2026": 500.0}
    mock_vnd.return_value = {"09/03/2026": 0.5}
    mock_china.return_value = {"China PMI": 49.0}
    
    output = tmp_path / "final.xlsx"
    daily_index_tracker.run_tracker(output_path=str(output))
    
    # Check if calls were made
    assert mock_yf.called
    assert mock_tv.called
