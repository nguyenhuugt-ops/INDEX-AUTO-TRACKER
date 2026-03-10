import pandas as pd

file_path = "E:\\OneDrive\\Index tracking.xlsx"
df_raw = pd.read_excel(file_path, index_col=0)

print("Duplicates in raw index (Excel rows):", df_raw.index.duplicated().sum())
print("Rows matching 'US Spot ETF Net Inflow (BTC)':")
print(df_raw[df_raw.index == "US Spot ETF Net Inflow (BTC)"])

# Fix duplicates by keeping last
df_clean = df_raw[~df_raw.index.duplicated(keep="first")]

df_clean.to_excel("E:\\OneDrive\\Index tracking.xlsx")
print("Cleaned and saved.")
