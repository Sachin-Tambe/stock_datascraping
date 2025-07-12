import streamlit as st
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(page_title="📈 Yahoo ISIN Fetcher", layout="wide")
st.title("📊 NSE ISIN to Yahoo Stock Data")

def get_yahoo_symbol_via_api(company_name):
    query = company_name.replace(" ", "%20")
    url = f"https://query2.finance.yahoo.com/v1/finance/search?q={query}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers)
        data = r.json()
        for item in data.get("quotes", []):
            if item.get("exchange") in ["NSI", "BSE"]:
                return item.get("symbol")
        return None
    except:
        return None

def fetch_single_stock(symbol, isin):
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        hist = ticker.history(period="1d")

        return {
            "ISIN": isin,
            "Symbol": symbol,
            "Current Price": info.get("regularMarketPrice", "N/A"),
            "52 Week High": info.get("fiftyTwoWeekHigh", "N/A"),
            "52 Week Low": info.get("fiftyTwoWeekLow", "N/A"),
            "Today's High": hist['High'].iloc[-1] if not hist.empty else "N/A",
            "Today's Low": hist['Low'].iloc[-1] if not hist.empty else "N/A",
            "Date": datetime.today().strftime('%Y-%m-%d')
        }
    except:
        return {
            "ISIN": isin,
            "Symbol": symbol,
            "Current Price": "Error",
            "52 Week High": "Error",
            "52 Week Low": "Error",
            "Today's High": "Error",
            "Today's Low": "Error",
            "Date": datetime.today().strftime('%Y-%m-%d')
        }

def fetch_yahoo_tickers_parallel(df, max_threads=20):
    def fetch_symbol(row):
        return get_yahoo_symbol_via_api(row['COMPANYNAME'])

    results = [None] * len(df)
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {executor.submit(fetch_symbol, row): i for i, row in df.iterrows()}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                results[idx] = future.result()
            except:
                results[idx] = None
    return results

# ---------------------- UI ----------------------
st.markdown("### ✅ Step 1: Read Master CSV File from Local Path")

try:
    df = pd.read_csv("Master.csv")
    df.columns = df.columns.str.strip().str.upper()
    if 'COMPANYNAME' not in df.columns:
        st.error("❌ CSV must contain a 'COMPANY' column named exactly.")
    else:
        st.session_state['nse_data'] = df
        st.success(f"✅ Loaded {len(df)} companies from Master.csv")
        st.dataframe(df.head())
except Exception as e:
    st.error(f"❌ Could not load Master.csv: {e}")

st.markdown("### 2️⃣ Scrap Yahoo Tickers using Company Name")
if st.button("🔍 Find Yahoo Tickers"):
    if 'nse_data' not in st.session_state:
        st.warning("Please ensure Master.csv is read correctly.")
    else:
        df = st.session_state['nse_data']
        st.info("🔄 Fetching Yahoo symbols using multithreading...")
        yahoo_symbols = fetch_yahoo_tickers_parallel(df)
        df['YAHOO_SYMBOL'] = yahoo_symbols
        df_valid = df.dropna(subset=['YAHOO_SYMBOL'])
        st.session_state['yahoo_data'] = df_valid
        df_valid.to_csv("Yahoo_Tickers.csv", index=False)
        st.success(f"✅ Mapped {len(df_valid)} Yahoo tickers.")
        st.dataframe(df_valid.head())

st.markdown("### 3️⃣ Fetch Stock Data from Yahoo")
if st.button("📈 Fetch Live Stock Data"):
    if 'yahoo_data' not in st.session_state:
        st.warning("Please fetch Yahoo tickers first.")
    else:
        data = []
        df_yahoo = st.session_state['yahoo_data']
        with ThreadPoolExecutor(max_workers=15) as executor:
            futures = [executor.submit(fetch_single_stock, row['YAHOO_SYMBOL'], row.get('ISIN', '')) for _, row in df_yahoo.iterrows()]
            for future in as_completed(futures):
                data.append(future.result())

        result_df = pd.DataFrame(data)
        filename = f"Yahoo_ISIN_Data_{datetime.now().strftime('%Y%m%d')}.xlsx"
        result_df.to_excel(filename, index=False)
        st.success("✅ Data fetched successfully!")
        st.dataframe(result_df.head())

        with open(filename, "rb") as f:
            st.download_button("📥 Download Excel", f, filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
