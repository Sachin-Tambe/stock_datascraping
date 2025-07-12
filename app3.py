import os
import streamlit as st
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---- Streamlit Page Config ----
st.set_page_config(page_title="📈 Yahoo ISIN Fetcher (Current Data)", layout="wide")
st.title("📊 NSE ISIN to Yahoo Stock Data (Current)")

# ---- File paths ----
MASTER_CSV = "Master.csv"
TICKERS_CSV = "Yahoo_Tickers.csv"

# ---- Function: Get Yahoo Symbol by Company Name ----
def get_yahoo_symbol_via_api(company_name):
    query = company_name.replace(" ", "%20")
    url = f"https://query2.finance.yahoo.com/v1/finance/search?q={query}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=5)
        data = r.json()
        for item in data.get("quotes", []):
            if item.get("exchange") in ["NSI", "BSE"]:
                return item.get("symbol")
    except Exception:
        pass
    return None

# ---- Function: Fetch Current Stock Data with Fallback ----
def fetch_single_stock(symbol, isin):
    default = "N/A"
    # Invalid symbol handling
    if not symbol or pd.isna(symbol):
        return {
            "ISIN": isin,
            "Symbol": symbol,
            "Current Price": "Invalid Symbol",
            "52‑Week High": "Invalid",
            "52‑Week Low": "Invalid",
            "Today's High": "Invalid",
            "Today's Low": "Invalid",
            "Scraped At": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        hist = ticker.history(period="2d")
        if hist.empty:
            high = low = default
        else:
            row = hist.iloc[-1]
            high = round(row['High'], 2)
            low = round(row['Low'], 2)
        return {
            "ISIN": isin,
            "Symbol": symbol,
            "Current Price": info.get("regularMarketPrice", default),
            "52‑Week High": info.get("fiftyTwoWeekHigh", default),
            "52‑Week Low": info.get("fiftyTwoWeekLow", default),
            "Today's High": high,
            "Today's Low": low,
            "Scraped At": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception:
        return {
            "ISIN": isin,
            "Symbol": symbol,
            "Current Price": "Error",
            "52‑Week High": "Error",
            "52‑Week Low": "Error",
            "Today's High": "Error",
            "Today's Low": "Error",
            "Scraped At": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

# ---- Function: Multithreaded Yahoo Symbol Fetching ----
def fetch_yahoo_tickers_parallel(df, max_threads=20):
    results = [None] * len(df)
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {executor.submit(get_yahoo_symbol_via_api, row['COMPANYNAME']): i
                   for i, row in df.iterrows()}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                results[idx] = future.result()
            except Exception:
                results[idx] = None
    return results

# ---- UI: Step 1 - Load Master.csv ----
st.markdown("### ✅ Step 1: Load Master CSV")
try:
    df = pd.read_csv(MASTER_CSV)
    df.columns = df.columns.str.strip().str.upper()
    if 'COMPANYNAME' not in df.columns:
        st.error("❌ CSV must contain a 'COMPANYNAME' column.")
    else:
        st.session_state['nse_data'] = df
        st.success(f"✅ Loaded {len(df)} companies")
        st.dataframe(df.head())
except Exception as e:
    st.error(f"❌ Could not load {MASTER_CSV}: {e}")

# ---- UI: Step 2 - Load or Map Yahoo Symbols ----
st.markdown("### 2️⃣ Load/Map Yahoo Symbols")
refresh_tickers = st.checkbox("🔄 Refresh tickers from Yahoo (ignore saved file)")

if not refresh_tickers and os.path.exists(TICKERS_CSV):
    st.info(f"🔄 Loading saved tickers from {TICKERS_CSV}")
    df_tickers = pd.read_csv(TICKERS_CSV)
    st.session_state['yahoo_data'] = df_tickers
    st.success(f"✅ Loaded {len(df_tickers)} saved tickers.")
    st.dataframe(df_tickers.head())
else:
    if st.button("🔍 Find Yahoo Tickers and Save"):
        if 'nse_data' not in st.session_state:
            st.warning("Please load the CSV first.")
        else:
            df = st.session_state['nse_data']
            st.info("🔄 Fetching Yahoo symbols…")
            symbols = fetch_yahoo_tickers_parallel(df)
            df['YAHOO_SYMBOL'] = symbols
            df_valid = df.dropna(subset=['YAHOO_SYMBOL'])
            df_valid.to_csv(TICKERS_CSV, index=False)
            st.session_state['yahoo_data'] = df_valid
            st.success(f"✅ Mapped & saved {len(df_valid)} symbols to {TICKERS_CSV}")
            st.dataframe(df_valid.head())

# ---- UI: Step 3 - Fetch Current Data ----
st.markdown("### 3️⃣ Fetch Current Stock Data")
if st.button("📈 Fetch Current Data"):
    if 'yahoo_data' not in st.session_state or st.session_state['yahoo_data'].empty:
        st.warning("Please map or load Yahoo tickers first.")
    else:
        df_yahoo = st.session_state['yahoo_data']
        data = []
        st.info("🔄 Scraping current data…")

        with ThreadPoolExecutor(max_workers=15) as executor:
            futures = [executor.submit(fetch_single_stock,
                                       row['YAHOO_SYMBOL'],
                                       row.get('ISIN', ''))
                       for _, row in df_yahoo.iterrows()]
            for future in as_completed(futures):
                data.append(future.result())

        result_df = pd.DataFrame(data)
        # Validate output
        if (result_df["Today's High"] == "N/A").all() and (result_df["Today's Low"] == "N/A").all():
            st.error("❌ All values are N/A. Market may be closed or symbols invalid.")
        filename = f"Yahoo_ISIN_Current_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        result_df.to_excel(filename, index=False)
        st.success("✅ Data fetched!")
        st.dataframe(result_df.head())

        with open(filename, "rb") as f:
            st.download_button("📥 Download Excel", f, filename,
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
