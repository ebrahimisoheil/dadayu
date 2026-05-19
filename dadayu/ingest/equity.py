from __future__ import annotations

from io import StringIO
from urllib.request import Request, urlopen

import pandas as pd
import yfinance as yf

MARKETS = ["germany", "us", "india"]

INTERVAL_TABLE = {
    "1h": "prices_hourly",
    "4h": "prices_4h",
    "1d": "prices_daily",
}

_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36"
    )
}


def _fetch_html(url: str) -> str:
    req = Request(url, headers=_REQUEST_HEADERS)
    with urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8")


def _get_germany_tickers() -> list[str]:
    sources = {
        "DAX":    ["https://en.wikipedia.org/wiki/DAX"],
        "MDAX":   ["https://en.wikipedia.org/wiki/MDAX"],
        "SDAX":   ["https://en.wikipedia.org/wiki/SDAX", "https://ru.wikipedia.org/wiki/SDAX"],
        "TecDAX": ["https://en.wikipedia.org/wiki/TecDAX", "https://de.wikipedia.org/wiki/TecDAX"],
    }
    candidates = ["ticker symbol", "ticker", "symbol", "кратное", "тикер", "abbr.", "abbreviation"]
    tickers: set[str] = set()

    for index, urls in sources.items():
        for url in urls:
            try:
                tables = pd.read_html(StringIO(_fetch_html(url)))
                for table in tables:
                    col_map = {str(c).strip().lower(): c for c in table.columns}
                    found_col = None
                    for cand in candidates:
                        if cand in col_map:
                            found_col = col_map[cand]
                            break
                        for key, orig in col_map.items():
                            if key.startswith(cand) and len(key) < len(cand) + 6:
                                found_col = orig
                                break
                        if found_col:
                            break
                    if found_col is None:
                        continue
                    raw = table[found_col].astype(str).str.strip().str.replace(r"\s+", "", regex=True)
                    raw = raw[raw.str.len() > 1]
                    for t in raw:
                        base = t.replace(".DE", "")
                        if "." in t and not t.endswith(".DE"):
                            tickers.add(t)
                        else:
                            tickers.add(base + ".DE")
                    break
            except Exception as exc:
                print(f"  [WARN] {index} from {url}: {exc}")
                continue
            break

    return sorted(tickers)


def _get_us_tickers() -> list[str]:
    def _convert(t: str) -> str:
        parts = t.split(".")
        return f"{parts[0]}-{parts[1]}" if len(parts) == 2 and len(parts[1]) <= 2 else t

    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        raw = tables[0]["Symbol"].astype(str).str.strip()
        return sorted(_convert(t) for t in raw)
    except Exception as exc:
        print(f"  [WARN] Wikipedia S&P500 failed: {exc} — trying GitHub backup...")

    try:
        import requests
        url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
        resp = requests.get(url, headers=_REQUEST_HEADERS, timeout=15)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text))
        raw = df["Symbol"].astype(str).str.strip()
        return sorted(_convert(t) for t in raw)
    except Exception as exc:
        print(f"  [WARN] GitHub S&P500 backup failed: {exc}")
        return []


def _get_india_tickers() -> list[str]:
    url = "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"
    try:
        content = _fetch_html(url)
        df = pd.read_csv(StringIO(content))
        if "Symbol" not in df.columns:
            raise ValueError("No Symbol column")
        return sorted(t + ".NS" for t in df["Symbol"].astype(str).str.strip())
    except Exception as exc:
        print(f"  [WARN] NSE CSV failed: {exc} — trying Wikipedia fallback")

    wiki_sources = [
        "https://en.wikipedia.org/wiki/NIFTY_50",
        "https://en.wikipedia.org/wiki/Nifty_Next_50",
    ]
    tickers: set[str] = set()
    for url in wiki_sources:
        try:
            tables = pd.read_html(StringIO(_fetch_html(url)))
            for table in tables:
                col_map = {str(c).strip().lower(): c for c in table.columns}
                if "symbol" in col_map:
                    raw = table[col_map["symbol"]].astype(str).str.strip()
                    for t in raw[raw.str.len() > 1]:
                        tickers.add(t if t.endswith(".NS") else t + ".NS")
                    break
        except Exception as exc:
            print(f"  [WARN] {url}: {exc}")
    return sorted(tickers)


def get_tickers(market: str) -> list[str]:
    print(f"  Fetching tickers for {market}...")
    if market == "germany":
        tickers = _get_germany_tickers()
    elif market == "us":
        tickers = _get_us_tickers()
    elif market == "india":
        tickers = _get_india_tickers()
    else:
        raise ValueError(f"Unknown market: {market}")
    print(f"  Found {len(tickers)} tickers")
    return tickers


def download_ohlcv(tickers: list[str], start: str, end: str, interval: str) -> pd.DataFrame:
    print(f"  Downloading {interval} data {start} → {end} for {len(tickers)} tickers...")
    end_exclusive = (pd.Timestamp(end) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    raw = yf.download(
        tickers=tickers,
        start=start,
        end=end_exclusive,
        interval=interval,
        auto_adjust=True,
        progress=True,
        group_by="ticker",
        threads=True,
    )

    if raw.empty:
        print("  [WARN] No data returned.")
        return pd.DataFrame()

    rows: list[pd.DataFrame] = []
    for ticker in tickers:
        try:
            if isinstance(raw.columns, pd.MultiIndex):
                if ticker not in raw.columns.get_level_values(0):
                    continue
                df = raw[ticker].copy()
            else:
                df = raw.copy()

            df = df.dropna(subset=["Close"])
            if df.empty:
                continue

            df.index = pd.to_datetime(df.index).tz_localize(None)
            df = df.reset_index().rename(columns={
                "index": "Datetime", "Datetime": "Datetime",
                "Date": "Datetime", "Price": "Datetime",
            })
            df["Ticker"] = ticker
            keep = ["Datetime", "Ticker", "Open", "High", "Low", "Close", "Volume"]
            rows.append(df[[c for c in keep if c in df.columns]])
        except Exception as exc:
            print(f"  [WARN] Parse failed for {ticker}: {exc}")

    if not rows:
        return pd.DataFrame()

    prices = pd.concat(rows, ignore_index=True)
    prices = prices.sort_values(["Ticker", "Datetime"]).reset_index(drop=True)
    print(f"  Got {len(prices):,} rows for {prices['Ticker'].nunique()} tickers")
    return prices


def fetch_ticker_metadata(tickers: list[str], market: str) -> pd.DataFrame:
    import time

    YFINANCE_FIELDS = {
        "longName":   "name",
        "sector":     "sector",
        "industry":   "industry",
        "currency":   "currency",
        "country":    "country",
        "marketCap":  "market_cap",
        "trailingPE": "pe_ratio",
    }

    records = []
    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i}/{len(tickers)}] {ticker}", end="\r")
        row: dict = {"ticker": ticker, "market": market}
        try:
            info = yf.Ticker(ticker).info
            for yf_key, col in YFINANCE_FIELDS.items():
                row[col] = info.get(yf_key)
        except Exception as exc:
            print(f"\n  [WARN] {ticker}: {exc}")
            for col in YFINANCE_FIELDS.values():
                row.setdefault(col, None)
        records.append(row)
        time.sleep(0.1)
    print()
    return pd.DataFrame(records)
