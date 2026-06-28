from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO
from pathlib import Path
from urllib.request import Request, urlopen

import pandas as pd

MARKETS = ["germany", "us"]

INTERVAL_TABLE = {
    "1d": "prices_daily",
}

EQUITY_EXTRA_CSV = Path(__file__).parent.parent.parent / "warehouse" / "seeds" / "equity_universe_extra.csv"

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


def _clean_text(value) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "null"} else text


def _convert_us_symbol(symbol: str) -> str:
    symbol = str(symbol).strip()
    parts = symbol.split(".")
    return f"{parts[0]}-{parts[1]}" if len(parts) == 2 and len(parts[1]) <= 2 else symbol


def _normalize_metadata(record: dict) -> dict:
    return {
        "name": _clean_text(record.get("name")),
        "sector": _clean_text(record.get("sector")),
        "industry": _clean_text(record.get("industry")),
        "currency": _clean_text(record.get("currency")),
        "country": _clean_text(record.get("country")),
        "market_cap": record.get("market_cap"),
        "pe_ratio": record.get("pe_ratio"),
    }


def _get_germany_membership() -> list[tuple[str, str]]:
    sources = {
        "DAX":    ["https://en.wikipedia.org/wiki/DAX"],
        "MDAX":   ["https://en.wikipedia.org/wiki/MDAX"],
        "SDAX":   ["https://en.wikipedia.org/wiki/SDAX", "https://ru.wikipedia.org/wiki/SDAX"],
        "TecDAX": ["https://en.wikipedia.org/wiki/TecDAX", "https://de.wikipedia.org/wiki/TecDAX"],
    }
    candidates = ["ticker symbol", "ticker", "symbol", "кратное", "тикер", "abbr.", "abbreviation"]
    pairs: list[tuple[str, str]] = []

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
                            ticker = t
                        else:
                            ticker = base + ".DE"
                        pairs.append((ticker, index))
                    break
            except Exception as exc:
                print(f"  [WARN] {index} from {url}: {exc}")
                continue
            break

    return pairs


def _get_germany_tickers() -> list[str]:
    return sorted({t for t, _ in _get_germany_membership()})


def _get_us_tickers() -> list[str]:
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        raw = tables[0]["Symbol"].astype(str).str.strip()
        return sorted(_convert_us_symbol(t) for t in raw)
    except Exception as exc:
        print(f"  [WARN] Wikipedia S&P500 failed: {exc} — trying GitHub backup...")

    try:
        import requests
        url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
        resp = requests.get(url, headers=_REQUEST_HEADERS, timeout=15)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text))
        raw = df["Symbol"].astype(str).str.strip()
        return sorted(_convert_us_symbol(t) for t in raw)
    except Exception as exc:
        print(f"  [WARN] GitHub S&P500 backup failed: {exc}")
        return []


def _get_us_company_metadata() -> dict[str, dict]:
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        df = tables[0]
        records = {}
        for _, row in df.iterrows():
            ticker = _convert_us_symbol(row.get("Symbol", ""))
            if not ticker:
                continue
            records[ticker] = _normalize_metadata({
                "name": row.get("Security"),
                "sector": row.get("GICS Sector"),
                "industry": row.get("GICS Sub-Industry"),
                "currency": "USD",
                "country": "United States",
            })
        return records
    except Exception as exc:
        print(f"  [WARN] Wikipedia S&P500 metadata failed: {exc} — trying GitHub backup...")

    try:
        import requests
        url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
        resp = requests.get(url, headers=_REQUEST_HEADERS, timeout=15)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text))
        records = {}
        for _, row in df.iterrows():
            ticker = _convert_us_symbol(row.get("Symbol", ""))
            if not ticker:
                continue
            records[ticker] = _normalize_metadata({
                "name": row.get("Name"),
                "sector": row.get("Sector"),
                "industry": row.get("Sub-Industry"),
                "currency": "USD",
                "country": "United States",
            })
        return records
    except Exception as exc:
        print(f"  [WARN] GitHub S&P500 metadata backup failed: {exc}")
        return {}


def _get_extra_metadata(market: str) -> dict[str, dict]:
    if not EQUITY_EXTRA_CSV.exists():
        return {}
    df = pd.read_csv(EQUITY_EXTRA_CSV)
    if "ticker" not in df.columns or "market" not in df.columns:
        return {}
    matched = df[df["market"].astype(str).str.lower() == market]
    records = {}
    for _, row in matched.iterrows():
        ticker = _clean_text(row.get("ticker"))
        if not ticker:
            continue
        records[ticker] = _normalize_metadata({
            "name": row.get("name"),
            "sector": row.get("sector"),
            "industry": row.get("industry"),
        })
    return records


def _fetch_yfinance_metadata(ticker: str) -> dict:
    try:
        import yfinance as yf

        info = yf.Ticker(ticker).info or {}
        return _normalize_metadata({
            "name": info.get("shortName") or info.get("longName"),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "currency": info.get("currency"),
            "country": info.get("country"),
            "market_cap": info.get("marketCap"),
            "pe_ratio": info.get("trailingPE") or info.get("forwardPE"),
        })
    except Exception as exc:
        print(f"  [WARN] yfinance metadata failed for {ticker}: {exc}")
        return {}


def _needs_yfinance_fallback(record: dict) -> bool:
    return (
        not record.get("sector")
        or not record.get("industry")
        or record.get("market_cap") is None
        or not record.get("name")
    )


def _get_extra_tickers(market: str) -> list[str]:
    if not EQUITY_EXTRA_CSV.exists():
        return []
    df = pd.read_csv(EQUITY_EXTRA_CSV)
    if "ticker" not in df.columns or "market" not in df.columns:
        return []
    matched = df[df["market"].astype(str).str.lower() == market]
    return sorted(matched["ticker"].dropna().astype(str).str.strip().unique())


def get_tickers(market: str) -> list[str]:
    print(f"  Fetching tickers for {market}...")
    if market == "germany":
        tickers = _get_germany_tickers()
    elif market == "us":
        tickers = _get_us_tickers()
    else:
        raise ValueError(f"Unknown market: {market}")
    tickers = sorted(set(tickers) | set(_get_extra_tickers(market)))
    print(f"  Found {len(tickers)} tickers")
    return tickers


def get_index_membership(market: str) -> list[tuple[str, str]]:
    if market == "germany":
        return sorted(set(_get_germany_membership()))
    if market == "us":
        return sorted((t, "SP500") for t in _get_us_tickers())
    raise ValueError(f"Unknown market: {market}")


def download_ohlcv(tickers: list[str], start: str, end: str, interval: str) -> pd.DataFrame:
    import yfinance as yf

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
    country = "United States" if market == "us" else "Germany"
    currency = "USD" if market == "us" else "EUR"
    source_metadata = _get_us_company_metadata() if market == "us" else {}
    extra_metadata = _get_extra_metadata(market)

    base_records: dict[str, dict] = {}
    for ticker in tickers:
        record = {
            "ticker": ticker,
            "market": market,
            "name": ticker,
            "sector": "",
            "industry": "",
            "currency": currency,
            "country": country,
            "market_cap": None,
            "pe_ratio": None,
        }
        for source in (source_metadata.get(ticker, {}), extra_metadata.get(ticker, {})):
            for key, value in source.items():
                if value not in (None, ""):
                    record[key] = value
        base_records[ticker] = record

    fallback_tickers = [ticker for ticker, record in base_records.items() if _needs_yfinance_fallback(record)]
    if fallback_tickers:
        print(f"  Fetching yfinance metadata fallback for {len(fallback_tickers)} {market} tickers...")
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(_fetch_yfinance_metadata, ticker): ticker for ticker in fallback_tickers}
            for future in as_completed(futures):
                ticker = futures[future]
                enriched = future.result()
                record = base_records[ticker]
                for key, value in enriched.items():
                    if value not in (None, "") and (record.get(key) in (None, "", ticker) or key in {"market_cap", "pe_ratio"}):
                        record[key] = value

    records = []
    for ticker in tickers:
        record = base_records[ticker]
        for key in ("name", "sector", "industry", "currency", "country"):
            record[key] = _clean_text(record.get(key))
        records.append(record)
    print(f"  Prepared metadata for {len(records)} {market} tickers")
    return pd.DataFrame(records)
