-- Hourly OHLCV prices for all markets
CREATE TABLE IF NOT EXISTS prices_hourly
(
    ticker      String,
    market      LowCardinality(String),   -- germany | us | india
    datetime    DateTime,
    open        Float64,
    high        Float64,
    low         Float64,
    close       Float64,
    volume      UInt64,
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (market, ticker, datetime)
PARTITION BY toYYYYMM(datetime);


-- 4-hour OHLCV prices for all markets
CREATE TABLE IF NOT EXISTS prices_4h
(
    ticker      String,
    market      LowCardinality(String),
    datetime    DateTime,
    open        Float64,
    high        Float64,
    low         Float64,
    close       Float64,
    volume      UInt64,
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (market, ticker, datetime)
PARTITION BY toYYYYMM(datetime);


-- Daily OHLCV prices for all markets
CREATE TABLE IF NOT EXISTS prices_daily
(
    ticker      String,
    market      LowCardinality(String),
    date        Date,
    open        Float64,
    high        Float64,
    low         Float64,
    close       Float64,
    volume      UInt64,
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (market, ticker, date)
PARTITION BY toYYYYMM(date);


-- Equity metadata — append-only, ReplacingMergeTree deduplicates by fetched_at
CREATE TABLE IF NOT EXISTS tickers
(
    ticker      String,
    market      LowCardinality(String),
    name        String,
    sector      String,
    industry    String,
    currency    LowCardinality(String),
    country     String,
    market_cap  Nullable(Float64),
    pe_ratio    Nullable(Float64),
    fetched_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (market, ticker)
PARTITION BY market;


-- Crypto 1h OHLCV
CREATE TABLE IF NOT EXISTS crypto_prices_hourly
(
    ticker      String,
    market      LowCardinality(String) DEFAULT 'crypto',
    datetime    DateTime,
    open        Float64,
    high        Float64,
    low         Float64,
    close       Float64,
    volume      UInt64,
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (ticker, datetime)
PARTITION BY toYYYYMM(datetime);


-- Crypto 4h OHLCV
CREATE TABLE IF NOT EXISTS crypto_prices_4h
(
    ticker      String,
    market      LowCardinality(String) DEFAULT 'crypto',
    datetime    DateTime,
    open        Float64,
    high        Float64,
    low         Float64,
    close       Float64,
    volume      UInt64,
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (ticker, datetime)
PARTITION BY toYYYYMM(datetime);


-- Crypto daily OHLCV
CREATE TABLE IF NOT EXISTS crypto_prices_daily
(
    ticker      String,
    market      LowCardinality(String) DEFAULT 'crypto',
    date        Date,
    open        Float64,
    high        Float64,
    low         Float64,
    close       Float64,
    volume      UInt64,
    ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (ticker, date)
PARTITION BY toYYYYMM(date);


-- Crypto metadata from CoinGecko — ReplacingMergeTree deduplicates by fetched_at
CREATE TABLE IF NOT EXISTS crypto_metadata
(
    coin_id     String,
    symbol      String,
    name        String,
    rank        UInt32,
    market_cap  Nullable(Float64),
    category    String,
    chain       String,
    fetched_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY coin_id
PARTITION BY tuple();
