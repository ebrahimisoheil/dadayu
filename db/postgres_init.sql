CREATE USER dadayu WITH PASSWORD 'dadayu';
CREATE DATABASE dadayu OWNER dadayu;
CREATE DATABASE metabase;

\connect dadayu

CREATE SCHEMA IF NOT EXISTS dadayu AUTHORIZATION dadayu;
GRANT USAGE, CREATE ON SCHEMA dadayu TO dadayu;
ALTER DATABASE dadayu SET search_path TO dadayu;
SET ROLE dadayu;
SET search_path TO dadayu;

CREATE TABLE IF NOT EXISTS prices_daily (
    ticker text NOT NULL,
    market text NOT NULL,
    date date NOT NULL,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume bigint NOT NULL DEFAULT 0,
    ingested_at timestamp NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (ticker, market, date)
);

CREATE TABLE IF NOT EXISTS index_prices_daily (
    ticker text NOT NULL,
    market text NOT NULL DEFAULT 'index',
    date date NOT NULL,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume bigint NOT NULL DEFAULT 0,
    ingested_at timestamp NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (ticker, market, date)
);

CREATE TABLE IF NOT EXISTS tickers (
    ticker text NOT NULL,
    market text NOT NULL,
    name text NOT NULL DEFAULT '',
    sector text NOT NULL DEFAULT '',
    industry text NOT NULL DEFAULT '',
    currency text NOT NULL DEFAULT '',
    country text NOT NULL DEFAULT '',
    market_cap double precision,
    pe_ratio double precision,
    fetched_at timestamp NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (ticker, market)
);

CREATE TABLE IF NOT EXISTS macro_prices_daily (
    ticker      text NOT NULL,
    market      text NOT NULL DEFAULT 'macro',
    date        date NOT NULL,
    open        double precision,
    high        double precision,
    low         double precision,
    close       double precision,
    volume      bigint NOT NULL DEFAULT 0,
    ingested_at timestamp NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (ticker, market, date)
);

RESET ROLE;
