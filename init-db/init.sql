-- This file runs automatically the FIRST time the postgres-stock
-- container starts (Postgres docker images run everything in
-- /docker-entrypoint-initdb.d on first boot).

CREATE TABLE IF NOT EXISTS stock_prices (
    id             SERIAL PRIMARY KEY,
    symbol         VARCHAR(10)     NOT NULL,
    trade_date     DATE            NOT NULL,
    open_price     NUMERIC(14, 4),
    high_price     NUMERIC(14, 4),
    low_price      NUMERIC(14, 4),
    close_price    NUMERIC(14, 4),
    volume         BIGINT,
    fetched_at     TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_symbol_date UNIQUE (symbol, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol ON stock_prices (symbol);
CREATE INDEX IF NOT EXISTS idx_stock_prices_trade_date ON stock_prices (trade_date);
