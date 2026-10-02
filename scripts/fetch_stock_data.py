"""
fetch_stock_data.py
--------------------
Fetches daily stock market data from the Alpha Vantage API and upserts
it into the `stock_prices` table in PostgreSQL.

This module is imported by the Airflow DAG (dags/stock_pipeline_dag.py)
but can also be run standalone for local testing:

    python scripts/fetch_stock_data.py

All configuration (API key, DB credentials, symbols) comes from
environment variables so no secrets are hard-coded anywhere.
"""

import os
import sys
import logging
import time
from datetime import datetime

import requests
import psycopg2
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("stock_pipeline")

# ---------------------------------------------------------------------
# Configuration (all via environment variables — never hard-coded)
# ---------------------------------------------------------------------
ALPHA_VANTAGE_API_KEY = os.environ.get("ALPHA_VANTAGE_API_KEY")
STOCK_SYMBOLS = [
    s.strip().upper()
    for s in os.environ.get("STOCK_SYMBOLS", "IBM").split(",")
    if s.strip()
]

DB_HOST = os.environ.get("STOCK_DB_HOST", "postgres-stock")
DB_PORT = os.environ.get("STOCK_DB_PORT", "5432")
DB_NAME = os.environ.get("STOCK_DB_NAME", "stockdata")
DB_USER = os.environ.get("STOCK_DB_USER", "stockuser")
DB_PASSWORD = os.environ.get("STOCK_DB_PASSWORD", "stockpass")

ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"

# How many times to retry a failed HTTP call before giving up on a symbol
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5


# ---------------------------------------------------------------------
# 1. API interaction
# ---------------------------------------------------------------------
def fetch_stock_data(symbol: str) -> dict | None:
    """
    Calls Alpha Vantage's TIME_SERIES_DAILY endpoint for a given symbol.
    Returns the parsed JSON dict, or None if the fetch ultimately fails.
    """
    if not ALPHA_VANTAGE_API_KEY:
        logger.error("ALPHA_VANTAGE_API_KEY is not set. Cannot fetch %s.", symbol)
        return None

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY,
        "outputsize": "compact",  # last 100 data points
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(ALPHA_VANTAGE_URL, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()

            # Alpha Vantage returns HTTP 200 even for errors/rate limits,
            # so we must inspect the payload itself.
            if "Error Message" in data:
                logger.error("API error for %s: %s", symbol, data["Error Message"])
                return None

            if "Note" in data:
                # Typically a rate-limit notice
                logger.warning(
                    "Rate limit / note for %s (attempt %s/%s): %s",
                    symbol, attempt, MAX_RETRIES, data["Note"],
                )
                time.sleep(RETRY_BACKOFF_SECONDS)
                continue

            if "Information" in data:
                logger.warning(
                    "API informational message for %s: %s", symbol, data["Information"]
                )
                time.sleep(RETRY_BACKOFF_SECONDS)
                continue

            if "Time Series (Daily)" not in data:
                logger.error(
                    "Unexpected response shape for %s (missing time series). Keys: %s",
                    symbol, list(data.keys()),
                )
                return None

            return data

        except requests.exceptions.Timeout:
            logger.warning("Timeout fetching %s (attempt %s/%s)", symbol, attempt, MAX_RETRIES)
        except requests.exceptions.RequestException as exc:
            logger.warning(
                "Request error fetching %s (attempt %s/%s): %s",
                symbol, attempt, MAX_RETRIES, exc,
            )
        except ValueError as exc:
            # response.json() failed to parse
            logger.error("Could not parse JSON response for %s: %s", symbol, exc)
            return None

        time.sleep(RETRY_BACKOFF_SECONDS)

    logger.error("Giving up on %s after %s attempts.", symbol, MAX_RETRIES)
    return None


# ---------------------------------------------------------------------
# 2. Parse / extract relevant data points
# ---------------------------------------------------------------------
def parse_time_series(symbol: str, raw_data: dict) -> list[tuple]:
    """
    Extracts (symbol, date, open, high, low, close, volume) tuples from
    the raw Alpha Vantage JSON. Rows with missing/invalid fields are
    skipped (and logged) rather than crashing the whole pipeline.
    """
    rows = []
    time_series = raw_data.get("Time Series (Daily)", {})

    if not time_series:
        logger.warning("No time series data found for %s — skipping.", symbol)
        return rows

    for trade_date_str, values in time_series.items():
        try:
            trade_date = datetime.strptime(trade_date_str, "%Y-%m-%d").date()

            # Use .get(..., None) so a single missing field doesn't
            # blow up the whole row; NULLs are allowed in the DB.
            open_price = _safe_float(values.get("1. open"))
            high_price = _safe_float(values.get("2. high"))
            low_price = _safe_float(values.get("3. low"))
            close_price = _safe_float(values.get("4. close"))
            volume = _safe_int(values.get("5. volume"))

            if open_price is None and close_price is None:
                logger.warning(
                    "Skipping %s on %s: both open and close are missing.",
                    symbol, trade_date_str,
                )
                continue

            rows.append(
                (symbol, trade_date, open_price, high_price, low_price, close_price, volume)
            )
        except (ValueError, TypeError) as exc:
            logger.warning(
                "Skipping malformed row for %s on %s: %s", symbol, trade_date_str, exc
            )
            continue

    logger.info("Parsed %s valid rows for %s.", len(rows), symbol)
    return rows


def _safe_float(value):
    try:
        return float(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def _safe_int(value):
    try:
        return int(float(value)) if value is not None else None
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------
# 3. Database update (upsert)
# ---------------------------------------------------------------------
def get_db_connection():
    """Creates a new psycopg2 connection using env-var credentials."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=10,
    )


def update_database(rows: list[tuple]) -> int:
    """
    Upserts rows into stock_prices. On conflict (same symbol + date),
    updates the price fields. Returns the number of rows written.
    Wrapped in try/except so a DB hiccup fails loudly but doesn't
    corrupt partial state (single transaction, rollback on error).
    """
    if not rows:
        logger.info("No rows to write to the database.")
        return 0

    insert_query = """
        INSERT INTO stock_prices
            (symbol, trade_date, open_price, high_price, low_price, close_price, volume)
        VALUES %s
        ON CONFLICT (symbol, trade_date)
        DO UPDATE SET
            open_price  = EXCLUDED.open_price,
            high_price  = EXCLUDED.high_price,
            low_price   = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume      = EXCLUDED.volume,
            fetched_at  = NOW();
    """

    conn = None
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_query, rows)
        logger.info("Successfully upserted %s rows.", len(rows))
        return len(rows)
    except psycopg2.OperationalError as exc:
        logger.error("Could not connect to the database: %s", exc)
        raise
    except psycopg2.Error as exc:
        logger.error("Database error while upserting rows: %s", exc)
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


# ---------------------------------------------------------------------
# 4. Orchestration entry point (called by the Airflow task)
# ---------------------------------------------------------------------
def run_pipeline():
    """
    Runs the full fetch -> parse -> store flow for every configured
    symbol. A failure on one symbol does not stop the others — each
    is isolated and logged, and the function raises at the end if
    every single symbol failed (so Airflow marks the task as failed).
    """
    if not STOCK_SYMBOLS:
        raise ValueError("STOCK_SYMBOLS is empty — nothing to fetch.")

    total_written = 0
    successful_symbols = []
    failed_symbols = []

    for symbol in STOCK_SYMBOLS:
        logger.info("=== Processing symbol: %s ===", symbol)
        try:
            raw_data = fetch_stock_data(symbol)
            if raw_data is None:
                failed_symbols.append(symbol)
                continue

            rows = parse_time_series(symbol, raw_data)
            if not rows:
                failed_symbols.append(symbol)
                continue

            written = update_database(rows)
            total_written += written
            successful_symbols.append(symbol)

        except Exception as exc:  # noqa: BLE001 - isolate per-symbol failures
            logger.error("Unhandled error while processing %s: %s", symbol, exc)
            failed_symbols.append(symbol)
            continue

    logger.info(
        "Pipeline run complete. Success: %s | Failed: %s | Rows written: %s",
        successful_symbols, failed_symbols, total_written,
    )

    if not successful_symbols:
        raise RuntimeError(
            f"Pipeline failed for ALL symbols: {failed_symbols}. Check logs above."
        )

    return {
        "successful_symbols": successful_symbols,
        "failed_symbols": failed_symbols,
        "rows_written": total_written,
    }


if __name__ == "__main__":
    try:
        result = run_pipeline()
        logger.info("Result: %s", result)
    except Exception as exc:  # noqa: BLE001
        logger.error("Pipeline run failed: %s", exc)
        sys.exit(1)
