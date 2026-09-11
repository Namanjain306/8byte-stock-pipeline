# Dockerized Stock Market Data Pipeline (Airflow + PostgreSQL)

An Airflow-orchestrated pipeline that fetches daily stock price data from the
**Alpha Vantage** API on a schedule, parses it, and upserts it into a
PostgreSQL table — fully containerized with Docker Compose.

---

## Architecture

```
                 ┌─────────────────────────┐
   Alpha Vantage │   Airflow Scheduler      │
   Stock API  ◄──┤   (triggers DAG hourly)  │
        │        └────────────┬─────────────┘
        │                     │ runs
        ▼                     ▼
  JSON response       PythonOperator task
        │              (fetch_stock_data.py)
        │                     │
        │        parse + validate + handle errors
        │                     │
        └────────────►  Upsert (ON CONFLICT) ─────► PostgreSQL
                                                     (stock_prices table)
```

Two separate Postgres containers are used:
1. `postgres-airflow` — internal metadata DB Airflow needs to run itself.
2. `postgres-stock` — the "existing" application database that the
   assignment refers to; it holds the `stock_prices` table our pipeline
   updates. It's exposed on host port `5433` so you can connect to it
   directly with any SQL client.

---

## Folder structure

```
8byte-stock-pipeline/
├── docker-compose.yml       # Spins up Airflow + both Postgres DBs
├── Dockerfile                # Custom Airflow image (adds requests/psycopg2)
├── requirements.txt          # Python deps installed into the Airflow image
├── .env.example               # Template for secrets/config (copy to .env)
├── .gitignore
├── dags/
│   └── stock_pipeline_dag.py # Airflow DAG: schedules the pipeline
├── scripts/
│   └── fetch_stock_data.py   # Core logic: fetch → parse → upsert
└── init-db/
    └── init.sql               # Auto-creates the stock_prices table
```

---

## Prerequisites

- Docker + Docker Compose installed
- A free Alpha Vantage API key: https://www.alphavantage.co/support/#api-key

---

## How to run it (step by step)

**1. Get into the project folder**
```bash
cd 8byte-stock-pipeline
```

**2. Create your `.env` file from the example**
```bash
cp .env.example .env
```
Open `.env` and paste in your real `ALPHA_VANTAGE_API_KEY`. You can also
change `STOCK_SYMBOLS` to whichever tickers you want (comma-separated).

**3. Build and start everything with a single command**
```bash
docker compose up --build
```
This will:
- Build a custom Airflow image with `requests` and `psycopg2-binary` installed
- Start `postgres-airflow` (Airflow's own metadata DB)
- Start `postgres-stock` and auto-run `init-db/init.sql`, which creates the
  `stock_prices` table
- Run `airflow-init` once, to initialize Airflow's DB and create an admin user
- Start the Airflow **webserver** and **scheduler**

First run can take a couple of minutes while images build and Airflow
initializes — that's normal.

**4. Open the Airflow UI**
Go to **http://localhost:8080**
- Username: `admin`
- Password: `admin`

**5. Turn the DAG on**
In the DAG list, find `stock_market_pipeline` and flip its toggle to **ON**
(it's paused by default). It will now run automatically every hour
(`@hourly`). You can also click the ▶ "Trigger DAG" button to run it
immediately instead of waiting.

**6. Watch it run**
Click into the DAG → click the task `fetch_and_store_stock_data` → **Logs**.
You'll see the fetch/parse/upsert steps logged line by line, including any
warnings about missing fields or skipped rows.

**7. Check the data landed in Postgres**
```bash
docker exec -it postgres-stock psql -U stockuser -d stockdata -c "SELECT * FROM stock_prices ORDER BY trade_date DESC LIMIT 10;"
```
(Or connect with any SQL client to `localhost:5433`, db `stockdata`, using
the credentials from your `.env`.)

**8. Stop everything**
```bash
docker compose down
```
Add `-v` if you also want to wipe the database volumes:
```bash
docker compose down -v
```

---

## What happens internally (in plain terms)

1. **Scheduler wakes up** (every hour, per the DAG's `schedule_interval`) and
   triggers the `stock_market_pipeline` DAG.
2. The single task, `fetch_and_store_stock_data`, calls `run_pipeline()`
   from `scripts/fetch_stock_data.py`.
3. For **each symbol** in `STOCK_SYMBOLS`:
   - `fetch_stock_data()` calls Alpha Vantage's `TIME_SERIES_DAILY` endpoint
     using the `requests` library, with retries (up to 3 attempts) if the
     request times out, fails, or Alpha Vantage returns a rate-limit "Note".
   - `parse_time_series()` walks through the JSON's daily entries and pulls
     out open/high/low/close/volume for each date. Rows with missing or
     unparseable fields are **skipped and logged**, not allowed to crash
     the run.
   - `update_database()` upserts the parsed rows into `stock_prices` in a
     single transaction using `ON CONFLICT (symbol, trade_date) DO UPDATE`,
     so re-running the pipeline never creates duplicates — it just refreshes
     existing rows.
4. A failure on **one symbol** (bad ticker, API hiccup, etc.) is caught,
   logged, and doesn't stop the other symbols from being processed. The
   task only fails outright if **every** symbol fails, which is what tells
   Airflow to retry the whole task (per `default_args`: 2 retries, 5 min
   apart).

---

## Error handling & robustness (how each requirement is met)

| Requirement | How it's handled |
|---|---|
| Missing/rate-limited API response | Detected via `"Note"`/`"Information"`/`"Error Message"` keys in the JSON; retried with backoff, then skipped with a logged error |
| Network errors / timeouts | `try/except` around `requests.get`, retried up to 3 times |
| Missing fields in a data point | `_safe_float` / `_safe_int` return `None` instead of raising; rows with no usable price data are skipped |
| DB connection failure | Caught, logged, and re-raised so Airflow marks the task failed and retries later |
| Partial failure across symbols | Each symbol is isolated in its own `try/except`; pipeline only hard-fails if *all* symbols fail |
| Secrets (API key, DB password) | Never hard-coded — read from environment variables set via `.env` / `docker-compose.yml` |
| Duplicate data on re-run | `ON CONFLICT (symbol, trade_date) DO UPDATE` upsert, not plain INSERT |

---

## Scalability notes

- Add more tickers any time by editing `STOCK_SYMBOLS` in `.env` and
  restarting — no code changes needed.
- Airflow's `LocalExecutor` runs tasks in parallel processes; switching to
  `CeleryExecutor` with additional worker containers would let this scale
  to many DAGs/tasks concurrently without changing the pipeline logic.
- Because each symbol is processed independently, this pipeline could be
  refactored to fan out one Airflow task per symbol (using dynamic task
  mapping) if you want per-symbol retry/monitoring granularity.

---

## Notes

- Default schedule is `@hourly`. Change `SCHEDULE` in
  `dags/stock_pipeline_dag.py` to `@daily` if you'd rather match the
  assignment's "hourly or daily" wording with a daily cadence.
- Alpha Vantage's free tier is rate-limited (5 requests/minute, 25/day at
  time of writing) — if you add many symbols you may hit the "Note" rate
  limit message, which the pipeline already handles gracefully.
