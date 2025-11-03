#!/usr/bin/env python3
# Alimenta_PostGre_Deribit.py
# Executar: python Alimenta_PostGre_Deribit.py

import os
import time
import json
import logging
import requests
import psycopg2
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

# --- Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("Alimenta_PostGre_Deribit")

# --- Env vars para DB
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_URL = os.getenv("DB_URL")
DB_USER = os.getenv("DB_USER")

if not (DB_NAME and DB_USER and (DB_HOST or DB_URL) and DB_PASSWORD):
    logger.error("Faltam variáveis de ambiente de BD. Defina DB_HOST/DB_URL, DB_NAME, DB_USER, DB_PASSWORD.")
    raise SystemExit(1)

# --- Deribit API base
DERIBIT_BASE = "https://www.deribit.com/api/v2"

# --- Tabela alvo
TABLE_NAME = "tb_deribit_info_ini"

# --- Criação da tabela (adicionados campos de candle upper/lower wicks)
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    btc_mark NUMERIC,
    btc_index NUMERIC,
    eth_mark NUMERIC,
    eth_index NUMERIC,
    sol_mark NUMERIC,
    sol_index NUMERIC,
    funding_btc NUMERIC,
    funding_eth NUMERIC,
    funding_sol NUMERIC,
    open_interest_btc NUMERIC,
    open_interest_eth NUMERIC,
    open_interest_sol NUMERIC,
    v24h_btc NUMERIC,
    v24h_eth NUMERIC,
    v24h_sol NUMERIC,
    dvol_btc NUMERIC,
    dvol_eth NUMERIC,
    upper_wick_btc NUMERIC,
    lower_wick_btc NUMERIC,
    upper_wick_eth NUMERIC,
    lower_wick_eth NUMERIC,
    upper_wick_sol NUMERIC,
    lower_wick_sol NUMERIC
);
"""

INSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (
    timestamp,
    btc_mark, btc_index,
    eth_mark, eth_index,
    sol_mark, sol_index,
    funding_btc, funding_eth, funding_sol,
    open_interest_btc, open_interest_eth, open_interest_sol,
    v24h_btc, v24h_eth, v24h_sol,
    dvol_btc, dvol_eth,
    upper_wick_btc, lower_wick_btc,
    upper_wick_eth, lower_wick_eth,
    upper_wick_sol, lower_wick_sol
) VALUES (
    %(timestamp)s,
    %(btc_mark)s, %(btc_index)s,
    %(eth_mark)s, %(eth_index)s,
    %(sol_mark)s, %(sol_index)s,
    %(funding_btc)s, %(funding_eth)s, %(funding_sol)s,
    %(open_interest_btc)s, %(open_interest_eth)s, %(open_interest_sol)s,
    %(v24h_btc)s, %(v24h_eth)s, %(v24h_sol)s,
    %(dvol_btc)s, %(dvol_eth)s,
    %(upper_wick_btc)s, %(lower_wick_btc)s,
    %(upper_wick_eth)s, %(lower_wick_eth)s,
    %(upper_wick_sol)s, %(lower_wick_sol)s
);
"""

# --- DB helpers
def get_db_connection():
    if DB_URL:
        return psycopg2.connect(DB_URL)
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

def ensure_table_exists(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    logger.debug("Tabela verificada/criada.")

# --- Deribit request with retries
def deribit_get(path: str, params: Optional[Dict[str, Any]] = None, retries: int = 3, backoff: float = 1.0) -> Any:
    url = f"{DERIBIT_BASE}{path}"
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and "result" in data:
                return data["result"]
            return data
        except Exception as exc:
            logger.warning("Erro Deribit %s tentativa %d/%d: %s", url, attempt, retries, exc)
            if attempt < retries:
                time.sleep(backoff * attempt)
            else:
                logger.error("Falha ao acessar Deribit: %s", exc)
                raise

# --- Collect ticker/summary per instrument (mark, index, funding, open_interest, volume, dvol)
def get_instrument_summary(instrument_name: str) -> Dict[str, Optional[float]]:
    # Usaremos endpoint /public/ticker para mark_price e index_price e /public/get_book_summary_by_instrument para open interest/volume possivelmente
    summary: Dict[str, Optional[float]] = {
        "mark": None,
        "index": None,
        "funding": None,
        "open_interest": None,
        "v24h": None,
        "dvol": None
    }
    try:
        # ticker fornece mark_price e index_price, and maybe funding rate fields
        ticker = deribit_get("/public/ticker", params={"instrument_name": instrument_name})
        if isinstance(ticker, dict):
            summary["mark"] = ticker.get("mark_price") or ticker.get("mark")
            summary["index"] = ticker.get("index_price") or ticker.get("underlying_index")
            # funding_rate may be in ticker as "funding_8h" or "current_funding"
            summary["funding"] = ticker.get("funding_8h") or ticker.get("funding_rate") or ticker.get("current_funding")
    except Exception as e:
        logger.debug("Erro ao obter ticker para %s: %s", instrument_name, e)

    try:
        # book summary by instrument can include open_interest and volume
        book = deribit_get("/public/get_book_summary_by_instrument", params={"instrument_name": instrument_name})
        # book may return a dict with "book_summary" or list; normalize
        summaries = []
        if isinstance(book, dict) and "book_summary" in book:
            summaries = book["book_summary"]
        elif isinstance(book, list):
            summaries = book
        elif isinstance(book, dict):
            summaries = [book]
        if summaries:
            b = summaries[0]
            summary["open_interest"] = b.get("open_interest") or b.get("oi")
            # Try several places for volume
            if "stats" in b and isinstance(b["stats"], dict):
                summary["v24h"] = b["stats"].get("volume") or b["stats"].get("volume_usd")
            summary["v24h"] = summary["v24h"] or b.get("volume") or b.get("volume_24h")
            # DVOL may not be present; try common keys
            summary["dvol"] = b.get("dvol") or b.get("dv") or b.get("daily_volatility")
    except Exception as e:
        logger.debug("Erro ao obter book_summary para %s: %s", instrument_name, e)

    # normalize floats
    def to_float(v):
        try:
            return float(v) if v is not None else None
        except Exception:
            return None

    return {k: to_float(v) for k, v in summary.items()}

# --- Get latest candle using tradingview endpoint and compute wicks
def get_latest_candle_wicks(instrument_name: str, resolution: str = "1") -> Dict[str, Optional[float]]:
    # Deribit public endpoint: /public/get_tradingview_chart_data
    # We'll request a small window (last 5 candles) and pick the last non-empty candle
    now_ms = int(datetime.utcnow().timestamp() * 1000)
    start_ms = now_ms - (5 * 60 * 1000)  # 5 minutes back
    params = {
        "symbol": instrument_name,
        "resolution": resolution,
        "start_timestamp": start_ms,
        "end_timestamp": now_ms
    }
    result = {
        "upper_wick": None,
        "lower_wick": None
    }
    try:
        data = deribit_get("/public/get_tradingview_chart_data", params=params)
        # Expected keys: t (timestamps), o, h, l, c, v
        if not data or not isinstance(data, dict):
            return result
        o = data.get("o") or []
        h = data.get("h") or []
        l = data.get("l") or []
        c = data.get("c") or []
        if not (o and h and l and c):
            return result
        # take last index where values exist
        last_idx = None
        for i in range(len(t := data.get("t", [])) - 1, -1, -1):
            try:
                if o[i] is not None and h[i] is not None and l[i] is not None and c[i] is not None:
                    last_idx = i
                    break
            except Exception:
                continue
        if last_idx is None:
            return result
        open_p = float(o[last_idx])
        high_p = float(h[last_idx])
        low_p = float(l[last_idx])
        close_p = float(c[last_idx])
        upper_wick = high_p - max(open_p, close_p)
        lower_wick = min(open_p, close_p) - low_p
        result["upper_wick"] = upper_wick
        result["lower_wick"] = lower_wick
    except Exception as e:
        logger.debug("Erro ao obter candles para %s: %s", instrument_name, e)
    return result

# --- Main collect and store
def collect_and_store():
    timestamp = datetime.utcnow()

    # instrument names
    BTC_INSTR = "BTC-PERPETUAL"
    ETH_INSTR = "ETH-PERPETUAL"
    SOL_INSTR = "SOL-PERPETUAL"

    # summaries
    btc_summary = get_instrument_summary(BTC_INSTR)
    eth_summary = get_instrument_summary(ETH_INSTR)
    sol_summary = get_instrument_summary(SOL_INSTR)

    # candles wicks (resolution 1 minute)
    btc_wicks = get_latest_candle_wicks(BTC_INSTR, resolution="1")
    eth_wicks = get_latest_candle_wicks(ETH_INSTR, resolution="1")
    sol_wicks = get_latest_candle_wicks(SOL_INSTR, resolution="1")

    payload = {
        "timestamp": timestamp,
        "btc_mark": btc_summary.get("mark"),
        "btc_index": btc_summary.get("index"),
        "eth_mark": eth_summary.get("mark"),
        "eth_index": eth_summary.get("index"),
        "sol_mark": sol_summary.get("mark"),
        "sol_index": sol_summary.get("index"),
        "funding_btc": btc_summary.get("funding"),
        "funding_eth": eth_summary.get("funding"),
        "funding_sol": sol_summary.get("funding"),
        "open_interest_btc": btc_summary.get("open_interest"),
        "open_interest_eth": eth_summary.get("open_interest"),
        "open_interest_sol": sol_summary.get("open_interest"),
        "v24h_btc": btc_summary.get("v24h"),
        "v24h_eth": eth_summary.get("v24h"),
        "v24h_sol": sol_summary.get("v24h"),
        "dvol_btc": btc_summary.get("dvol"),
        "dvol_eth": eth_summary.get("dvol"),
        "upper_wick_btc": btc_wicks.get("upper_wick"),
        "lower_wick_btc": btc_wicks.get("lower_wick"),
        "upper_wick_eth": eth_wicks.get("upper_wick"),
        "lower_wick_eth": eth_wicks.get("lower_wick"),
        "upper_wick_sol": sol_wicks.get("upper_wick"),
        "lower_wick_sol": sol_wicks.get("lower_wick")
    }

    logger.info("Payload coletado: %s", json.dumps({k: v for k, v in payload.items() if k != "timestamp"}, default=str))

    conn = None
    try:
        conn = get_db_connection()
        ensure_table_exists(conn)
        with conn.cursor() as cur:
            cur.execute(INSERT_SQL, payload)
        conn.commit()
        logger.info("Inserido no banco com timestamp %s", timestamp.isoformat())
    except Exception as e:
        logger.exception("Erro ao persistir dados: %s", e)
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def main():
    try:
        collect_and_store()
    except Exception as e:
        logger.error("Execução finalizada com erro: %s", e)
        raise

if __name__ == "__main__":
    main()
