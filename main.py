#!/usr/bin/env python3
# Alimenta_PostGre_Deribit.py

import os
import time
import logging
import requests
import psycopg2
from datetime import datetime
from typing import Optional, Dict, Any

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("Alimenta_PostGre_Deribit")

# Env vars para DB
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_URL = os.getenv("DB_URL")
DB_USER = os.getenv("DB_USER")

if not (DB_NAME and DB_USER and (DB_HOST or DB_URL) and DB_PASSWORD):
    logger.error("Faltam variáveis de ambiente de BD. Defina DB_HOST/DB_URL, DB_NAME, DB_USER, DB_PASSWORD.")
    raise SystemExit(1)

# Deribit base
DERIBIT_BASE = "https://www.deribit.com/api/v2"

# Tabela alvo
TABLE_NAME = "tb_deribit_info_ini"

# Criação da tabela simplificada (BTC + ETH)
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    btc_mark NUMERIC,
    btc_index NUMERIC,
    eth_mark NUMERIC,
    eth_index NUMERIC,
    funding_btc NUMERIC,
    funding_eth NUMERIC,
    open_interest_btc NUMERIC,
    open_interest_eth NUMERIC,
    v24h_btc NUMERIC,
    v24h_eth NUMERIC,
    dvol_btc NUMERIC,
    dvol_eth NUMERIC,
    upper_wick_btc NUMERIC,
    lower_wick_btc NUMERIC,
    upper_wick_eth NUMERIC,
    lower_wick_eth NUMERIC
);
"""

INSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (
    timestamp,
    btc_mark, btc_index,
    eth_mark, eth_index,
    funding_btc, funding_eth,
    open_interest_btc, open_interest_eth,
    v24h_btc, v24h_eth,
    dvol_btc, dvol_eth,
    upper_wick_btc, lower_wick_btc,
    upper_wick_eth, lower_wick_eth
) VALUES (
    %(timestamp)s,
    %(btc_mark)s, %(btc_index)s,
    %(eth_mark)s, %(eth_index)s,
    %(funding_btc)s, %(funding_eth)s,
    %(open_interest_btc)s, %(open_interest_eth)s,
    %(v24h_btc)s, %(v24h_eth)s,
    %(dvol_btc)s, %(dvol_eth)s,
    %(upper_wick_btc)s, %(lower_wick_btc)s,
    %(upper_wick_eth)s, %(lower_wick_eth)s
);
"""

# DB helpers
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

# Deribit request com retries
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

# Resolver instrument perpétuo disponível para a moeda
def resolve_perp_instrument(currency: str) -> Optional[str]:
    try:
        instruments = deribit_get("/public/get_instruments", params={"currency": currency})
        if not instruments or not isinstance(instruments, list):
            return None
        # procurar por kind == "perpetual" ou nome contendo "PERPETUAL" ou "PERP"
        for ins in instruments:
            name = ins.get("instrument_name", "")
            kind = (ins.get("kind") or "").lower()
            if kind == "perpetual" or "perpetual" in name.lower() or "perp" in name.lower():
                return name
        # fallback: retornar primeiro instrumento com kind == "future" ou primeiro da lista
        for ins in instruments:
            if (ins.get("kind") or "").lower() == "future":
                return ins.get("instrument_name")
        return instruments[0].get("instrument_name")
    except Exception as e:
        logger.debug("Erro ao resolver instrumento para %s: %s", currency, e)
        return None

# Obter summary por instrumento
def get_instrument_summary(instrument_name: str) -> Dict[str, Optional[float]]:
    summary = {"mark": None, "index": None, "funding": None, "open_interest": None, "v24h": None, "dvol": None}
    if not instrument_name:
        return summary
    # ticker
    try:
        ticker = deribit_get("/public/ticker", params={"instrument_name": instrument_name})
        if isinstance(ticker, dict):
            summary["mark"] = ticker.get("mark_price") or ticker.get("mark") or ticker.get("last")
            summary["index"] = ticker.get("index_price") or ticker.get("underlying_index")
            # funding: vários nomes possíveis
            summary["funding"] = ticker.get("funding_8h") or ticker.get("funding_rate") or ticker.get("current_funding")
    except Exception as e:
        logger.debug("Erro ticker %s: %s", instrument_name, e)
    # book summary
    try:
        book = deribit_get("/public/get_book_summary_by_instrument", params={"instrument_name": instrument_name})
        # book pode ser lista ou dict
        b = None
        if isinstance(book, list) and book:
            b = book[0]
        elif isinstance(book, dict):
            b = book
        if b:
            summary["open_interest"] = b.get("open_interest") or b.get("oi")
            # tentar stats.volume
            stats = b.get("stats") if isinstance(b.get("stats"), dict) else None
            if stats:
                summary["v24h"] = stats.get("volume") or stats.get("volume_usd")
            summary["v24h"] = summary["v24h"] or b.get("volume") or b.get("volume_24h")
            summary["dvol"] = b.get("dvol") or b.get("daily_volatility") or b.get("dv")
    except Exception as e:
        logger.debug("Erro book_summary %s: %s", instrument_name, e)
    # normalizar
    def to_float(v):
        try:
            return float(v) if v is not None else None
        except Exception:
            return None
    return {k: to_float(v) for k, v in summary.items()}

# Obter candle e calcular wicks (usa instrument_name)
def get_latest_candle_wicks(instrument_name: str, resolution: str = "1") -> Dict[str, Optional[float]]:
    result = {"upper_wick": None, "lower_wick": None}
    if not instrument_name:
        return result
    now_ms = int(datetime.utcnow().timestamp() * 1000)
    start_ms = now_ms - (10 * 60 * 1000)  # 10 minutos atrás
    params = {
        "instrument_name": instrument_name,
        "resolution": resolution,
        "start_timestamp": start_ms,
        "end_timestamp": now_ms
    }
    try:
        data = deribit_get("/public/get_tradingview_chart_data", params=params)
        if not data or not isinstance(data, dict):
            return result
        o = data.get("o") or []
        h = data.get("h") or []
        l = data.get("l") or []
        c = data.get("c") or []
        t = data.get("t") or []
        if not (o and h and l and c and t):
            return result
        last_idx = None
        for i in range(len(t) - 1, -1, -1):
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
        logger.debug("Erro candles %s: %s", instrument_name, e)
    return result

# Collect & store
def collect_and_store():
    timestamp = datetime.utcnow()

    # resolver instrumentos perpétuos
    btc_instr = resolve_perp_instrument("BTC")
    eth_instr = resolve_perp_instrument("ETH")
    logger.info("Resolved instruments: BTC=%s ETH=%s", btc_instr, eth_instr)

    # coletar summaries
    btc_summary = get_instrument_summary(btc_instr)
    eth_summary = get_instrument_summary(eth_instr)

    # coletar wicks
    btc_wicks = get_latest_candle_wicks(btc_instr, resolution="1")
    eth_wicks = get_latest_candle_wicks(eth_instr, resolution="1")

    payload = {
        "timestamp": timestamp,
        "btc_mark": btc_summary.get("mark"),
        "btc_index": btc_summary.get("index"),
        "eth_mark": eth_summary.get("mark"),
        "eth_index": eth_summary.get("index"),
        "funding_btc": btc_summary.get("funding"),
        "funding_eth": eth_summary.get("funding"),
        "open_interest_btc": btc_summary.get("open_interest"),
        "open_interest_eth": eth_summary.get("open_interest"),
        "v24h_btc": btc_summary.get("v24h"),
        "v24h_eth": eth_summary.get("v24h"),
        "dvol_btc": btc_summary.get("dvol"),
        "dvol_eth": eth_summary.get("dvol"),
        "upper_wick_btc": btc_wicks.get("upper_wick"),
        "lower_wick_btc": btc_wicks.get("lower_wick"),
        "upper_wick_eth": eth_wicks.get("upper_wick"),
        "lower_wick_eth": eth_wicks.get("lower_wick")
    }

    logger.info("Payload coletado: %s", {k: v for k, v in payload.items() if k != "timestamp"})

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
