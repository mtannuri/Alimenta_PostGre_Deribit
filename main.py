#!/usr/bin/env python3
# Alimenta_PostGre_Deribit.py
# Único script para coletar métricas da Deribit e inserir em PostgreSQL
# Start Command (Render PredCripto): python Alimenta_PostGre_Deribit.py

import os
import time
import logging
import requests
import psycopg2
import pandas as pd

from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
from Alimenta_PostGre_Deribit import get_volatility_index


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

# Criação da tabela (colunas solicitadas)
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

# Escolha determinística do par representativo a partir de get_book_summary_by_currency
# Implementamos preferencia por quote_currency == "USDC" (escolha do usuário) e maior volume_usd
def choose_pair_from_currency(currency: str, preferred_quote: str = "USDC") -> Optional[Dict[str, Any]]:
    summaries = deribit_get("/public/get_book_summary_by_currency", params={"currency": currency})
    if not summaries or not isinstance(summaries, list):
        return None
    # Filtrar por base_currency == currency ou base_currency igual ao esperado
    candidates = [s for s in summaries if (s.get("base_currency") or "").upper() == currency.upper()]
    if not candidates:
        candidates = summaries
    # Prefer preferred_quote
    preferred = [c for c in candidates if (c.get("quote_currency") or "").upper() == preferred_quote.upper()]
    if preferred:
        # escolher o com maior volume_usd
        def vol_usd(x): 
            try: return float(x.get("volume_usd") or 0)
            except Exception: return 0
        chosen = max(preferred, key=vol_usd)
        logger.info("Pair chosen for %s by preferred_quote %s: %s", currency, preferred_quote, chosen.get("instrument_name"))
        return chosen
    # se não encontrou preferred, escolher por maior volume_usd entre candidatos
    def vol_usd(x):
        try: return float(x.get("volume_usd") or 0)
        except Exception: return 0
    chosen = max(candidates, key=vol_usd)
    logger.info("Pair chosen for %s by volume: %s", currency, chosen.get("instrument_name"))
    return chosen

# Resolver instrument perp para candle/funding/open_interest
from datetime import timezone, timedelta

def resolve_perp_instrument(currency: str, min_days_to_expire: int = 30) -> Optional[str]:
    try:
        instruments = deribit_get("/public/get_instruments", params={"currency": currency})
        if not instruments or not isinstance(instruments, list):
            return None

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        min_expiration_ms = now_ms + int(min_days_to_expire * 24 * 60 * 60 * 1000)

        def is_long_lived(ins):
            exp = ins.get("expiration_timestamp")
            if not exp:
                return True
            try:
                return int(exp) >= min_expiration_ms
            except Exception:
                return True

        perp_candidates = []
        for ins in instruments:
            name = (ins.get("instrument_name") or "").lower()
            settlement = (ins.get("settlement_period") or "").lower()
            kind = (ins.get("kind") or "").lower()
            if ("perp" in name or "perpetual" in name or settlement == "perpetual" or kind == "perpetual") and is_long_lived(ins):
                perp_candidates.append(ins)

        exact_name = f"{currency.upper()}-PERPETUAL"
        for ins in perp_candidates:
            if ins.get("instrument_name") == exact_name:
                return exact_name

        if perp_candidates:
            def score(ins):
                exp = ins.get("expiration_timestamp") or 0
                s = 0
                if (ins.get("settlement_period") or "").lower() == "perpetual":
                    s += 1000
                try:
                    s += int(exp) // (24*3600*1000)
                except Exception:
                    s += 0
                return s
            best = max(perp_candidates, key=score)
            return best.get("instrument_name")

        far_candidates = [ins for ins in instruments if is_long_lived(ins)]
        if far_candidates:
            def exp_value(ins):
                try:
                    return int(ins.get("expiration_timestamp") or 0)
                except Exception:
                    return 0
            best = max(far_candidates, key=exp_value)
            return best.get("instrument_name")

        if any(ins.get("instrument_name") == exact_name for ins in instruments):
            return exact_name
        return None
    except Exception as e:
        logger.debug("Erro ao resolver perp para %s: %s", currency, e)
        return None
# Extrair summary (mark/index/v24h/dvol/open_interest) a partir de objeto de book_summary (par)
def extract_from_book_summary_obj(obj: Dict[str, Any]) -> Dict[str, Optional[float]]:
    def to_float(v):
        try:
            return float(v) if v is not None else None
        except Exception:
            return None
    mark = obj.get("mark_price") or obj.get("mark") or obj.get("estimated_delivery_price")
    index = obj.get("estimated_delivery_price") or obj.get("index_price") or obj.get("last")
    v24h = obj.get("volume_usd") or obj.get("volume") or (obj.get("stats") or {}).get("volume")
    dvol = (obj.get("dvol") or (obj.get("stats") or {}).get("dvol") or obj.get("daily_volatility"))
    return {
        "mark": to_float(mark),
        "index": to_float(index),
        "v24h": to_float(v24h),
        "dvol": to_float(dvol)
    }

# Obter summary por instrument_name (ticker + book summary) para funding/open_interest/dvol
def get_instrument_metrics(instrument_name: str) -> Dict[str, Optional[float]]:
    result = {"funding": None, "open_interest": None, "v24h": None, "dvol": None, "mark_from_ticker": None, "index_from_ticker": None}
    if not instrument_name:
        return result
    try:
        ticker = deribit_get("/public/ticker", params={"instrument_name": instrument_name})
        if isinstance(ticker, dict):
            # tentativas de campos possíveis
            result["mark_from_ticker"] = ticker.get("mark_price") or ticker.get("mark") or ticker.get("last")
            result["index_from_ticker"] = ticker.get("index_price") or ticker.get("underlying_index") or ticker.get("last")
            result["funding"] = ticker.get("funding_8h") or ticker.get("funding_rate") or ticker.get("current_funding") or ticker.get("estimated_funding_rate")
    except Exception as e:
        logger.debug("Erro ticker %s: %s", instrument_name, e)
    try:
        book = deribit_get("/public/get_book_summary_by_instrument", params={"instrument_name": instrument_name})
        b = None
        if isinstance(book, list) and book:
            b = book[0]
        elif isinstance(book, dict):
            b = book
        if b:
            result["open_interest"] = b.get("open_interest") or b.get("oi")
            stats = b.get("stats") if isinstance(b.get("stats"), dict) else None
            if stats:
                result["v24h"] = stats.get("volume") or stats.get("volume_usd")
            result["v24h"] = result["v24h"] or b.get("volume") or b.get("volume_24h")
            result["dvol"] = b.get("dvol") or b.get("daily_volatility") or (stats and stats.get("dvol"))
    except Exception as e:
        logger.debug("Erro book_summary_by_instrument %s: %s", instrument_name, e)
    # normalizar floats
    def to_float(v):
        try:
            return float(v) if v is not None else None
        except Exception:
            return None
    return {k: to_float(v) for k, v in result.items()}

# Obter candles e calcular wicks (resolução definida como 15)
CANDLE_RESOLUTION = "15"

def get_latest_candle_wicks(instrument_name: str, resolution: str = CANDLE_RESOLUTION) -> Dict[str, Optional[float]]:
    return {"upper_wick": 0.0, "lower_wick": 0.0}


# Função principal de coleta e persistência
def collect_and_store():
    timestamp = datetime.utcnow()

    # pares representativos (escolha do usuário: ambos em USDC)
    btc_pair = choose_pair_from_currency("BTC", preferred_quote="USDC")
    eth_pair = choose_pair_from_currency("ETH", preferred_quote="USDC")

    # extrair mark/index/v24h/dvol a partir do par escolhido
    btc_from_pair = extract_from_book_summary_obj(btc_pair) if btc_pair else {"mark": None, "index": None, "v24h": None, "dvol": None}
    eth_from_pair = extract_from_book_summary_obj(eth_pair) if eth_pair else {"mark": None, "index": None, "v24h": None, "dvol": None}

    # resolver perps para funding/oi/candles
    btc_perp = resolve_perp_instrument("BTC") or "BTC-PERPETUAL"
    eth_perp = resolve_perp_instrument("ETH") or "ETH-PERPETUAL"
    logger.info("Perp instruments resolved: BTC=%s ETH=%s", btc_perp, eth_perp)

    # coletar métricas adicionais por perp
    btc_metrics = get_instrument_metrics(btc_perp)
    eth_metrics = get_instrument_metrics(eth_perp)

    # coletar wicks com resolução definida
    btc_wicks = get_latest_candle_wicks(btc_perp)
    eth_wicks = get_latest_candle_wicks(eth_perp)

    # montar payload (escolhas de prioridade: prefer book_summary_by_currency para mark/index; ticker fallback)
    payload = {
        "timestamp": timestamp,
        "btc_mark": btc_from_pair.get("mark") or btc_metrics.get("mark_from_ticker"),
        "btc_index": btc_from_pair.get("index") or btc_metrics.get("index_from_ticker"),
        "eth_mark": eth_from_pair.get("mark") or eth_metrics.get("mark_from_ticker"),
        "eth_index": eth_from_pair.get("index") or eth_metrics.get("index_from_ticker"),
        "funding_btc": btc_metrics.get("funding"),
        "funding_eth": eth_metrics.get("funding"),
        "open_interest_btc": btc_metrics.get("open_interest"),
        "open_interest_eth": eth_metrics.get("open_interest"),
        "v24h_btc": btc_from_pair.get("v24h") or btc_metrics.get("v24h"),
        "v24h_eth": eth_from_pair.get("v24h") or eth_metrics.get("v24h"),
        "dvol_btc": btc_from_pair.get("dvol") or btc_metrics.get("dvol"),
        "dvol_eth": eth_from_pair.get("dvol") or eth_metrics.get("dvol"),
        "upper_wick_btc": btc_wicks.get("upper_wick"),
        "lower_wick_btc": btc_wicks.get("lower_wick"),
        "upper_wick_eth": eth_wicks.get("upper_wick"),
        "lower_wick_eth": eth_wicks.get("lower_wick")
    }

    logger.info("Payload coletado (excluindo timestamp): %s", {k: v for k, v in payload.items() if k != "timestamp"})

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
