
import os
import psycopg2
import requests
import time
import pandas as pd
import numpy as np
from psycopg2 import sql
from dotenv import load_dotenv
from datetime import datetime

# Carrega variáveis de ambiente
load_dotenv()

# Conexão ao banco
def conectar_postgres():
    db_url = os.getenv("DB_URL")
    if db_url:
        return psycopg2.connect(db_url)
    else:
        return psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )

# Criação da tabela se não existir
def criar_tabela_deribit(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tb_deribit_info_ini (
                timestamp TIMESTAMP PRIMARY KEY,
                BTC_Mark FLOAT,
                BTC_Index FLOAT,
                ETH_Mark FLOAT,
                ETH_Index FLOAT,
                SOL_Mark FLOAT,
                SOL_Index FLOAT,
                funding_BTC FLOAT,
                funding_ETH FLOAT,
                funding_SOL FLOAT,
                open_interest_BTC FLOAT,
                open_interest_ETH FLOAT,
                open_interest_SOL FLOAT,
                funding_BTC_PremiumRate FLOAT,
                funding_ETH_PremiumRate FLOAT,
                funding_SOL_PremiumRate FLOAT,
                v24h_BTC FLOAT,
                v24h_ETH FLOAT,
                v24h_SOL FLOAT,
                DeltaVol_24h_BTC FLOAT,
                DeltaVol_24h_ETH FLOAT,
                DeltaVol_24h_SOL FLOAT,
                MA_DeltaVol_24h_BTC FLOAT,
                MA_DeltaVol_24h_ETH FLOAT,
                MA_DeltaVol_24h_SOL FLOAT,
                Zscore_DeltaVol_24h_BTC FLOAT,
                Zscore_DeltaVol_24h_ETH FLOAT,
                Zscore_DeltaVol_24h_SOL FLOAT,
                Delta_open_interes_BTC FLOAT,
                Delta_open_interes_ETH FLOAT,
                Delta_open_interes_SOL FLOAT,
                MA_OI_BTC_12 FLOAT,
                MA_OI_ETH_12 FLOAT,
                MA_OI_SOL_12 FLOAT,
                MA_OI_BTC_48 FLOAT,
                MA_OI_ETH_48 FLOAT,
                MA_OI_SOL_48 FLOAT,
                STD_OI_BTC_12 FLOAT,
                STD_OI_ETH_12 FLOAT,
                STD_OI_SOL_12 FLOAT,
                STD_OI_BTC_48 FLOAT,
                STD_OI_ETH_48 FLOAT,
                STD_OI_SOL_48 FLOAT,
                Zscore_OI_BTC_12 FLOAT,
                Zscore_OI_ETH_12 FLOAT,
                Zscore_OI_SOL_12 FLOAT,
                Zscore_OI_BTC_48 FLOAT,
                Zscore_OI_ETH_48 FLOAT,
                Zscore_OI_SOL_48 FLOAT,
                Upper_Wick_BTC FLOAT,
                Lower_Wick_BTC FLOAT,
                Upper_Wick_ETH FLOAT,
                Lower_Wick_ETH FLOAT,
                Upper_Wick_SOL FLOAT,
                Lower_Wick_SOL FLOAT,
                DVOL_BTC FLOAT,
                DVOL_ETH FLOAT
            );
        """)
        conn.commit()

# Função auxiliar para decodificar resposta da API
def get_result_safe(response, descricao=""):
    try:
        data = response.json()
    except Exception as e:
        print(f"Erro ao decodificar JSON {descricao}: {e}")
        return None
    if isinstance(data, dict) and "result" in data:
        return data["result"]
    print(f"Resposta inesperada {descricao}: {data}")
    return None

# Coleta dados da API Deribit
def coletar_dados_deribit():
    ativos = ["btc", "eth", "sol"]
    dados = {}
    timestamp = int(time.time() * 1000)

    for ativo in ativos:
        r_book = requests.get(f"https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency={ativo.upper()}")
        book_result = get_result_safe(r_book, f"book_summary {ativo}")
        if not book_result or not isinstance(book_result, list) or len(book_result) == 0:
            continue
        book_data = book_result[0]

        mark_price = book_data.get("mark_price")
        volume_24h = book_data.get("volume")
        open_interest = book_data.get("open_interest")
        funding_rate = book_data.get("funding_8h") or book_data.get("funding_8h_rate") or book_data.get("funding_rate")

        r_index = requests.get(f"https://www.deribit.com/api/v2/public/get_index_price?index_name={ativo}_usd")
        index_result = get_result_safe(r_index, f"index_price {ativo}")
        index_price = index_result.get("index_price") if isinstance(index_result, dict) else None

        premium_rate = None
        if isinstance(mark_price, (int, float)) and isinstance(index_price, (int, float)) and index_price != 0:
            premium_rate = (mark_price - index_price) / index_price

        dvol = None
        if ativo in ["btc", "eth"]:
            start_ts = timestamp - (15 * 60 * 1000)
            end_ts = timestamp
            r_dvol = requests.get(
                f"https://www.deribit.com/api/v2/public/get_volatility_index_data?currency={ativo}&start_timestamp={start_ts}&end_timestamp={end_ts}&resolution=60"
            )
            dvol_result = get_result_safe(r_dvol, f"dvol {ativo}")
            if isinstance(dvol_result, dict):
                dvol = dvol_result.get("volatility") or dvol_result.get("value")

        r_candle = requests.get(
            f"https://www.deribit.com/api/v2/public/get_tradingview_chart_data?instrument_name={ativo.upper()}-PERPETUAL&start_timestamp={timestamp - 15*60*1000}&end_timestamp={timestamp}&resolution=15"
        )
        candle_result = get_result_safe(r_candle, f"candlestick {ativo}")
        upper_wick = lower_wick = None
        if isinstance(candle_result, dict):
            ticks = candle_result.get("ticks") or []
            if isinstance(ticks, list) and len(ticks) > 0:
                ultima = ticks[-1]
                o = ultima.get("open"); h = ultima.get("high"); l = ultima.get("low"); c = ultima.get("close")
                if all(isinstance(x, (int, float)) for x in [o, h, l, c]):
                    upper_wick = h - max(o, c)
                    lower_wick = min(o, c) - l

        dados[ativo] = {
            "mark": mark_price,
            "index": index_price,
            "funding": funding_rate,
            "premium_rate": premium_rate,
            "volume_24h": volume_24h,
            "open_interest": open_interest,
            "dvol": dvol,
            "upper_wick": upper_wick,
            "lower_wick": lower_wick
        }

    return dados

# Normaliza valores para inserção
def normalize_value(v):
    if v is None:
        return None
    if isinstance(v, (np.generic,)):
        return v.item()
    if isinstance(v, (np.ndarray,)):
        return v.tolist()
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if isinstance(v, pd.Series):
        return v.tolist()
    if isinstance(v, (int, float, str, bool)):
        return v
    try:
        return float(v)
    except Exception:
        return str(v)

# Função principal de atualização
def atualizar_deribit():
    conn = None
    try:
        conn = conectar_postgres()
        criar_tabela_deribit(conn)

        dados_api = coletar_dados_deribit()
        timestamp = datetime.now()

        if not dados_api:
            print(f"[{timestamp}] Nenhum ativo coletado da Deribit — pulando insert.")
            return

        df_antigos = pd.read_sql("SELECT * FROM tb_deribit_info_ini ORDER BY timestamp DESC LIMIT 100", conn)

        def calcular_delta_vol(ativo):
            vol_atual = dados_api.get(ativo, {}).get("volume_24h")
            vol_anterior = df_antigos[f"v24h_{ativo}"].iloc[0] if not df_antigos.empty and f"v24h_{ativo}" in df_antigos.columns else None
            return vol_atual - vol_anterior if isinstance(vol_atual, (int, float)) and isinstance(vol_anterior, (int, float)) else None

        def calcular_ma(serie, n):
            return serie.head(n).mean() if len(serie) >= n else None

        def calcular_std(serie, n):
            return serie.head(n).std() if len(serie) >= n else None

        def calcular_zscore(valor, serie, n):
            media = calcular_ma(serie, n)
            desvio = calcular_std(serie, n)
            return (valor - media) / desvio if isinstance(valor, (int, float)) and media and desvio else None

        linha = {"timestamp": timestamp}

        for ativo in ["btc", "eth", "sol"]:
            if ativo not in dados_api:
                continue

            dados = dados_api[ativo]
            mark = dados.get("mark")
            index = dados.get("index")
            funding = dados.get("funding")
            oi = dados.get("open_interest")
            premium = dados.get("premium_rate")
            v24h = dados.get("volume_24h")
            upper_wick = dados.get("upper_wick")
            lower_wick = dados.get("lower_wick")
            dvol = dados.get("dvol") if ativo in ["btc", "eth"] else None

            if mark is not None: linha[f"{ativo.upper()}_Mark"] = mark
            if index is not None: linha[f"{ativo.upper()}_Index"] = index
            if funding is not None: linha[f"funding_{ativo.upper()}"] = funding
            if oi is not None: linha[f"open_interest_{ativo.upper()}"] = oi
            if premium is not None: linha[f"funding_{ativo.upper()}_PremiumRate"] = premium
            if v24h is not None: linha[f"v24h_{ativo.upper()}"] = v24h
            delta_vol = calcular_delta_vol(ativo)
            if delta_vol is not None: linha[f"DeltaVol_24h_{ativo.upper()}"] = delta_vol
            if upper_wick is not None: linha[f"Upper_Wick_{ativo.upper()}"] = upper_wick
            if lower_wick is not None: linha[f"Lower_Wick_{ativo.upper()}"] = lower_wick
            if dvol is not None: linha[f"DVOL_{ativo.upper()}"] = dvol

            if not df_antigos.empty:
                col_delta = f"DeltaVol_24h_{ativo.upper()}"
                if col_delta in df_antigos.columns:
                    serie_delta_vol = df_antigos[col_delta]
                    ma = calcular_ma(serie_delta_vol, 12)
                    if ma is not None: linha[f"MA_DeltaVol_24h_{ativo.upper()}"] = ma
                    z = calcular_zscore(delta_vol, serie_delta_vol, 12) if delta_vol is not None else None
                    if z is not None: linha[f"Zscore_DeltaVol_24h_{ativo.upper()}"] = z

                col_oi = f"open_interest_{ativo.upper()}"
                if col_oi in df_antigos.columns and oi is not None:
                    serie_oi = df_antigos[col_oi]
                    linha[f"Delta_open_interes_{ativo.upper()}"] = oi - serie_oi.iloc[0] if isinstance(serie_oi.iloc[0], (int, float)) else None
                    ma12 = calcular_ma(serie_oi, 12); ma48 = calcular_ma(serie_oi, 48)
                    if ma12 is not None: linha[f"MA_OI_{ativo.upper()}_12"] = ma12
                    if ma48 is not None: linha[f"MA_OI_{ativo.upper()}_48"] = ma48
                    std12 = calcular_std(serie_oi, 12); std48 = calcular_std(serie_oi, 48)
                    if std12 is not None: linha[f"STD_OI_{ativo.upper()}_12"] = std12
                    if std48 is not None: linha[f"STD_OI_{ativo.upper()}_48"] = std48
                    z12 = calcular_zscore(oi, serie_oi, 12); z48 = calcular_zscore(oi, serie_oi, 48)
                    if z12 is not None: linha[f"Zscore_OI_{ativo.upper()}_12"] = z12
                    if z48 is not None: linha[f"Zscore_OI_{ativo.upper()}_48"] = z48

        campos_validos = {k: v for k, v in linha.items() if v is not None and k != "timestamp"}
        if not campos_validos:
            print(f"[{timestamp}] Nenhum dado válido coletado — pulando insert.")
            return

        colunas = list(linha.keys())
        valores_raw = [linha[c] for c in colunas]
        valores = [normalize_value(v) for v in valores_raw]

        placeholders = ", ".join(["%s"] * len(colunas))
        colunas_sql = ", ".join(colunas)

        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO tb_deribit_info_ini ({colunas_sql}) VALUES ({placeholders})", valores)
            conn.commit()

        print(f"[{timestamp}] Dados inseridos com sucesso.")

    except Exception as e:
        print(f"Erro na atualização: {e}")
    finally:
        if conn is not None:
            conn.close()

# Executa a função principal quando chamado
if __name__ == "__main__":
    atualizar_deribit()
