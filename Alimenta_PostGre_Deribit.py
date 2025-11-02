import os
import psycopg2
import requests
import time
import pandas as pd

from psycopg2 import sql
from dotenv import load_dotenv
from datetime import datetime

# Carrega variáveis de ambiente
load_dotenv()

# Conexão ao banco
def conectar_postgres():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    return conn

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

# Função principal chamada pelo main.py
def atualizar_deribit():
    conn = None
    try:
        conn = conectar_postgres()
        criar_tabela_deribit(conn)
        print(f"[{datetime.now()}] Tabela verificada/criada com sucesso.")
        # Aqui virá a lógica de coleta da API e inserção
    except Exception as e:
        print(f"Erro na atualização: {e}")
    finally:
        if conn is not None:
        conn.close()


DERIBIT_API_URL = "https://www.deribit.com/api/v2"

def coletar_dados_deribit():
    ativos = ["BTC", "ETH", "SOL"]
    dados = {}
    timestamp = int(time.time() * 1000)  # milissegundos

    for ativo in ativos:
        # 1. Book summary (Mark Price, Volume, Open Interest)
        r_book = requests.get(f"{DERIBIT_API_URL}/public/get_book_summary_by_currency?currency={ativo}&kind=future")
        book_data = r_book.json()["result"][0]

        mark_price = book_data["mark_price"]
        volume_24h = book_data["volume"]
        open_interest = book_data["open_interest"]

        # 2. Index Price
        r_index = requests.get(f"{DERIBIT_API_URL}/public/get_index_price?index_name={ativo}_usd")
        index_price = r_index.json()["result"]["index_price"]

        # 3. Funding Rate
        r_funding = requests.get(f"{DERIBIT_API_URL}/public/get_funding_rate?instrument_name={ativo}-PERPETUAL")
        funding_rate = r_funding.json()["result"]["interest_8h"]

        # 4. Premium Rate
        premium_rate = (mark_price - index_price) / index_price

        # 5. DVOL (apenas BTC e ETH)
        dvol = None
        if ativo in ["BTC", "ETH"]:
            r_dvol = requests.get(f"{DERIBIT_API_URL}/public/get_volatility_index_data?currency={ativo}")
            dvol = r_dvol.json()["result"]["volatility"]

        # 6. Candlestick (vela)
        r_candle = requests.get(
            f"{DERIBIT_API_URL}/public/get_tradingview_chart_data?instrument_name={ativo}-PERPETUAL"
            f"&start_timestamp={timestamp - 900000}&end_timestamp={timestamp}&resolution=15"
        )
        candles = r_candle.json()["result"]["ticks"]
        if candles:
            ultima_vela = candles[-1]
            open_, high, low, close = ultima_vela["open"], ultima_vela["high"], ultima_vela["low"], ultima_vela["close"]
            upper_wick = high - max(open_, close)
            lower_wick = min(open_, close) - low
        else:
            upper_wick = lower_wick = None

        # 7. Armazenar dados
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


def atualizar_deribit():
    conn = None
    try:
        conn = conectar_postgres()
        criar_tabela_deribit(conn)

        # Coleta dados atuais da Deribit
        dados_api = coletar_dados_deribit()
        timestamp = datetime.now()

        # Busca dados anteriores para cálculo de métricas derivadas
        df_antigos = pd.read_sql("SELECT * FROM tb_deribit_info_ini ORDER BY timestamp DESC LIMIT 100", conn)

        # Cálculo de métricas derivadas
        def calcular_delta_vol(ativo):
            vol_atual = dados_api[ativo]["volume_24h"]
            vol_anterior = df_antigos[f"v24h_{ativo}"].iloc[0] if not df_antigos.empty else None
            return vol_atual - vol_anterior if vol_anterior is not None else None

        def calcular_ma(serie, n):
            return serie.head(n).mean() if len(serie) >= n else None

        def calcular_std(serie, n):
            return serie.head(n).std() if len(serie) >= n else None

        def calcular_zscore(valor, serie, n):
            media = calcular_ma(serie, n)
            desvio = calcular_std(serie, n)
            return (valor - media) / desvio if media and desvio else None

        # Monta linha para inserção
        linha = {
            "timestamp": timestamp
        }

        for ativo in ["BTC", "ETH", "SOL"]:
            linha[f"{ativo}_Mark"] = dados_api[ativo]["mark"]
            linha[f"{ativo}_Index"] = dados_api[ativo]["index"]
            linha[f"funding_{ativo}"] = dados_api[ativo]["funding"]
            linha[f"open_interest_{ativo}"] = dados_api[ativo]["open_interest"]
            linha[f"funding_{ativo}_PremiumRate"] = dados_api[ativo]["premium_rate"]
            linha[f"v24h_{ativo}"] = dados_api[ativo]["volume_24h"]
            linha[f"DeltaVol_24h_{ativo}"] = calcular_delta_vol(ativo)
            linha[f"Upper_Wick_{ativo}"] = dados_api[ativo]["upper_wick"]
            linha[f"Lower_Wick_{ativo}"] = dados_api[ativo]["lower_wick"]
            linha[f"DVOL_{ativo}"] = dados_api[ativo]["dvol"] if ativo in ["BTC", "ETH"] else None

            # Cálculos com pandas
            if not df_antigos.empty:
                serie_delta_vol = df_antigos[f"DeltaVol_24h_{ativo}"]
                linha[f"MA_DeltaVol_24h_{ativo}"] = calcular_ma(serie_delta_vol, 12)
                linha[f"Zscore_DeltaVol_24h_{ativo}"] = calcular_zscore(linha[f"DeltaVol_24h_{ativo}"], serie_delta_vol, 12)

                serie_oi = df_antigos[f"open_interest_{ativo}"]
                linha[f"Delta_open_interes_{ativo}"] = linha[f"open_interest_{ativo}"] - serie_oi.iloc[0]
                linha[f"MA_OI_{ativo}_12"] = calcular_ma(serie_oi, 12)
                linha[f"MA_OI_{ativo}_48"] = calcular_ma(serie_oi, 48)
                linha[f"STD_OI_{ativo}_12"] = calcular_std(serie_oi, 12)
                linha[f"STD_OI_{ativo}_48"] = calcular_std(serie_oi, 48)
                linha[f"Zscore_OI_{ativo}_12"] = calcular_zscore(linha[f"open_interest_{ativo}"], serie_oi, 12)
                linha[f"Zscore_OI_{ativo}_48"] = calcular_zscore(linha[f"open_interest_{ativo}"], serie_oi, 48)

        # Monta comando de inserção
        colunas = ", ".join(linha.keys())
        valores = ", ".join(["%s"] * len(linha))
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO tb_deribit_info_ini ({colunas}) VALUES ({valores})", list(linha.values()))
            conn.commit()

        print(f"[{timestamp}] Dados inseridos com sucesso.")

    except Exception as e:
        print(f"Erro na atualização: {e}")
    finally:
        if conn is not None:
            conn.close()
