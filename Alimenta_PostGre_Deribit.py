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

def get_result_safe(response, descricao=""):
    try:
        data = response.json()
    except Exception as e:
        print(f"⚠️ Erro ao decodificar JSON {descricao}: {e}")
        return None
    if isinstance(data, dict) and "result" in data:
        return data["result"]
    print(f"⚠️ Resposta inesperada {descricao}: {data}")
    return None

def coletar_dados_deribit():
    import requests, time
    DERIBIT_API_URL = "https://www.deribit.com/api/v2"
    ativos = ["btc", "eth", "sol"]
    dados = {}
    timestamp = int(time.time() * 1000)  # ms

    for ativo in ativos:
        # 1) Book summary (currency={ativo})
        # Request book summary — sem 'kind=all', use currency em MAIÚSCULAS
r_book = requests.get(f"{DERIBIT_API_URL}/public/get_book_summary_by_currency?currency={ativo.upper()}")
        book_result = get_result_safe(r_book, f"book_summary {ativo}")
        if not book_result or not isinstance(book_result, list) or len(book_result) == 0:
            print(f"⚠️ book_summary vazio para {ativo}, pulando ativo")
            continue
        book_data = book_result[0] if isinstance(book_result, list) else book_result

        # Campos básicos (validação)
        mark_price = book_data.get("mark_price") if isinstance(book_data, dict) else None
        volume_24h = book_data.get("volume") if isinstance(book_data, dict) else None
        open_interest = book_data.get("open_interest") if isinstance(book_data, dict) else None

        # Funding rate extraído do book_summary quando disponível
        funding_rate = None
        if isinstance(book_data, dict):
            funding_rate = book_data.get("funding_8h") or book_data.get("funding_8h_rate") or book_data.get("funding_rate")

        # 2) Index price (index_name deve ser lowercase)
        r_index = requests.get(f"{DERIBIT_API_URL}/public/get_index_price?index_name={ativo}_usd")
        index_result = get_result_safe(r_index, f"index_price {ativo}")
        index_price = index_result.get("index_price") if isinstance(index_result, dict) else None

        # 3) Premium rate (seguro)
        premium_rate = None
        if isinstance(mark_price, (int, float)) and isinstance(index_price, (int, float)) and index_price != 0:
            premium_rate = (mark_price - index_price) / index_price

        # 4) DVOL (apenas btc e eth) - exige start_timestamp e end_timestamp
        dvol = None
        if ativo in ["btc", "eth"]:
            start_ts = timestamp - (15 * 60 * 1000)  # 15 minutos atrás em ms (ajuste se quiser)
            end_ts = timestamp
            r_dvol = requests.get(
                f"{DERIBIT_API_URL}/public/get_volatility_index_data?currency={ativo}"
                f"&start_timestamp={start_ts}&end_timestamp={end_ts}"
            )
            dvol_result = get_result_safe(r_dvol, f"dvol {ativo}")
            if isinstance(dvol_result, dict):
                dvol = dvol_result.get("volatility") or dvol_result.get("value")

        # 5) Candlestick (vela) - instrument_name com ticker em MAIÚSCULAS para PERPETUAL
        start_ts_candle = timestamp - (15 * 60 * 1000)
        r_candle = requests.get(
            f"{DERIBIT_API_URL}/public/get_tradingview_chart_data?instrument_name={ativo.upper()}-PERPETUAL"
            f"&start_timestamp={start_ts_candle}&end_timestamp={timestamp}&resolution=15"
        )
        candle_result = get_result_safe(r_candle, f"candlestick {ativo}")
        upper_wick = lower_wick = None
        if isinstance(candle_result, dict):
            ticks = candle_result.get("ticks") or candle_result.get("ticks_list") or []
            if isinstance(ticks, list) and len(ticks) > 0:
                ultima = ticks[-1]
                if isinstance(ultima, dict):
                    o = ultima.get("open"); h = ultima.get("high"); l = ultima.get("low"); c = ultima.get("close")
                    if all(isinstance(x, (int, float)) for x in [o, h, l, c]):
                        upper_wick = h - max(o, c)
                        lower_wick = min(o, c) - l

        # 6) armazenar com segurança
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
    

from datetime import datetime

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

        # Funções auxiliares
        def calcular_delta_vol(ativo):
            vol_atual = dados_api[ativo].get("volume_24h")
            vol_anterior = df_antigos[f"v24h_{ativo}"].iloc[0] if not df_antigos.empty else None
            return vol_atual - vol_anterior if vol_atual and vol_anterior else None

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
            if ativo not in dados_api:
                print(f"⚠️ Dados ausentes para {ativo}, pulando...")
                continue

            dados = dados_api[ativo]
            linha[f"{ativo}_Mark"] = dados.get("mark")
            linha[f"{ativo}_Index"] = dados.get("index")
            linha[f"funding_{ativo}"] = dados.get("funding")
            linha[f"open_interest_{ativo}"] = dados.get("open_interest")
            linha[f"funding_{ativo}_PremiumRate"] = dados.get("premium_rate")
            linha[f"v24h_{ativo}"] = dados.get("volume_24h")
            linha[f"DeltaVol_24h_{ativo}"] = calcular_delta_vol(ativo)
            linha[f"Upper_Wick_{ativo}"] = dados.get("upper_wick")
            linha[f"Lower_Wick_{ativo}"] = dados.get("lower_wick")
            linha[f"DVOL_{ativo}"] = dados.get("dvol") if ativo in ["BTC", "ETH"] else None

            # Cálculos com pandas
            if not df_antigos.empty:
                serie_delta_vol = df_antigos[f"DeltaVol_24h_{ativo}"]
                linha[f"MA_DeltaVol_24h_{ativo}"] = calcular_ma(serie_delta_vol, 12)
                linha[f"Zscore_DeltaVol_24h_{ativo}"] = calcular_zscore(linha[f"DeltaVol_24h_{ativo}"], serie_delta_vol, 12)

                serie_oi = df_antigos[f"open_interest_{ativo}"]
                linha[f"Delta_open_interes_{ativo}"] = linha[f"open_interest_{ativo}"] - serie_oi.iloc[0] if linha[f"open_interest_{ativo}"] else None
                linha[f"MA_OI_{ativo}_12"] = calcular_ma(serie_oi, 12)
                linha[f"MA_OI_{ativo}_48"] = calcular_ma(serie_oi, 48)
                linha[f"STD_OI_{ativo}_12"] = calcular_std(serie_oi, 12)
                linha[f"STD_OI_{ativo}_48"] = calcular_std(serie_oi, 48)
                linha[f"Zscore_OI_{ativo}_12"] = calcular_zscore(linha[f"open_interest_{ativo}"], serie_oi, 12)
                linha[f"Zscore_OI_{ativo}_48"] = calcular_zscore(linha[f"open_interest_{ativo}"], serie_oi, 48)

# Depois de montar `linha` (dicionário)....
# Remover chaves com valor None não é obrigatório, mas ajuda a verificação
campos_validos = {k: v for k, v in linha.items() if v is not None and k != "timestamp"}

if not campos_validos:
    print(f"[{timestamp}] Nenhum dado válido coletado — pulando insert.")
    return  # ou continue, dependendo do fluxo desejado

# Se quiser manter o comportamento de inserir apenas quando há dados:
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
