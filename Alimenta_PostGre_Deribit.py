import os
import psycopg2
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
    try:
        conn = conectar_postgres()
        criar_tabela_deribit(conn)
        print(f"[{datetime.now()}] Tabela verificada/criada com sucesso.")
        # Aqui virá a lógica de coleta da API e inserção
    except Exception as e:
        print(f"Erro na atualização: {e}")
    finally:
        if conn:
            conn.close()
