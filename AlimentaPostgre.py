# AlimentaPostgre.py

import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os

from organizador import (
    dados_spot,
    dados_dvol,
    dados_resumo_perpetuo,
    dados_candle_15m,
    dados_imbalance
)

# Carrega variáveis de ambiente
load_dotenv()

def montar_linha_para_insercao():
    row = {"Datetime": datetime.utcnow()}

    try:
        #conn = psycopg2.connect(      para rodar local
        #    dbname=os.getenv("DB_NAME"),
        #    user=os.getenv("DB_USER"),
        #    password=os.getenv("DB_PASSWORD"),
        #    host=os.getenv("DB_HOST"),
        #    port=os.getenv("DB_PORT")
        #)
        conn = psycopg2.connect(os.getenv("DB_URL"))
        cur = conn.cursor()

        for moeda in ["BTC", "ETH"]:
            spot = next((d for d in reversed(dados_spot) if d.get("instrument_name") == f"{moeda}_USDC"), {})
            row[f"{moeda}_USDC_spot"] = spot.get("mark_price")

            dvol = next((d for d in reversed(dados_dvol) if d.get("moeda", "").upper() == moeda), {})
            row[f"{moeda}_USDC_DVOL"] = dvol.get("volatility")

            perp = next((d for d in reversed(dados_resumo_perpetuo) if d.get("moeda", "").upper() == moeda), {})
            row[f"{moeda}_PERPETUAL_mark_price"] = perp.get("mark_price")
            row[f"{moeda}_funding_8h"] = perp.get("funding_8h")
            row[f"{moeda}_current_funding"] = perp.get("current_funding")
            row[f"{moeda}_open_interest"] = perp.get("open_interest")
            row[f"{moeda}_PERPETUAL_volume_24h"] = perp.get("volume")
            row[f"{moeda}_PERPETUAL_volume_24h_notional"] = perp.get("volume_notional")

            candle = next((d for d in reversed(dados_candle_15m) if d.get("moeda", "").upper() == moeda), {})
            volume_atual = candle.get("volume")
            row[f"{moeda}_volume_candle"] = volume_atual
            row[f"{moeda}_open_candle"] = candle.get("open")
            row[f"{moeda}_high_candle"] = candle.get("high")
            row[f"{moeda}_low_candle"] = candle.get("low")
            row[f"{moeda}_close_candle"] = candle.get("close")

            imb = next((d for d in reversed(dados_imbalance) if d.get("moeda", "").upper() == moeda), {})
            row[f"{moeda}_volume_bids"] = imb.get("volume_bids")
            row[f"{moeda}_volume_asks"] = imb.get("volume_asks")
            row[f"{moeda}_Imbalance"] = imb.get("imbalance")

            # Médias móveis de volume (incluindo o volume atual)
            cur.execute(f"""
                SELECT {moeda}_volume_candle
                FROM deribit_15m
                WHERE {moeda}_volume_candle IS NOT NULL
                ORDER BY Datetime DESC
                LIMIT 15
            """)
            volumes_anteriores = [v[0] for v in cur.fetchall()]
            volumes = [float(volume_atual)] + [float(v) for v in volumes_anteriores]
            row[f"{moeda}_volume_3_p"] = round(sum(volumes[:3]) / 3, 2) if len(volumes) >= 3 else None
            row[f"{moeda}_volume_8_p"] = round(sum(volumes[:8]) / 8, 2) if len(volumes) >= 8 else None
            row[f"{moeda}_volume_16_p"] = round(sum(volumes[:16]) / 16, 2) if len(volumes) >= 16 else None

            # Deltas de open interest (atual - valor de N períodos atrás)
            atual = row[f"{moeda}_open_interest"]
            cur.execute(f"""
                SELECT {moeda}_open_interest
                FROM deribit_15m
                WHERE {moeda}_open_interest IS NOT NULL
                ORDER BY Datetime DESC
                LIMIT 16
            """)
            historico = [v[0] for v in cur.fetchall()]
            for p in [1, 2, 3, 4, 8, 16]:
                delta = atual - historico[p - 1] if len(historico) >= p else None
                row[f"{moeda}_delta_open_interest_{p}p"] = round(delta, 2) if delta is not None else None

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Erro ao calcular campos derivados: {e}")

    return row




def inserir_linha_no_banco(row):
    colunas = list(row.keys())
    valores = [row[col] for col in colunas]

    placeholders = ', '.join(['%s'] * len(colunas))
    nomes_colunas = ', '.join(colunas)

    query = f"""
        INSERT INTO deribit_15m ({nomes_colunas})
        VALUES ({placeholders})
    """

    try:
        #conn = psycopg2.connect( para rodar local
        #    dbname=os.getenv("DB_NAME"),
        #    user=os.getenv("DB_USER"),
        #    password=os.getenv("DB_PASSWORD"),
        #    host=os.getenv("DB_HOST"),
        #    port=os.getenv("DB_PORT")
        #)

        conn = psycopg2.connect(os.getenv("DB_URL"))

        
        cur = conn.cursor()
        cur.execute(query, valores)
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Linha inserida com sucesso no banco de dados.")
    except Exception as e:
        print(f"❌ Erro ao inserir no banco: {e}")
