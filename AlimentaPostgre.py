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
        conn = psycopg2.connect(os.getenv("DB_URL"))
        cur = conn.cursor()

        cur.execute("SELECT current_database(), current_schema();")
        print("Conectado em:", cur.fetchone())

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
            row[f"{moeda}_volume_candle"] = candle.get("volume")
            row[f"{moeda}_open_candle"] = candle.get("open")
            row[f"{moeda}_high_candle"] = candle.get("high")
            row[f"{moeda}_low_candle"] = candle.get("low")
            row[f"{moeda}_close_candle"] = candle.get("close")

            imb = next((d for d in reversed(dados_imbalance) if d.get("moeda", "").upper() == moeda), {})
            row[f"{moeda}_volume_bids"] = imb.get("volume_bids")
            row[f"{moeda}_volume_asks"] = imb.get("volume_asks")
            row[f"{moeda}_Imbalance"] = imb.get("imbalance")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Erro ao montar linha: {e}")

    return row


def inserir_linha_no_banco(row):
    colunas = list(row.keys())
    valores = [row[col] for col in colunas]

    placeholders = ', '.join(['%s'] * len(colunas))
    nomes_colunas = ', '.join(colunas)

    # Query para tabela principal
    query = f"""
        INSERT INTO tbGeralDeribit ({nomes_colunas})
        VALUES ({placeholders})
    """

    # Query para tabela temporária
    query_temp = f"""
        INSERT INTO tbGeralDeribit_temp ({nomes_colunas})
        VALUES ({placeholders})
    """

    try:
        conn = psycopg2.connect(os.getenv("DB_URL"))
        cur = conn.cursor()

        cur.execute("SELECT current_database(), current_schema();")
        print("Conectado em:", cur.fetchone())

        # 1) Sempre insere na tabela temporária
        cur.execute(query_temp, valores)

        # 2) Conta registros na temp
        cur.execute("SELECT COUNT(*) FROM tbGeralDeribit_temp;")
        count_temp = cur.fetchone()[0]
        delta = count_temp - 31
        print(f"Registros em tbGeralDeribit_temp: {count_temp} | delta = {delta}")

        # 3) Se delta >= 46: apaga 15 mais antigas e insere na principal
        if delta >= 46:
            cur.execute("""
                DELETE FROM tbGeralDeribit_temp
                WHERE id IN (
                    SELECT id FROM tbGeralDeribit_temp
                    ORDER BY id ASC
                    LIMIT 15
                )
            """)
            print("🧹 Removidas as 15 linhas mais antigas de tbGeralDeribit_temp.")

            cur.execute(query, valores)
            print("📦 Inserção realizada em tbGeralDeribit.")

        conn.commit()
        cur.close()
        conn.close()
        print("✅ Operação concluída com sucesso.")
    except Exception as e:
        print(f"❌ Erro ao inserir no banco: {e}")
