#!/usr/bin/env python3

import os
import logging
#from dotenv import load_dotenv - para rodar localmente
from organizador import (
    adicionar_spot,
    adicionar_dvol,
    obter_dataframe_spot,
    obter_dataframe_dvol,
    dados_spot,
    dados_dvol,
    adicionar_resumo_perpetuo,
    obter_dataframe_resumo_perpetuo,
    dados_resumo_perpetuo,
    adicionar_resumo_candle_15m,
    obter_dataframe_resumo_candle_15m,
    dados_candle_15m,
    adicionar_imbalance,
    obter_dataframe_imbalance
)

from acessa_deribit import (
    get_spot_price_deribit,
    get_dvol_live,
    get_resumo_perpetuo_deribit,
    get_resumo_candle_deribit,
    get_imbalance
)



# Carrega variáveis de ambiente
#load_dotenv() - para rodar localmente

# Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("Alimenta_PostGre_Deribit")

# Função para processar preços spot
def processar_spot(moeda: str):
    preco_spot = get_spot_price_deribit(moeda)
    if preco_spot:
        print(f"💰 Preço spot {moeda}/USDC: {preco_spot['mark_price']} | timestamp: {preco_spot['timestamp']}")
        adicionar_spot(preco_spot)
    else:
        print(f"❌ Não foi possível obter o preço spot de {moeda}.")

# Função para processar DVOL
def processar_dvol(moeda: str):
    dado_dvol = get_dvol_live(moeda, timeout_segundos=60)
    if dado_dvol:
        print(f"🎯 DVOL recebido: {dado_dvol}")
        adicionar_dvol(dado_dvol)
    else:
        print(f"❌ Não houve atualização para DVOL {moeda.upper()}.")

# Função para processar resumo do contrato perpétuo
def processar_resumo_perpetuo(moeda: str):
    resumo = get_resumo_perpetuo_deribit(moeda)
    if resumo:
        print(f"📈 Mark price futuro {resumo['instrument_name']}: {resumo['mark_price']} | timestamp: {resumo['timestamp']}")
        print(f"📊 Resumo PERPETUAL {resumo['instrument_name']}:")
        print(f"   - funding_8h: {resumo['funding_8h']}")
        print(f"   - current_funding: {resumo['current_funding']}")
        print(f"   - open_interest: {resumo['open_interest']}")
        print(f"   - volume: {resumo['volume']}")
        print(f"   - volume_notional: {resumo['volume_notional']}")
        print(f"   - volume_usd: {resumo['volume_usd']}")
        print(f"   - timestamp: {resumo['timestamp']}")
        adicionar_resumo_perpetuo(resumo)
    else:
        print(f"❌ Não foi possível obter o resumo PERPETUAL de {moeda.upper()}.")

def processar_candle_15m(moeda: str):
    candle = get_resumo_candle_deribit(moeda)
    if candle:
        print(f"🕒 Candle 15min {candle['instrument_name']}:")
        print(f"   - open: {candle['open']}")
        print(f"   - high: {candle['high']}")
        print(f"   - low: {candle['low']}")
        print(f"   - close: {candle['close']}")
        print(f"   - volume: {candle['volume']}")
        print(f"   - cost: {candle['cost']}")
        print(f"   - timestamp: {candle['timestamp']}")
        adicionar_resumo_candle_15m(candle)
    else:
        print(f"❌ Não foi possível obter o candle de 15min de {moeda.upper()}.")



def main():
    # Processa preços spot
    for moeda in ["BTC", "ETH"]:
        processar_spot(moeda)

    # Processa DVOL
    for moeda in ["eth", "btc"]:
        processar_dvol(moeda)

    # Processa resumo do contrato perpétuo
    for moeda in ["btc", "eth"]:
        processar_resumo_perpetuo(moeda)
        
    # Processa candle de 15 minutos
    for moeda in ["btc", "eth"]:
        processar_candle_15m(moeda)
        
    for moeda in ["btc", "eth"]:
        imbalance = get_imbalance(moeda)
        if imbalance:
            adicionar_imbalance(imbalance)


    # Converte dados para DataFrame (se necessário para salvar ou exibir)
    df_spot = obter_dataframe_spot()
    df_dvol = obter_dataframe_dvol()
    df_resumo_perpetuo = obter_dataframe_resumo_perpetuo()
    df_candle_15m = obter_dataframe_resumo_candle_15m()
    df_imbalance = obter_dataframe_imbalance()


    from AlimentaPostgre import montar_linha_para_insercao, inserir_linha_no_banco

    linha = montar_linha_para_insercao()
    inserir_linha_no_banco(linha)

    
    




    # Exibe dados consolidados
    print("\nValores guardados:")
    print(f"- dados_spot: {dados_spot}")
    print(f"- dados_dvol: {dados_dvol}")
    print(f"- dados_resumo_perpetuo: {dados_resumo_perpetuo}")
    print(f"- dados_candle_15m: {dados_candle_15m}")
    print(f"- dados_imbalance:\n{df_imbalance}")
 
    
    
    
    # Limpeza de variáveis grandes
    del df_spot, df_dvol, df_resumo_perpetuo, df_candle_15m, df_imbalance
    # Força coleta de lixo para liberar memória
    import gc
    gc.collect()

if __name__ == "__main__":
    main()

