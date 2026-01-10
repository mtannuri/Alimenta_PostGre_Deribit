from typing import Dict, List
import pandas as pd

# Armazena os dados temporariamente
dados_spot: List[Dict] = []
dados_dvol: List[Dict] = []
dados_resumo_perpetuo: List[Dict] = []
dados_futuro: List[Dict] = []  # <-- adicionado

def adicionar_spot(dado: Dict):
    """Adiciona um registro de preço spot à lista."""
    dados_spot.append(dado)

def adicionar_dvol(dado: Dict):
    """Adiciona um registro de DVOL à lista."""
    dados_dvol.append(dado)

def adicionar_resumo_perpetuo(dado: Dict):
    """Adiciona um registro de resumo do contrato perpétuo à lista."""
    dados_resumo_perpetuo.append(dado)

dados_candle_15m: List[Dict] = []

def adicionar_resumo_candle_15m(dado: Dict):
    """Adiciona um registro de candle de 15 minutos à lista."""
    dados_candle_15m.append(dado)
    
# Lista para armazenar os dados de imbalance
dados_imbalance: List[Dict] = []

def adicionar_imbalance(dado: dict):
    print(f"🟢 Salvando imbalance: {dado}")
    dados_imbalance.append(dado)

def obter_dataframe_spot() -> pd.DataFrame:
    """Retorna os dados spot como DataFrame."""
    return pd.DataFrame(dados_spot)

def obter_dataframe_dvol() -> pd.DataFrame:
    """Retorna os dados DVOL como DataFrame."""
    return pd.DataFrame(dados_dvol)

def obter_dataframe_futuro() -> pd.DataFrame:
    """Retorna os dados futuros como DataFrame."""
    return pd.DataFrame(dados_futuro)

def obter_dataframe_resumo_perpetuo() -> pd.DataFrame:
    """Retorna os dados do resumo do contrato perpétuo como DataFrame."""
    return pd.DataFrame(dados_resumo_perpetuo)

def obter_dataframe_resumo_candle_15m() -> pd.DataFrame:
    """Retorna os dados de candle de 15 minutos como DataFrame."""
    return pd.DataFrame(dados_candle_15m)

def obter_dataframe_imbalance() -> pd.DataFrame:
    return pd.DataFrame(dados_imbalance)

def limpar():
    """Limpa todos os dados armazenados."""
    dados_spot.clear()
    dados_dvol.clear()
    dados_futuro.clear()
    dados_resumo_perpetuo.clear()
    dados_candle_15m.clear()
    dados_imbalance.clear()
