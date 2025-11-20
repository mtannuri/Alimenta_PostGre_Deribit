import requests
import websocket
import json
import threading
import time
import asyncio
import websockets


from datetime import datetime, timezone, timedelta

#Com objetivo de suavização, estou usando o Mark Price como SPOT
def get_spot_price_deribit(moeda: str, timeout_segundos: int = 10):
    """
    Consulta o preço spot da moeda via WebSocket Deribit usando o método get_book_summary_by_currency,
    filtrando por instrumentos com quote_currency = 'USDC'.

    Args:
        moeda (str): 'BTC', 'ETH', etc.
        timeout_segundos (int): tempo máximo de espera pela resposta

    Retorna:
        dict com 'instrument_name', 'mark_price', 'timestamp' ou None se falhar
    """
    resultado = {}

    msg = {
        "id": 9344,
        "jsonrpc": "2.0",
        "method": "public/get_book_summary_by_currency",
        "params": {
            "currency": moeda.upper(),
            "kind": "spot"
        }
    }

    async def call_api():
        uri = "wss://www.deribit.com/ws/api/v2"
        try:
            async with websockets.connect(uri) as websocket:
                await websocket.send(json.dumps(msg))
                response = await asyncio.wait_for(websocket.recv(), timeout=timeout_segundos)
                data = json.loads(response)
                print("📩 Resposta recebida:")
                print(json.dumps(data, indent=2))

                if "result" in data:
                    for item in data["result"]:
                        if item.get("quote_currency") == "USDC":
                            resultado.update({
                                "instrument_name": item["instrument_name"],
                                "mark_price": item["mark_price"],
                                "timestamp": datetime.utcfromtimestamp(item["creation_timestamp"] / 1000).strftime("%Y-%m-%d %H:%M:%S")
                            })
                            break
        except Exception as e:
            print(f"❌ Erro ao consultar preço spot: {e}")

    def run_async(func):
        try:
            return asyncio.run(func)
        except RuntimeError:
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(func)

    run_async(call_api())
    return resultado if resultado else None






def get_dvol_live(moeda: str, timeout_segundos: int = 60):
    """
    Escuta atualizações do DVOL para a moeda especificada via WebSocket da Deribit.
    
    Args:
        moeda (str): Código da moeda em minúsculas, ex: 'btc', 'eth'
        timeout_segundos (int): Tempo máximo de espera por uma atualização
    
    Retorna:
        dict com 'timestamp', 'volatility', 'moeda' ou None se não houver atualização
    """
    resultado = {}

    canal = f"deribit_volatility_index.{moeda.lower()}_usd"

    def on_message(ws, message):
        data = json.loads(message)
        print("📩 Mensagem recebida:")
        print(json.dumps(data, indent=2))
        if "params" in data and "data" in data["params"]:
            info = data["params"]["data"]
            resultado.update({
                "timestamp": datetime.utcfromtimestamp(info.get("timestamp") / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                "volatility": info.get("volatility"),
                "moeda": moeda.upper()
            })
            print(f"✅ DVOL {moeda.upper()}/USD: {resultado['volatility']} | timestamp: {resultado['timestamp']}")
            ws.close()

    def on_error(ws, error):
        print(f"Erro: {error}")

    def on_close(ws, close_status_code, close_msg):
        print("Conexão encerrada.")

    def on_open(ws):
        subscribe_msg = {
            "jsonrpc": "2.0",
            "method": "public/subscribe",
            "params": {
                "channels": [canal]
            }
        }
        ws.send(json.dumps(subscribe_msg))
        print(f"📡 Assinado canal {canal}")

    ws_url = "wss://www.deribit.com/ws/api/v2"
    ws = websocket.WebSocketApp(ws_url,
                                 on_open=on_open,
                                 on_message=on_message,
                                 on_error=on_error,
                                 on_close=on_close)

    wst = threading.Thread(target=lambda: ws.run_forever(ping_interval=30))
    wst.daemon = True
    wst.start()

    inicio = time.time()
    while time.time() - inicio < timeout_segundos:
        if resultado:
            return resultado
        print(f"⏳ Aguardando atualizações do DVOL {moeda.upper()}/USD...")
        time.sleep(10)

    print(f"❌ Nenhuma atualização recebida em {timeout_segundos} segundos.")
    return None



def get_resumo_perpetuo_deribit(moeda: str, timeout_segundos: int = 10) -> dict:
    """
    Consulta os dados do contrato PERPETUAL da moeda especificada na Deribit.

    Args:
        moeda (str): Código da moeda (ex: 'btc', 'eth') — minúsculo.
        timeout_segundos (int): Tempo máximo de espera pela resposta.

    Returns:
        dict com os campos:
            - instrument_name
            - funding_8h
            - interest_rate
            - current_funding
            - open_interest
            - volume
            - volume_notional
            - volume_usd
            - mark_price
            - timestamp (em UTC)
            - moeda
    """
    import requests
    from datetime import datetime

    url = "https://www.deribit.com/api/v2/public/get_book_summary_by_currency"
    params = {
        "currency": moeda.lower(),
        "kind": "future"
    }

    try:
        response = requests.get(url, params=params, timeout=timeout_segundos)
        response.raise_for_status()
        resultados = response.json().get("result", [])

        for item in resultados:
            if item.get("instrument_name", "").endswith("-PERPETUAL"):
                return {
                    "instrument_name": item.get("instrument_name"),
                    "funding_8h": item.get("funding_8h"),
                    "interest_rate": item.get("interest_rate"),
                    "current_funding": item.get("current_funding"),
                    "open_interest": item.get("open_interest"),
                    "volume": item.get("volume"),
                    "volume_notional": item.get("volume_notional"),
                    "volume_usd": item.get("volume_usd"),
                    "mark_price": item.get("mark_price"),
                    "timestamp": datetime.utcfromtimestamp(item.get("creation_timestamp") / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                    "moeda": moeda.upper()
                }

        print(f"❌ Contrato PERPETUAL não encontrado para {moeda.upper()}.")
        return None

    except Exception as e:
        print(f"❌ Erro ao buscar dados PERPETUAL de {moeda.upper()}: {e}")
        return None
    

def get_resumo_candle_deribit(moeda: str, timeout_segundos: int = 10) -> dict:
    """
    Consulta o candle de 15 minutos mais recente para o contrato PERPETUAL da moeda especificada.

    Retorna:
        dict com open, high, low, close, volume, cost, timestamp, instrumento, moeda
    """
    import time
    from datetime import datetime
    import requests

    agora = int(time.time())
    fim = agora * 1000
    inicio = (agora - 900) * 1000  # 15 minutos atrás

    instrumento = f"{moeda.upper()}-PERPETUAL"
    url = "https://www.deribit.com/api/v2/public/get_tradingview_chart_data"
    params = {
        "instrument_name": instrumento,
        "start_timestamp": inicio,
        "end_timestamp": fim,
        "resolution": 15
    }

    try:
        response = requests.get(url, params=params, timeout=timeout_segundos)
        response.raise_for_status()
        data = response.json().get("result", {})

        if data and data.get("ticks"):
            return {
                "instrument_name": instrumento,
                "moeda": moeda.upper(),
                "timestamp": datetime.utcfromtimestamp(data["ticks"][-1] / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                "open": data["open"][-1],
                "high": data["high"][-1],
                "low": data["low"][-1],
                "close": data["close"][-1],
                "volume": data["volume"][-1],
                "cost": data["cost"][-1]
            }

    except Exception as e:
        print(f"❌ Erro ao buscar candle de 15min para {moeda.upper()}: {e}")

    return None



def calcular_imbalance(bids, asks, n=5):
    volume_bids = sum([b[1] for b in bids[:n]])
    volume_asks = sum([a[1] for a in asks[:n]])

    if volume_bids + volume_asks == 0:
        return 0

    return (volume_bids - volume_asks) / (volume_bids + volume_asks)

def get_imbalance(moeda: str) -> dict:
    instrument = f"{moeda.upper()}-PERPETUAL"
    url = "https://www.deribit.com/api/v2/public/get_order_book"
    params = {"instrument_name": instrument}

    try:
        response = requests.get(url, params=params)
        data = response.json()

        if "result" in data:
            bids = data["result"]["bids"]
            asks = data["result"]["asks"]
            imbalance = calcular_imbalance(bids, asks)

            return {
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "moeda": moeda.upper(),
                "imbalance": imbalance,
                "volume_bids": sum([b[1] for b in bids[:5]]),
                "volume_asks": sum([a[1] for a in asks[:5]])
            }

    except Exception as e:
        print(f"❌ Erro ao obter imbalance: {e}")

    return {}
