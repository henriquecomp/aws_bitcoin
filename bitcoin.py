import websocket
import json
import boto3
from datetime import datetime
import threading

SOCKET_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
FIREHOSE_STREAM_NAME = 'bitcoin_firehose'
AWS_REGION = 'us-east-1'

latest_price = None
firehose_client = boto3.client('firehose', region_name=AWS_REGION)


def send_to_firehose():
    """
    Pega o último preço conhecido e envia para o Firehose.
    Esta função é chamada a cada 60 segundos pelo Timer.
    """
    global latest_price
    
    if latest_price is None:
        print(f"[{datetime.now():%H:%M:%S}] Nenhuma atualização de preço para enviar.")
        return

    # Dados para envio ao Firehose
    data_to_send = {
        'price': latest_price,
        'date': datetime.now().isoformat()
    }

    try:
        # Converte para JSON e depois para bytes
        data_bytes = json.dumps(data_to_send).encode('utf-8')
        
        response = firehose_client.put_record(
            DeliveryStreamName=FIREHOSE_STREAM_NAME,
            Record={'Data': data_bytes}
        )
        
        print(f"[{datetime.now():%H:%M:%S}] Registro enviado! Preço: {data_to_send['price']:.2f}, Record ID: {response['RecordId']}")
    except Exception as e:
        print(f"[{datetime.now():%H:%M:%S}] Erro ao enviar o registro: {e}")
        

def start_sending_schedule():
    """
    Função que agenda a si mesma para rodar a cada 60 segundos.
    """
    send_to_firehose()
    # Cria e inicia o próximo timer para rodar em 60 segundos
    threading.Timer(60, start_sending_schedule).start()


# --- Funções de Callback do WebSocket ---

def on_message(ws, message):
    """
    Chamada a cada trade. Sua única responsabilidade é atualizar o preço mais recente.
    """
    global latest_price
    data = json.loads(message)
    latest_price = float(data['p'])
    # print(f"Novo preço recebido: {latest_price}") # Descomente para debug

def on_error(ws, error):
    print(f"Ocorreu um erro: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Conexão fechada")

def on_open(ws):
    print("Conexão estabelecida. Aguardando dados de preço em tempo real...")


if __name__ == "__main__":
    print("Iniciando o agendador de envio para o Firehose (a cada 60s)...")
    start_sending_schedule()

    # Cria e inicia a conexão do WebSocket
    ws = websocket.WebSocketApp(SOCKET_URL,
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)

    ws.run_forever()