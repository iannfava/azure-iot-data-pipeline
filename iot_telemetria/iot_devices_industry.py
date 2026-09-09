import time, json, random
from datetime import datetime, timedelta, timezone
from azure.iot.device import IoTHubDeviceClient, Message
from dotenv import load_dotenv
import os

load_dotenv()  # le o arquivo .env na mesma pasta

# Um device por maquina -- cria os 8 dispositivos no IoT Hub antes
# (Portal Azure -> IoT Hub -> Dispositivos -> Adicionar, um por id_maquina)
MAQUINAS = [
    {"id_maquina": "M01", "linha_producao": "L1", "connection_string": os.getenv("MAQUINA_M01_CONN")},
    {"id_maquina": "M02", "linha_producao": "L1", "connection_string": os.getenv("MAQUINA_M02_CONN")},
    {"id_maquina": "M03", "linha_producao": "L1", "connection_string": os.getenv("MAQUINA_M03_CONN")},
    {"id_maquina": "M04", "linha_producao": "L2", "connection_string": os.getenv("MAQUINA_M04_CONN")},
    {"id_maquina": "M05", "linha_producao": "L2", "connection_string": os.getenv("MAQUINA_M05_CONN")},
    {"id_maquina": "M06", "linha_producao": "L2", "connection_string": os.getenv("MAQUINA_M06_CONN")},
    {"id_maquina": "M07", "linha_producao": "L3", "connection_string": os.getenv("MAQUINA_M07_CONN")},
    {"id_maquina": "M08", "linha_producao": "L3", "connection_string": os.getenv("MAQUINA_M08_CONN")},
]
DIA_INICIO = datetime(2026, 7, 1, tzinfo=timezone.utc)
DIAS = 7
LEITURAS_POR_DIA = 15  # espalhadas ao longo do dia, por maquina
def gerar_leitura(maquina, timestamp):
    return {
        "id_maquina": maquina["id_maquina"],
        "linha_producao": maquina["linha_producao"],
        "timestamp": timestamp.isoformat(),
        "temperatura": round(random.uniform(60.0, 95.0), 2),
        "vibracao_mm_s": round(random.uniform(0.5, 4.5), 2),
        "rpm": random.randint(1200, 1800),
    }
clients = [
    (m, IoTHubDeviceClient.create_from_connection_string(m["connection_string"]))
    for m in MAQUINAS
]
for dia_offset in range(DIAS):
    dia = DIA_INICIO + timedelta(days=dia_offset)
    for maquina, client in clients:
        for _ in range(LEITURAS_POR_DIA):
            # timestamp aleatorio dentro do dia -- e isso que faz a leitura
            # "pertencer" aquele dia, mesmo enviada agora
            hora_aleatoria = dia + timedelta(
                hours=random.randint(0, 23), minutes=random.randint(0, 59)
            )
            leitura = gerar_leitura(maquina, hora_aleatoria)
            client.send_message(Message(json.dumps(leitura)))
        print(f"Dia {dia.date()} - {maquina['id_maquina']}: {LEITURAS_POR_DIA} leituras enviadas")
    time.sleep(1)  # respiro entre dias, evita throttling do IoT Hub
print("Backfill concluido -- 7 dias de telemetria para as 8 maquinas")