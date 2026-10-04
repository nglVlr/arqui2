# Despachador: avisa a las entidades externas. Consume correlated.alerts y,
# según el tipo de alerta, la reenvía a los topics external.pnc,
# external.bomberos y external.conred (Kafka) y a la aseguradora (REST).
# Registra cada envío en la tabla dispatches, indexa la alerta en
# Elasticsearch (si ELASTICSEARCH_URL está definido) y guarda los eventos de
# events.invalid en la tabla invalid_events para el panel de calidad de datos.

import json
import logging
import os
import signal

import psycopg2
import requests
from confluent_kafka import Consumer, Producer

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
URL_ASEGURADORA = os.getenv("ASEGURADORA_URL", "http://aseguradora-mock:8000")
URL_ELASTIC = os.getenv("ELASTICSEARCH_URL", "")   # vacío en el perfil core

# Tabla de enrutamiento (sección 7.4 del plan)
DESTINOS = {
    "ROBO_EN_CURSO": ["external.pnc"],
    "ATAQUE_ARMADO": ["external.pnc"],
    "VEHICULO_REPORTADO": ["external.pnc"],
    "INCENDIO_CONFIRMADO": ["external.bomberos", "external.conred"],
    "ACCIDENTE_VIAL": ["external.pnc", "external.bomberos", "aseguradora"],
    "VELOCIDAD_ENTRE_CAMARAS": ["external.pnc"],
    "ZONA_ESCOLAR": ["external.pnc"],
    "PANICO_NO_CONFIRMADO": [],   # solo se registra, no se avisa a nadie
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s despachador: %(message)s")
log = logging.getLogger("despachador")

consumidor = Consumer({"bootstrap.servers": KAFKA, "group.id": "despachador", "enable.auto.commit": False,
                       "auto.offset.reset": "earliest", "session.timeout.ms": 10000})
productor = Producer({"bootstrap.servers": KAFKA, "acks": "all", "enable.idempotence": True, "linger.ms": 5})
conexion = psycopg2.connect(host=os.getenv("POSTGRES_HOST", "postgres"), dbname=os.getenv("POSTGRES_DB"),
                            user=os.getenv("APP_DB_USER"), password=os.getenv("APP_DB_PASSWORD"))
cursor = conexion.cursor()


def registrar_envio(alert_id, destino, protocolo, resultado):
    cursor.execute("INSERT INTO dispatches (alert_id, destino, protocolo, resultado) VALUES (%s, %s, %s, %s)",
                   (alert_id, destino, protocolo, resultado))


def ya_despachada(alert_id):
    # Tras una caída se reprocesa el último lote: no avisar dos veces la misma alerta
    cursor.execute("SELECT 1 FROM dispatches WHERE alert_id = %s LIMIT 1", (alert_id,))
    return cursor.fetchone() is not None


def avisar_aseguradora(alerta):
    """Envío por REST. Si la aseguradora no responde, queda registrado el error."""
    try:
        respuesta = requests.post(f"{URL_ASEGURADORA}/siniestros", json=alerta, timeout=3)
        respuesta.raise_for_status()
        return f"OK caso {respuesta.json()['numeroCaso']}"
    except requests.RequestException as error:
        log.warning("Aseguradora no disponible: %s", error)
        return f"ERROR {type(error).__name__}"


def indexar_en_elasticsearch(alerta):
    """PUT con el alertId como id del documento: si se repite, se sobrescribe (no duplica)."""
    if not URL_ELASTIC:
        return
    try:
        requests.put(f"{URL_ELASTIC}/alertas/_doc/{alerta['alertId']}", json=alerta, timeout=3).raise_for_status()
    except requests.RequestException as error:
        log.warning("No se pudo indexar en Elasticsearch: %s", error)


def despachar(alerta):
    indexar_en_elasticsearch(alerta)
    if ya_despachada(alerta["alertId"]):
        return
    destinos = DESTINOS.get(alerta["alertType"], [])
    for destino in destinos:
        if destino == "aseguradora":
            registrar_envio(alerta["alertId"], destino, "REST", avisar_aseguradora(alerta))
        else:
            productor.produce(destino, key=alerta["zone"], value=json.dumps(alerta, ensure_ascii=False))
            registrar_envio(alerta["alertId"], destino, "KAFKA", "OK")
    if destinos:
        log.info("Alerta %s en %s enviada a: %s", alerta["alertType"], alerta["zoneName"], ", ".join(destinos))
    else:
        registrar_envio(alerta["alertId"], "ninguno", "NINGUNO", "SOLO_REGISTRO")


def guardar_invalido(invalido):
    original = invalido.get("original")
    zona = original.get("location", {}).get("zone") if isinstance(original, dict) and \
        isinstance(original.get("location"), dict) else None
    cursor.execute("INSERT INTO invalid_events (reason, ingestor_id, received_at, zone, original) "
                   "VALUES (%s, %s, %s, %s, %s)",
                   (invalido["reason"], invalido.get("ingestorId"), invalido["receivedAt"], zona,
                    json.dumps(original)))


corriendo = True


def detener(_senal, _marco):
    global corriendo
    corriendo = False


def main():
    signal.signal(signal.SIGTERM, detener)
    consumidor.subscribe(["correlated.alerts", "events.invalid"])
    log.info("Despachador iniciado (Elasticsearch: %s)", URL_ELASTIC or "desactivado")
    while corriendo:
        mensajes = consumidor.consume(num_messages=500, timeout=0.2)
        for mensaje in mensajes:
            if mensaje.error():
                log.warning("Error de Kafka: %s", mensaje.error())
                continue
            datos = json.loads(mensaje.value())
            if mensaje.topic() == "correlated.alerts":
                despachar(datos)
            else:
                guardar_invalido(datos)
        # Mismo orden que el correlador: base de datos, Kafka y al final el offset
        conexion.commit()
        productor.flush(10)
        if mensajes:
            consumidor.commit(asynchronous=False)
    consumidor.close()
    log.info("Despachador detenido ordenadamente")


if __name__ == "__main__":
    main()
