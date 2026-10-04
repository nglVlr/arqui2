# Productor de Kafka del ingestor.
# app.py le pasa listas de mensajes ya validados (o rechazados) y este archivo
# los publica en events.standardized o events.invalid usando confluent-kafka.
# Si Kafka no confirma a tiempo, lanza KafkaNoDisponible para que app.py
# responda HTTP 503 y el cliente (simulador o JMeter) reintente.

import json
import os

from confluent_kafka import Producer

TOPIC_VALIDOS = "events.standardized"
TOPIC_INVALIDOS = "events.invalid"
ESPERA_CONFIRMACION_S = 5

# acks=all + idempotencia: Kafka confirma solo cuando el mensaje quedó guardado,
# y si el productor reintenta no se escriben copias repetidas en el topic.
productor = Producer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 5,                 # junta mensajes 5 ms para enviarlos en bloque
    "compression.type": "lz4",
    "message.timeout.ms": 30000,    # tras 30 s sin Kafka, el mensaje se da por fallido
})


class KafkaNoDisponible(Exception):
    """Kafka no confirmó los mensajes a tiempo."""


def publicar(mensajes):
    """Publica una lista de (topic, clave, diccionario) y espera la confirmación.

    Lanza KafkaNoDisponible si algún mensaje no se pudo guardar.
    """
    errores = []

    def al_entregar(error, _mensaje):
        # Kafka llama a esta función por cada mensaje, con error=None si salió bien
        if error is not None:
            errores.append(error)

    for topic, clave, valor in mensajes:
        try:
            productor.produce(topic, key=clave, value=json.dumps(valor, ensure_ascii=False),
                              on_delivery=al_entregar)
        except BufferError:
            # La cola interna está llena: Kafka lleva rato sin responder
            raise KafkaNoDisponible("cola interna del productor llena")
        productor.poll(0)

    pendientes = productor.flush(ESPERA_CONFIRMACION_S)
    if pendientes > 0 or errores:
        raise KafkaNoDisponible(f"pendientes={pendientes} errores={len(errores)}")


def kafka_responde():
    """Pregunta a Kafka la lista de topics; sirve para /health."""
    try:
        productor.list_topics(timeout=2)
        return True
    except Exception:
        return False
