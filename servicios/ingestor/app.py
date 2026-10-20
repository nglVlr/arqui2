# API REST del ingestor (FastAPI). Hay 3 copias de este servicio detrás de NGINX.
# Recibe eventos de los simuladores, los valida con validacion.py y los publica
# en Kafka con kafka_productor.py: los válidos en events.standardized y los
# inválidos en events.invalid (dead-letter).
# Endpoints: POST /events, POST /events/batch, GET /health.

import copy
import logging
import os
import time
from datetime import datetime, timezone

from fastapi import Body, FastAPI, Response
from fastapi.responses import JSONResponse

from kafka_productor import TOPIC_INVALIDOS, TOPIC_VALIDOS, KafkaNoDisponible, kafka_responde, publicar
from validacion import fecha_iso, validar_evento

INGESTOR_ID = os.getenv("INGESTOR_ID", "ingestor-x")
MAXIMO_LOTE = 500

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s " + INGESTOR_ID + ": %(message)s")
log = logging.getLogger("ingestor")

app = FastAPI(title=f"Ingestor {INGESTOR_ID}")

# Contadores para un resumen en el log cada 10 s (un log por petición
# sería demasiado ruido durante las pruebas de carga)
contadores = {"aceptados": 0, "rechazados": 0, "ultimo_log": time.time()}


def registrar_resumen(aceptados, rechazados):
    contadores["aceptados"] += aceptados
    contadores["rechazados"] += rechazados
    if time.time() - contadores["ultimo_log"] >= 10:
        log.info("Últimos 10 s: %d eventos aceptados, %d rechazados",
                 contadores["aceptados"], contadores["rechazados"])
        contadores.update(aceptados=0, rechazados=0, ultimo_log=time.time())


def redactar_placa(evento):
    """Copia del evento con data.plate reemplazada por "REDACTADA".
    Privacidad: ni los eventos rechazados deben guardar la placa real, porque
    la dead-letter termina en Kafka (events.invalid) y en PostgreSQL
    (invalid_events). Se trabaja sobre una copia para no tocar el evento recibido."""
    copia = copy.deepcopy(evento)
    copia["data"]["plate"] = "REDACTADA"
    return copia


def procesar(eventos):
    """Valida una lista de eventos, los publica en Kafka y arma la respuesta HTTP."""
    ahora = fecha_iso(datetime.now(timezone.utc).astimezone())
    mensajes = []
    motivos = []
    for evento in eventos:
        estandar, motivo = validar_evento(evento)
        if estandar is not None:
            # Trazabilidad: quién y cuándo recibió el evento
            estandar["ingestion"] = {"ingestedAt": ahora, "ingestorId": INGESTOR_ID}
            mensajes.append((TOPIC_VALIDOS, estandar["location"]["zone"], estandar))
        else:
            zona = "sin-zona"
            if isinstance(evento, dict) and isinstance(evento.get("location"), dict):
                zona = str(evento["location"].get("zone") or "sin-zona")
            original = redactar_placa(evento) if motivo == "PLACA_EN_CLARO" else evento
            invalido = {"reason": motivo, "ingestorId": INGESTOR_ID, "receivedAt": ahora, "original": original}
            mensajes.append((TOPIC_INVALIDOS, zona, invalido))
            motivos.append(motivo)

    try:
        publicar(mensajes)
    except KafkaNoDisponible as error:
        log.warning("Kafka no disponible, se responde 503: %s", error)
        return JSONResponse(status_code=503, headers={"X-Ingestor-Id": INGESTOR_ID},
                            content={"error": "KAFKA_NO_DISPONIBLE", "ingestorId": INGESTOR_ID})

    aceptados = len(eventos) - len(motivos)
    registrar_resumen(aceptados, len(motivos))
    return JSONResponse(headers={"X-Ingestor-Id": INGESTOR_ID}, content={
        "ingestorId": INGESTOR_ID,
        "aceptados": aceptados,
        "rechazados": len(motivos),
        "motivos": motivos,
    })


# Los endpoints son "def" normales: FastAPI los ejecuta en un grupo de hilos,
# así una petición que espera a Kafka no bloquea a las demás.
@app.post("/events")
def recibir_evento(evento: dict = Body(...)):
    return procesar([evento])


@app.post("/events/batch")
def recibir_lote(eventos: list = Body(...)):
    if len(eventos) > MAXIMO_LOTE:
        return JSONResponse(status_code=413, content={"error": f"El lote supera {MAXIMO_LOTE} eventos"})
    return procesar(eventos)


@app.get("/health")
def salud(response: Response):
    kafka_ok = kafka_responde()
    response.headers["X-Ingestor-Id"] = INGESTOR_ID
    if not kafka_ok:
        response.status_code = 503
    return {"ingestorId": INGESTOR_ID, "kafka": "ok" if kafka_ok else "sin conexion"}
