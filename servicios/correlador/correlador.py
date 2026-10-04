# Correlador: el "cerebro" del sistema. Consume events.standardized de Kafka,
# guarda cada evento en PostgreSQL (base_datos.py), actualiza el estado en
# Redis (estado_redis.py), aplica las reglas (reglas.py) y publica las
# alertas en correlated.alerts. Cada 10 s revisa los pánicos sin confirmar.
# Entrega al menos una vez: el lote se confirma en Kafka solo al final.

import json
import logging
import os
import signal
import time
import uuid
from datetime import datetime

from confluent_kafka import Consumer, Producer

import base_datos as db
import estado_redis as estado
import reglas

SLO_LATENCIA_MS = int(os.getenv("SLO_LATENCIA_MS", "2000"))
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TIPOS = {"security.panic_button.pressed": "panico", "traffic.lpr.read": "lpr", "traffic.speed.reading": "velocidad",
         "environment.acoustic.detected": "acustico", "citizen.report.created": "reporte"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s correlador: %(message)s")
log = logging.getLogger("correlador")

# Commit manual: Kafka no da por leído un mensaje hasta que lo digamos nosotros.
# session.timeout.ms=10 s: si el correlador muere sin avisar, Kafka libera sus
# particiones en 10 s (por defecto serían 45 s sin procesar nada).
consumidor = Consumer({"bootstrap.servers": KAFKA, "group.id": "correlador",
                       "enable.auto.commit": False, "auto.offset.reset": "earliest",
                       "session.timeout.ms": 10000})
productor = Producer({"bootstrap.servers": KAFKA, "acks": "all", "enable.idempotence": True, "linger.ms": 5})


def segundos(fecha_iso):
    return datetime.fromisoformat(fecha_iso).timestamp()


def ahora_iso():
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def resumir(e):
    """Versión compacta del evento para guardar en Redis (pasos 2 y 7)."""
    return {"id": e["eventId"], "tipo": TIPOS[e["eventType"]], "ts": segundos(e["timestamp"]),
            "emit": segundos(e["emittedAt"]), "zone": e["location"]["zone"], "lat": e["location"]["latitude"],
            "lon": e["location"]["longitude"], "dev": e["device"]["deviceId"], "esc": e.get("scenarioId"),
            "d": e["data"]}


def publicar_alerta(a, ultimo_emit, status="NUEVA", medir_slo=True):
    """Arma la alerta (formato 5.4), la guarda, la publica y revisa el SLO de latencia."""
    eventos = a["eventos"]
    escenarios = {e["esc"] for e in eventos}
    ultimo = max(eventos, key=lambda e: e["ts"])
    creada = time.time()
    alerta = {
        "alertId": str(uuid.uuid4()), "alertType": a["alertType"], "severity": a["severity"], "status": status,
        "zone": ultimo["zone"], "zoneName": reglas.SECTORES[ultimo["zone"]]["zoneName"],
        "latitude": ultimo["lat"], "longitude": ultimo["lon"], "score": a["score"], "rule": a["rule"],
        "description": a["description"], "sourceEventIds": [e["id"] for e in eventos],
        # El escenario solo se copia si todos los eventos vienen del mismo (sirve para evaluar)
        "scenarioId": escenarios.pop() if len(escenarios) == 1 else None,
        "createdAt": ahora_iso(),
        "latencyMs": int((creada - ultimo_emit) * 1000),
    }
    db.guardar_alerta(alerta)
    productor.produce("correlated.alerts", key=alerta["zone"], value=json.dumps(alerta, ensure_ascii=False))
    log.info("Alerta %s generada en %s (latencia %d ms)", alerta["alertType"], alerta["zoneName"], alerta["latencyMs"])
    if medir_slo and alerta["latencyMs"] > SLO_LATENCIA_MS:
        log.warning("SLO incumplido: alerta %s tardó %d ms (umbral %d ms)",
                    alerta["alertId"], alerta["latencyMs"], SLO_LATENCIA_MS)
        db.guardar_violacion_slo(alerta["alertId"], alerta["latencyMs"], SLO_LATENCIA_MS)


def procesar(evento):
    """Pasos 1 a 8 del diagrama del catedrático para un evento."""
    db.guardar_evento(evento)
    nuevo = resumir(evento)
    paso_anterior = None
    # Pasos 2 y 3: buscar el sector y el vehículo en Redis
    if nuevo["tipo"] == "lpr":
        paso_anterior = estado.ultimo_paso(nuevo["d"]["plateHash"])
    estado.agregar_a_ventana(nuevo)
    ventana = estado.eventos_en_ventana(nuevo["zone"], nuevo["ts"])
    if nuevo["tipo"] == "panico":
        estado.agregar_panico_pendiente(nuevo)

    # Pasos 4 a 6: correlacionar, calcular velocidad y aplicar reglas
    for alerta, sujeto in reglas.evaluar(nuevo, ventana, paso_anterior):
        if not estado.tomar_cooldown(alerta["rule"], sujeto):
            continue   # ya se avisó lo mismo hace poco
        ids = [e["id"] for e in alerta["eventos"]]
        estado.marcar_usados(alerta["alertType"], ids)
        for e in alerta["eventos"]:
            if e["tipo"] == "panico":
                estado.confirmar_panico(e["id"])   # este pánico ya no es falso positivo
        publicar_alerta(alerta, nuevo["emit"])

    # Paso 7: guardar el nuevo estado del vehículo (solo si es más reciente que el guardado,
    # porque al procesar un atraso pueden llegar antes lecturas nuevas de otra partición)
    if nuevo["tipo"] == "lpr" and (not paso_anterior or nuevo["ts"] > paso_anterior["ts"]):
        estado.guardar_paso(nuevo)


def revisar_panicos():
    for panico in estado.panicos_vencidos(time.time()):
        # La latencia del falso positivo se mide desde que cerró la ventana, no desde el pánico,
        # y no cuenta para el SLO: esperar la ventana completa es parte de la regla
        publicar_alerta(reglas.falso_positivo(panico), panico["ts"] + estado.VENTANA,
                        status="FALSO_POSITIVO_PROBABLE", medir_slo=False)


def detener(_senal, _marco):
    # docker stop envía SIGTERM: terminamos el lote actual y salimos ordenadamente
    global corriendo
    corriendo = False


corriendo = True


def main():
    signal.signal(signal.SIGTERM, detener)
    consumidor.subscribe(["events.standardized"])
    log.info("Correlador iniciado (ventana %d s, SLO %d ms)", estado.VENTANA, SLO_LATENCIA_MS)
    ultima_revision = time.time()
    procesados = 0
    while corriendo:
        # Espera corta: con poco tráfico no conviene esperar a juntar 500 (subiría la latencia)
        mensajes = consumidor.consume(num_messages=500, timeout=0.2)
        ids_lote = []
        for mensaje in mensajes:
            if mensaje.error():
                log.warning("Error de Kafka: %s", mensaje.error())
                continue
            evento = json.loads(mensaje.value())
            # Descartar duplicados (reintentos del simulador o de NGINX)
            if evento["eventId"] in ids_lote or estado.ya_procesado(evento["eventId"]):
                continue
            procesar(evento)
            ids_lote.append(evento["eventId"])

        if time.time() - ultima_revision >= 10:
            revisar_panicos()
            ultima_revision = time.time()
            log.info("Eventos procesados en los últimos 10 s: %d", procesados)
            procesados = 0

        # Orden importante: PostgreSQL, luego alertas a Kafka, luego Redis y al final el offset
        db.confirmar()
        productor.flush(10)
        if ids_lote:
            estado.marcar_procesados(ids_lote)
            procesados += len(ids_lote)
        if mensajes:
            consumidor.commit(asynchronous=False)

    # close() avisa a Kafka que nos vamos: reparte las particiones de inmediato
    consumidor.close()
    log.info("Correlador detenido ordenadamente")


if __name__ == "__main__":
    main()
