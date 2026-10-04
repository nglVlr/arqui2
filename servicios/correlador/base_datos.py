# Acceso a PostgreSQL del correlador (paso 8 del diagrama).
# correlador.py guarda aquí cada evento, cada alerta y las violaciones de SLO.
# Todo se hace en una transacción por lote: correlador.py llama a confirmar()
# antes de confirmar el lote en Kafka, así nunca se marca como leído algo
# que no quedó guardado.

import json
import os

import psycopg2
from psycopg2.extras import execute_values

conexion = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("APP_DB_USER"),
    password=os.getenv("APP_DB_PASSWORD"),
)
cursor = conexion.cursor()


# Los eventos se juntan aquí y se insertan todos juntos en confirmar():
# un solo INSERT por lote es mucho más rápido que uno por evento.
pendientes = []


def guardar_evento(e):
    pendientes.append(
        (e["eventId"], e["eventType"], e["location"]["zone"], e["device"]["deviceId"],
         e["device"]["deviceType"], e["location"]["latitude"], e["location"]["longitude"],
         e["timestamp"], e["emittedAt"], e["ingestion"]["ingestedAt"], e["ingestion"]["ingestorId"],
         e.get("scenarioId"), json.dumps(e["data"]), json.dumps(e.get("metadata"))))


def insertar_eventos_pendientes():
    if not pendientes:
        return
    # ON CONFLICT: si el evento ya estaba (reproceso tras una caída), no se duplica
    execute_values(
        cursor,
        """INSERT INTO events (event_id, event_type, zone, device_id, device_type, latitude, longitude,
                               occurred_at, emitted_at, ingested_at, ingestor_id,
                               scenario_id, data, metadata)
           VALUES %s ON CONFLICT (event_id) DO NOTHING""",
        pendientes,
    )
    pendientes.clear()


def guardar_alerta(a):
    cursor.execute(
        """INSERT INTO alerts (alert_id, alert_type, severity, status, zone, latitude, longitude, score,
                               rule, description, source_event_ids, scenario_id, created_at, latency_ms)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::uuid[], %s, %s, %s)""",
        (a["alertId"], a["alertType"], a["severity"], a["status"], a["zone"], a["latitude"],
         a["longitude"], a["score"], a["rule"], a["description"], a["sourceEventIds"],
         a["scenarioId"], a["createdAt"], a["latencyMs"]),
    )


def guardar_violacion_slo(alert_id, latencia_ms, umbral_ms):
    cursor.execute(
        "INSERT INTO slo_violations (alert_id, latency_ms, umbral_ms) VALUES (%s, %s, %s)",
        (alert_id, latencia_ms, umbral_ms),
    )


def confirmar():
    insertar_eventos_pendientes()
    conexion.commit()
