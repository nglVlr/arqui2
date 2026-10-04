# DAG analisis_falsos_positivos (cada hora): calcula por sector, en las
# últimas 24 horas, cuántos pánicos fueron confirmados por una regla
# (usados en ROBO_EN_CURSO, ATAQUE_ARMADO o INCENDIO_CONFIRMADO) y cuántos
# terminaron como PANICO_NO_CONFIRMADO. Guarda la proporción en
# false_positive_stats para el dashboard 3 (mérito adicional del enunciado).

import logging
from datetime import datetime

from airflow.sdk import dag, task

from conexion import conectar

log = logging.getLogger(__name__)

CONSULTA = """
    WITH confirmados AS (
        -- Pánicos que aparecen como evento de origen de una alerta confirmada
        SELECT e.zone, count(DISTINCT e.event_id) AS n
        FROM alerts a
        CROSS JOIN unnest(a.source_event_ids) AS origen(event_id)
        JOIN events e ON e.event_id = origen.event_id
        WHERE a.alert_type <> 'PANICO_NO_CONFIRMADO'
          AND e.event_type = 'security.panic_button.pressed'
          AND a.created_at > now() - interval '24 hours'
        GROUP BY e.zone
    ),
    no_confirmados AS (
        SELECT zone, count(*) AS n
        FROM alerts
        WHERE alert_type = 'PANICO_NO_CONFIRMADO' AND created_at > now() - interval '24 hours'
        GROUP BY zone
    )
    INSERT INTO false_positive_stats (zone, panicos_confirmados, panicos_no_confirmados, proporcion_fp)
    SELECT z.zone,
           coalesce(c.n, 0),
           coalesce(n.n, 0),
           -- proporción de falsos positivos; 0 si no hubo pánicos
           coalesce(n.n, 0)::numeric / greatest(coalesce(c.n, 0) + coalesce(n.n, 0), 1)
    FROM zones z
    LEFT JOIN confirmados c ON c.zone = z.zone
    LEFT JOIN no_confirmados n ON n.zone = z.zone
"""


@dag(schedule="@hourly", start_date=datetime(2026, 10, 1), catchup=False, tags=["monitoreo-sjs"])
def analisis_falsos_positivos():

    @task
    def calcular_proporciones():
        conexion = conectar()
        with conexion, conexion.cursor() as cursor:
            cursor.execute(CONSULTA)
            log.info("Análisis de falsos positivos: %d sectores calculados", cursor.rowcount)
        conexion.close()

    calcular_proporciones()


analisis_falsos_positivos()
