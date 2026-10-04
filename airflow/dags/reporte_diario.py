# DAG reporte_diario (diario y ejecutable a mano): resume las alertas por
# día, sector, tipo y severidad en la tabla daily_stats, que muestra el
# dashboard 3 de Grafana. Recalcula ayer (día ya cerrado) y hoy (día en curso);
# si se ejecuta varias veces, actualiza las filas en lugar de duplicarlas.

import logging
from datetime import datetime

from airflow.sdk import dag, task

from conexion import conectar

log = logging.getLogger(__name__)

# Las fechas se calculan en hora de Guatemala, no en UTC
CONSULTA = """
    INSERT INTO daily_stats (fecha, zone, alert_type, severity, cantidad)
    SELECT (created_at AT TIME ZONE 'America/Guatemala')::date, zone, alert_type, severity, count(*)
    FROM alerts
    WHERE (created_at AT TIME ZONE 'America/Guatemala')::date
          >= (now() AT TIME ZONE 'America/Guatemala')::date - 1
    GROUP BY 1, 2, 3, 4
    ON CONFLICT (fecha, zone, alert_type, severity) DO UPDATE SET cantidad = EXCLUDED.cantidad
"""


@dag(schedule="@daily", start_date=datetime(2026, 10, 1), catchup=False, tags=["monitoreo-sjs"])
def reporte_diario():

    @task
    def resumir_alertas():
        conexion = conectar()
        with conexion, conexion.cursor() as cursor:
            cursor.execute(CONSULTA)
            log.info("Reporte diario: %d filas insertadas o actualizadas en daily_stats", cursor.rowcount)
        conexion.close()

    resumir_alertas()


reporte_diario()
