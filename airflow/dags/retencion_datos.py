# DAG retencion_datos (diario): aplica la política de retención de datos.
# Borra de PostgreSQL los eventos y eventos inválidos de más de 30 días y
# las alertas de más de 365 días (junto con sus envíos y violaciones de SLO).
# Es parte de la privacidad: no guardar ubicaciones ni hashes de placas
# más tiempo del necesario.

import logging
from datetime import datetime

from airflow.sdk import dag, task

from conexion import conectar

log = logging.getLogger(__name__)

# (descripción, consulta) en el orden en que se ejecutan
BORRADOS = [
    ("eventos de más de 30 días", "DELETE FROM events WHERE occurred_at < now() - interval '30 days'"),
    ("eventos inválidos de más de 30 días", "DELETE FROM invalid_events WHERE received_at < now() - interval '30 days'"),
    ("envíos de alertas de más de 365 días",
     "DELETE FROM dispatches WHERE alert_id IN (SELECT alert_id FROM alerts WHERE created_at < now() - interval '365 days')"),
    ("violaciones de SLO de más de 365 días",
     "DELETE FROM slo_violations WHERE alert_id IN (SELECT alert_id FROM alerts WHERE created_at < now() - interval '365 days')"),
    ("alertas de más de 365 días", "DELETE FROM alerts WHERE created_at < now() - interval '365 days'"),
]


@dag(schedule="@daily", start_date=datetime(2026, 10, 1), catchup=False, tags=["monitoreo-sjs"])
def retencion_datos():

    @task
    def borrar_datos_viejos():
        conexion = conectar()
        with conexion, conexion.cursor() as cursor:   # "with conexion" hace commit al terminar
            for descripcion, consulta in BORRADOS:
                cursor.execute(consulta)
                log.info("Borrados %d %s", cursor.rowcount, descripcion)
        conexion.close()

    borrar_datos_viejos()


retencion_datos()
