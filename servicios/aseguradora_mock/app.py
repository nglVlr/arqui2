# Aseguradora simulada (mock): representa a una entidad externa que recibe
# avisos de accidentes viales por REST. El despachador le envía cada alerta
# ACCIDENTE_VIAL a POST /siniestros; aquí solo se registra en el log y se
# responde con un número de caso, como haría el sistema real de la aseguradora.

import itertools
import logging

from fastapi import Body, FastAPI

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s aseguradora: %(message)s")
log = logging.getLogger("aseguradora")

app = FastAPI(title="Aseguradora mock")
numeros = itertools.count(1)   # contador de casos (se reinicia si el contenedor reinicia)


@app.post("/siniestros")
def registrar_siniestro(alerta: dict = Body(...)):
    caso = f"SIN-2026-{next(numeros):06d}"
    log.info("Siniestro %s recibido: %s en %s (alerta %s)", caso, alerta.get("description"),
             alerta.get("zoneName"), alerta.get("alertId"))
    return {"numeroCaso": caso, "estado": "RECIBIDO"}


@app.get("/health")
def salud():
    return {"estado": "ok"}
