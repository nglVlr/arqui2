# Envío de eventos al sistema por HTTP (a NGINX, que reparte a los ingestores).
# Lo usan simulador.py, escenarios.py y reportar.py.
# Si un envío falla, reintenta con espera creciente (1, 2, 4, 8 s) hasta 5 intentos;
# si aun así falla, cuenta el lote como perdido. Lleva contadores para el resumen final.

import logging
import os
import threading
import time

import requests

from generadores import ahora_iso

URL = os.getenv("INGESTA_URL", "http://nginx")
INTENTOS = 5

log = logging.getLogger("envio")
# Una sesión reutiliza la conexión TCP entre envíos (más rápido); una por hilo
sesiones = threading.local()
candado = threading.Lock()
contadores = {"enviados": 0, "aceptados": 0, "rechazados": 0, "perdidos": 0, "reintentos": 0}


def sumar(campo, cantidad):
    # Varios hilos envían a la vez: el candado evita que se pisen los contadores
    with candado:
        contadores[campo] += cantidad


def sesion():
    if not hasattr(sesiones, "s"):
        sesiones.s = requests.Session()
    return sesiones.s


def enviar(eventos):
    """Envía una lista de eventos a /events/batch. Devuelve la respuesta JSON o None si se perdió."""
    if not eventos:
        return None
    # emittedAt = momento real de envío; el correlador lo usa para medir la latencia
    momento = ahora_iso()
    for evento in eventos:
        if isinstance(evento, dict) and evento.get("emittedAt") is None:
            evento["emittedAt"] = momento

    espera = 1
    for intento in range(1, INTENTOS + 1):
        try:
            respuesta = sesion().post(f"{URL}/events/batch", json=eventos, timeout=15)
            if respuesta.status_code == 200:
                cuerpo = respuesta.json()
                sumar("enviados", len(eventos))
                sumar("aceptados", cuerpo["aceptados"])
                sumar("rechazados", cuerpo["rechazados"])
                return cuerpo
            motivo = f"HTTP {respuesta.status_code}"
        except requests.RequestException as error:
            motivo = type(error).__name__
        if intento < INTENTOS:
            log.warning("Envío fallido (%s), intento %d de %d, reintento en %d s", motivo, intento, INTENTOS, espera)
            sumar("reintentos", 1)
            time.sleep(espera)
            espera *= 2

    log.error("Lote de %d eventos PERDIDO después de %d intentos", len(eventos), INTENTOS)
    sumar("enviados", len(eventos))
    sumar("perdidos", len(eventos))
    return None


def imprimir_resumen(segundos):
    c = contadores
    print("\n========== Resumen de la simulación ==========")
    print(f"Duración:            {segundos:.1f} s")
    print(f"Eventos enviados:    {c['enviados']}")
    print(f"Aceptados:           {c['aceptados']}")
    print(f"Rechazados:          {c['rechazados']}"
          f" ({100 * c['rechazados'] / max(c['enviados'], 1):.2f} %)")
    print(f"Perdidos:            {c['perdidos']}")
    print(f"Reintentos:          {c['reintentos']}")
    print(f"Eventos por segundo: {c['enviados'] / max(segundos, 0.001):.1f}")
    print("==============================================")
