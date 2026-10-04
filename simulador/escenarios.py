# Escenarios guionados para la demo y la evaluación (sección 10 del plan).
# Cada escenario es una lista de pasos: (segundos de espera, función que crea el evento).
# simulador.py llama a ejecutar(); evaluar.py usa ESPERADAS como verdad esperada.
# Con "todos", cada escenario corre en su propio hilo, a la vez, como en la vida real.
# --velocidad divide todas las esperas (velocidad 2 = el doble de rápido).

import logging
import threading
import time

import envio
from cargar_buscados import PLACAS_BUSCADAS
from generadores import evento_acustico, evento_lpr, evento_panico, evento_reporte, evento_velocidad

log = logging.getLogger("escenarios")

# Alerta esperada de cada escenario (la usa evaluar.py)
ESPERADAS = {
    "ESC-ROBO-01": "ROBO_EN_CURSO",
    "ESC-ARMADO-01": "ATAQUE_ARMADO",
    "ESC-ACCIDENTE-01": "ACCIDENTE_VIAL",
    "ESC-INCENDIO-01": "INCENDIO_CONFIRMADO",
    "ESC-TRAMO-01": "VELOCIDAD_ENTRE_CAMARAS",
    "ESC-ESCOLAR-01": "ZONA_ESCOLAR",
    "ESC-BUSCADO-01": "VEHICULO_REPORTADO",
    "ESC-FALSO-01": "PANICO_NO_CONFIRMADO",
    "ESC-RUIDO-01": "PANICO_NO_CONFIRMADO",
}

# Placa del escenario de tramo (sintética, no está en la lista de buscados)
PLACA_TRAMO = "P482KTV"
# SJS-04 a SJS-02 hay unos 3.33 km entre centros: 120 s equivalen a ~100 km/h
SEGUNDOS_TRAMO = 120


def pasos(nombre):
    """Devuelve la lista de pasos (espera, función) del escenario. e = escenarioId."""
    e = nombre
    guiones = {
        "ESC-ROBO-01": [
            (0, lambda: evento_panico("SJS-05", "PANICO", e, centro=True)),
            (40, lambda: evento_lpr("SJS-05", velocidad=85, direccion="SALIDA", escenario=e, centro=True)),
            (20, lambda: evento_velocidad("SJS-05", 92, "EXCESO_VELOCIDAD", e, centro=True)),
        ],
        "ESC-ARMADO-01": [
            (0, lambda: evento_acustico("SJS-01", "DISPARO", 0.9, e, centro=True)),
            (30, lambda: evento_panico("SJS-01", "PANICO", e, centro=True)),
        ],
        "ESC-ACCIDENTE-01": [
            (0, lambda: evento_velocidad("SJS-04", 55, "TRAYECTORIA_ANOMALA", e, centro=True)),
            (60, lambda: evento_reporte("SJS-04", "ACCIDENTE", "Pickup se salió de la carretera y chocó contra un poste", "APP", e, centro=True)),
        ],
        "ESC-INCENDIO-01": [
            (0, lambda: evento_panico("SJS-06", "INCENDIO", e, centro=True)),
            (45, lambda: evento_reporte("SJS-06", "INCENDIO", "Sale humo negro de una casa en la esquina", "USUARIO", e, centro=True)),
        ],
        "ESC-TRAMO-01": [
            # Velocidad legal en cada cámara y llegada como ENTRADA a Sajcavillá (donde corre
            # ESC-RUIDO-01 con un pánico): así solo debe dispararse la regla R05
            (0, lambda: evento_lpr("SJS-04", PLACA_TRAMO, 55, "SALIDA", e, centro=True, numero=1)),
            (SEGUNDOS_TRAMO, lambda: evento_lpr("SJS-02", PLACA_TRAMO, 45, "ENTRADA", e, centro=True, numero=1)),
        ],
        "ESC-ESCOLAR-01": [
            (0, lambda: evento_velocidad("SJS-01", 45, "EXCESO_VELOCIDAD", e, centro=True)),
        ],
        "ESC-BUSCADO-01": [
            (0, lambda: evento_lpr("SJS-03", PLACAS_BUSCADAS[0], 35, "ENTRADA", e, centro=True)),
        ],
        "ESC-FALSO-01": [
            (0, lambda: evento_panico("SJS-07", "PANICO", e, centro=True)),
        ],
        "ESC-RUIDO-01": [
            (0, lambda: evento_panico("SJS-02", "PANICO", e, centro=True)),
            (10, lambda: evento_acustico("SJS-02", "VIDRIO_ROTO", 0.4, e, centro=True)),
        ],
    }
    return guiones[nombre]


def correr(nombre, velocidad):
    """Ejecuta un escenario paso a paso, esperando entre eventos."""
    log.info("Inicia %s", nombre)
    for espera, crear in pasos(nombre):
        time.sleep(espera / velocidad)
        evento = crear()
        envio.enviar([evento])
        log.info("%s: enviado %s (%s)", nombre, evento["eventType"], evento["location"]["zone"])
    log.info("Termina %s (alerta esperada: %s)", nombre, ESPERADAS[nombre])


def ejecutar(nombre, velocidad=1):
    nombres = list(ESPERADAS) if nombre == "todos" else [nombre]
    for n in nombres:
        if n not in ESPERADAS:
            raise SystemExit(f"Escenario desconocido: {n}. Opciones: {', '.join(ESPERADAS)} o todos")
    inicio = time.time()
    hilos = [threading.Thread(target=correr, args=(n, velocidad)) for n in nombres]
    for hilo in hilos:
        hilo.start()
    for hilo in hilos:
        hilo.join()
    envio.imprimir_resumen(time.time() - inicio)
    print("Los falsos positivos (PANICO_NO_CONFIRMADO) aparecen cuando se cierra la ventana de")
    print("correlación. Espera ese tiempo y luego ejecuta: python evaluar.py")
