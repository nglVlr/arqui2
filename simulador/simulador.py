# Simulador principal de sensores de San Juan Sacatepéquez.
# Modos:
#   python simulador.py normal [--duracion S]                    tráfico continuo (sección 9)
#   python simulador.py pico --multiplicador 20 --duracion 60    prueba de carga
#   python simulador.py escenario <nombre|todos> [--velocidad N] escenarios guionados
# Genera eventos con generadores.py y los envía con envio.py a NGINX.

import argparse
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import envio
import escenarios
from generadores import (elegir_sector, evento_acustico, evento_lpr, evento_panico,
                         evento_reporte, evento_velocidad, volver_invalido)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("simulador")

TAMANO_LOTE = 50
PORCENTAJE_INVALIDOS = 0.015   # entre 1 y 2 % de eventos dañados a propósito

# Tasas por segundo en todo el municipio (sección 9 del plan)
TASAS = {
    "lpr": 3,
    "velocidad": 2,
    "acustico": 2 / 60,
    "panico": 1 / 60,
    "reporte": 2 / 60,
}
GENERADORES = {
    "lpr": evento_lpr, "velocidad": evento_velocidad, "acustico": evento_acustico,
    "panico": evento_panico, "reporte": evento_reporte,
}


def factor_hora(tipo, hora):
    """Más tráfico en horas pico; más pánico y disparos de noche."""
    if tipo in ("lpr", "velocidad") and (6 <= hora < 9 or 17 <= hora < 20):
        return 1.5
    if tipo in ("panico", "acustico") and (hora >= 19 or hora < 2):
        return 1.5
    return 1.0


def cuantos(tasa):
    """Cantidad de eventos en este segundo: la parte entera más una moneda al aire por la fracción."""
    entera = int(tasa)
    return entera + (1 if random.random() < tasa - entera else 0)


def eventos_de_un_segundo(multiplicador):
    hora = datetime.now().hour
    eventos = []
    for tipo, tasa in TASAS.items():
        for _ in range(cuantos(tasa * multiplicador * factor_hora(tipo, hora))):
            evento = GENERADORES[tipo](elegir_sector())
            if random.random() < PORCENTAJE_INVALIDOS:
                evento = volver_invalido(evento)
            eventos.append(evento)
    return eventos


def trafico(multiplicador, duracion, hilos):
    """Genera eventos cada segundo y los envía en lotes de hasta 50 con varios hilos."""
    log.info("Tráfico con multiplicador %s durante %s s (Ctrl+C para detener)", multiplicador, duracion or "infinitos")
    inicio = time.time()
    with ThreadPoolExecutor(max_workers=hilos) as ejecutor:
        try:
            segundo = 0
            while not duracion or segundo < duracion:
                eventos = eventos_de_un_segundo(multiplicador)
                # Se manda al menos una vez por segundo, aunque el lote no llegue a 50
                for i in range(0, len(eventos), TAMANO_LOTE):
                    ejecutor.submit(envio.enviar, eventos[i:i + TAMANO_LOTE])
                segundo += 1
                if segundo % 10 == 0:
                    log.info("%d s, %d eventos enviados", segundo, envio.contadores["enviados"])
                # Dormir hasta el próximo segundo exacto (si generar tardó, se duerme menos)
                time.sleep(max(0, inicio + segundo - time.time()))
        except KeyboardInterrupt:
            log.info("Detenido por el usuario")
    envio.imprimir_resumen(time.time() - inicio)


def main():
    parser = argparse.ArgumentParser(description="Simulador de sensores San Juan Sacatepéquez")
    parser.add_argument("modo", choices=["normal", "pico", "escenario"])
    parser.add_argument("nombre", nargs="?", help="Nombre del escenario o 'todos'")
    parser.add_argument("--multiplicador", type=float, default=20)
    parser.add_argument("--duracion", type=int, default=0, help="Segundos (0 = sin fin en modo normal)")
    parser.add_argument("--velocidad", type=float, default=1, help="Acelera los escenarios")
    parser.add_argument("--hilos", type=int, default=8, help="Envíos en paralelo")
    args = parser.parse_args()

    if args.modo == "normal":
        trafico(1, args.duracion, args.hilos)
    elif args.modo == "pico":
        trafico(args.multiplicador, args.duracion or 60, args.hilos)
    else:
        escenarios.ejecutar(args.nombre or "todos", args.velocidad)


if __name__ == "__main__":
    main()
