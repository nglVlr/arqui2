# Generadores de eventos simulados: uno por tipo de sensor (sección 5.2).
# Los usan simulador.py (tráfico normal y pico), escenarios.py y reportar.py.
# Cada función devuelve un diccionario con el formato del contrato de eventos.
# Aquí también se calcula el HMAC de las placas: la "cámara" nunca envía la
# placa en claro, solo su hash y una versión parcial para el operador.

import hashlib
import hmac
import json
import math
import os
import random
import uuid
from datetime import datetime

with open(os.getenv("SECTORES_PATH", "/config/sectores.json"), encoding="utf-8") as archivo:
    CONFIG = json.load(archivo)
SECTORES = {s["zone"]: s for s in CONFIG["sectores"]}
CIUDAD = CONFIG["city"]
CLAVE_HMAC = os.getenv("PLATE_HMAC_KEY", "").encode()

CONSONANTES = "BCDFGHJKLMNPQRSTVWXYZ"
MODELOS = {"panic_button": "SafeCity-PB100", "lpr_camera": "PlateEye-L4",
           "speed_sensor": "RadarSJ-200", "acoustic_sensor": "ShotSpot-A3", "citizen_app": "AppVecino-2"}
PREFIJOS = {"panic_button": "PAN", "lpr_camera": "LPR", "speed_sensor": "VEL",
            "acoustic_sensor": "ACU", "citizen_app": "REP"}
VEHICULOS = [("SEDAN", "COROLLA"), ("PICKUP", "HILUX"), ("MOTO", "GN125"), ("SUV", "RAV4"), ("MICROBUS", "HIACE")]
COLORES = ["GRIS", "BLANCO", "NEGRO", "ROJO", "AZUL"]
MENSAJES = {
    "ACCIDENTE": ["Choque entre moto y pickup frente a la gasolinera", "Carro volcado en la curva", "Atropello cerca del mercado"],
    "INCENDIO": ["Sale humo de una casa", "Se quema un terreno baldío", "Fuego en una bodega"],
    "ALTERCADO": ["Pelea en la parada de bus", "Discusión fuerte en la calle", "Grupo peleando afuera de la cantina"],
}


# ------------------------------------------------------------- Utilidades
def ahora_iso():
    """Hora local con milisegundos y zona, ej. 2026-10-28T22:05:13.412-06:00."""
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def elegir_sector():
    """Elige un sector al azar según su peso (los más poblados salen más)."""
    zonas = list(SECTORES)
    return random.choices(zonas, weights=[SECTORES[z]["peso"] for z in zonas])[0]


def punto_aleatorio(zona):
    """Un punto al azar dentro del radio del sector, para que el mapa de calor se vea natural."""
    sector = SECTORES[zona]
    distancia = sector["radio_m"] * math.sqrt(random.random())   # sqrt reparte parejo en el círculo
    angulo = random.uniform(0, 2 * math.pi)
    lat = sector["lat"] + (distancia * math.cos(angulo)) / 111320
    lon = sector["lon"] + (distancia * math.sin(angulo)) / (111320 * math.cos(math.radians(sector["lat"])))
    return round(lat, 6), round(lon, 6)


def placa_aleatoria():
    """Placa sintética guatemalteca: letra de tipo, 3 dígitos, 3 consonantes (ej. P123BCD)."""
    return random.choice("PMC") + f"{random.randint(0, 999):03d}" + "".join(random.choices(CONSONANTES, k=3))


def hash_placa(placa):
    """HMAC-SHA256 de la placa: sin la clave nadie puede recuperar la placa original."""
    return hmac.new(CLAVE_HMAC, placa.encode(), hashlib.sha256).hexdigest()


def placa_parcial(placa):
    return f"{placa[0]}-***{placa[-3:]}"


# ------------------------------------------------------------ Sobre común
def crear_evento(event_type, device_type, zona, data, escenario=None, centro=False, numero=None):
    """Arma el evento completo. centro=True usa el centro exacto del sector (escenarios)."""
    sector = SECTORES[zona]
    lat, lon = (sector["lat"], sector["lon"]) if centro else punto_aleatorio(zona)
    numero = numero or random.randint(1, 20)
    return {
        "schemaVersion": "1.0",
        "eventId": str(uuid.uuid4()),
        "eventType": event_type,
        "timestamp": ahora_iso(),
        "emittedAt": None,   # se llena justo antes de enviar (mide la latencia real)
        "scenarioId": escenario,
        "device": {
            "deviceId": f"{PREFIJOS[device_type]}-{zona.replace('-', '')}-{numero:03d}",
            "deviceType": device_type,
            "model": MODELOS[device_type],
            "firmwareVersion": "1.2.0",
        },
        "location": {"zone": zona, "zoneName": sector["zoneName"], "city": CIUDAD, "latitude": lat, "longitude": lon},
        "data": data,
        "metadata": {"batteryLevel": random.randint(20, 100), "signalStrength": random.randint(-95, -50),
                     "protocol": random.choice(["MQTT", "HTTP", "LoRaWAN"])},
    }


# ---------------------------------------------------- Un generador por tipo
def evento_panico(zona, tipo=None, escenario=None, centro=False):
    tipo = tipo or random.choices(["PANICO", "EMERGENCIA_MEDICA", "INCENDIO"], weights=[80, 15, 5])[0]
    origen = random.choice(["BOTON_FIJO", "APP_MOVIL"])
    # El userRef es un seudónimo y solo existe si el pánico viene de la app
    usuario = f"u_{uuid.uuid4().hex[:6]}" if origen == "APP_MOVIL" else None
    data = {"alertType": tipo, "origin": origen, "userRef": usuario}
    return crear_evento("security.panic_button.pressed", "panic_button", zona, data, escenario, centro)


def evento_lpr(zona, placa=None, velocidad=None, direccion=None, escenario=None, centro=False, numero=None):
    placa = placa or placa_aleatoria()
    limite = SECTORES[zona]["limite_kmh"]
    if velocidad is None:
        # En tráfico normal casi nadie pasa del límite (0.5 %), para no generar alertas por azar
        velocidad = random.randint(limite + 5, limite + 30) if random.random() < 0.005 else random.randint(15, limite)
    tipo, modelo = random.choice(VEHICULOS)
    data = {
        "plateHash": hash_placa(placa),
        "platePartial": placa_parcial(placa),
        "estimatedSpeedKmh": velocidad,
        "direction": direccion or random.choice(["ENTRADA", "SALIDA"]),
        "vehicle": {"type": tipo, "color": random.choice(COLORES), "model": modelo},
    }
    return crear_evento("traffic.lpr.read", "lpr_camera", zona, data, escenario, centro, numero)


def evento_velocidad(zona, velocidad=None, anomalia=None, escenario=None, centro=False):
    limite = SECTORES[zona]["limite_kmh"]
    if anomalia is None:
        anomalia = random.choices(["NINGUNA", "EXCESO_VELOCIDAD", "TRAYECTORIA_ANOMALA"], weights=[993, 5, 2])[0]
    if velocidad is None:
        velocidad = random.randint(limite + 10, limite + 50) if anomalia == "EXCESO_VELOCIDAD" else random.randint(10, limite)
    data = {"speedKmh": velocidad, "limitKmh": limite, "headingDegrees": random.randint(0, 359), "anomalyType": anomalia}
    return crear_evento("traffic.speed.reading", "speed_sensor", zona, data, escenario, centro)


def evento_acustico(zona, sonido=None, probabilidad=None, escenario=None, centro=False):
    sonido = sonido or random.choices(["DISPARO", "EXPLOSION", "VIDRIO_ROTO"], weights=[20, 10, 70])[0]
    if probabilidad is None:
        probabilidad = round(random.uniform(0.2, 0.75), 2)   # casi siempre por debajo de 0.7
    decibeles = {"DISPARO": 125, "EXPLOSION": 135, "VIDRIO_ROTO": 95}[sonido] + random.randint(-10, 10)
    data = {"soundType": sonido, "decibels": decibeles, "criticalProbability": probabilidad}
    return crear_evento("environment.acoustic.detected", "acoustic_sensor", zona, data, escenario, centro)


def evento_reporte(zona, tipo=None, mensaje=None, origen=None, escenario=None, centro=False):
    tipo = tipo or random.choice(["ACCIDENTE", "INCENDIO", "ALTERCADO"])
    data = {
        "reportType": tipo,
        "message": mensaje if mensaje is not None else random.choice(MENSAJES[tipo]),
        "origin": origen or random.choice(["USUARIO", "APP", "PUNTO_FISICO"]),
        "accuracyMeters": random.choice([10, 50, 150, 300]),
    }
    return crear_evento("citizen.report.created", "citizen_app", zona, data, escenario, centro)


# --------------------------------------------------- Eventos inválidos
def volver_invalido(evento):
    """Daña un evento a propósito para probar la validación y el dead-letter."""
    motivo = random.choice(["placa", "zona", "coordenadas", "futuro", "campo", "mensaje", "hash"])
    if motivo == "placa":
        evento["data"]["plate"] = placa_aleatoria()          # placa en claro: PLACA_EN_CLARO
    elif motivo == "zona":
        evento["location"]["zone"] = "SJS-99"
    elif motivo == "coordenadas":
        evento["location"]["latitude"] = 15.5
    elif motivo == "futuro":
        evento["timestamp"] = "2099-01-01T00:00:00.000-06:00"
    elif motivo == "campo":
        del evento["device"]
    elif motivo == "mensaje" and "reportType" in evento["data"]:
        evento["data"]["message"] = "x" * 300
    elif motivo == "hash" and "plateHash" in evento["data"]:
        evento["data"]["plateHash"] = "no-es-un-hash"
    else:
        evento["schemaVersion"] = "9.9"
    return evento
