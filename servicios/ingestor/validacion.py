# Validación y estandarización de eventos (sección 5 del plan).
# La usa app.py: recibe un evento como diccionario y devuelve el evento
# estandarizado o el motivo por el que se rechaza.
# Los modelos de Pydantic revisan campos obligatorios, tipos y catálogos;
# las reglas que dependen de datos externos (sectores, hora actual) van aparte.

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field, ValidationError

# ------------------------------------------------------------------ Sectores
RUTA_SECTORES = os.getenv("SECTORES_PATH", "/config/sectores.json")
with open(RUTA_SECTORES, encoding="utf-8") as archivo:
    CONFIG = json.load(archivo)
ZONAS = {s["zone"] for s in CONFIG["sectores"]}
LIMITES = CONFIG["limites"]


# ------------------------------------------------------- Sobre común (5.1)
class Dispositivo(BaseModel):
    deviceId: str = Field(min_length=3)
    deviceType: Literal["panic_button", "lpr_camera", "speed_sensor", "acoustic_sensor", "citizen_app"]
    model: str
    firmwareVersion: str


class Ubicacion(BaseModel):
    zone: str
    zoneName: str
    city: str
    latitude: float
    longitude: float


class Metadatos(BaseModel):
    batteryLevel: Optional[int] = Field(default=None, ge=0, le=100)
    signalStrength: Optional[int] = None
    protocol: Optional[str] = None


class Sobre(BaseModel):
    schemaVersion: Literal["1.0"]
    eventId: UUID
    eventType: str
    timestamp: datetime
    emittedAt: datetime
    scenarioId: Optional[str] = None
    device: Dispositivo
    location: Ubicacion
    data: dict
    metadata: Optional[Metadatos] = None


# ------------------------------------------------- data por tipo (5.2)
class DatosPanico(BaseModel):
    alertType: Literal["PANICO", "EMERGENCIA_MEDICA", "INCENDIO"]
    origin: Literal["BOTON_FIJO", "APP_MOVIL"]
    userRef: Optional[str] = None


class Vehiculo(BaseModel):
    type: str
    color: str
    model: str


class DatosLPR(BaseModel):
    # 64 caracteres hexadecimales = HMAC-SHA256 de la placa
    plateHash: str = Field(pattern=r"^[0-9a-f]{64}$")
    platePartial: str
    estimatedSpeedKmh: float = Field(ge=0, le=300)
    direction: Literal["ENTRADA", "SALIDA"]
    vehicle: Vehiculo


class DatosVelocidad(BaseModel):
    speedKmh: float = Field(ge=0, le=300)
    limitKmh: int = Field(gt=0)
    headingDegrees: int = Field(ge=0, lt=360)
    anomalyType: Literal["NINGUNA", "EXCESO_VELOCIDAD", "TRAYECTORIA_ANOMALA"]


class DatosAcustico(BaseModel):
    soundType: Literal["DISPARO", "EXPLOSION", "VIDRIO_ROTO"]
    decibels: float = Field(ge=0, le=200)
    criticalProbability: float = Field(ge=0, le=1)


class DatosReporte(BaseModel):
    reportType: Literal["ACCIDENTE", "INCENDIO", "ALTERCADO"]
    message: Optional[str] = Field(default=None, max_length=280)
    origin: Literal["USUARIO", "APP", "PUNTO_FISICO"]
    accuracyMeters: Optional[int] = Field(default=None, ge=0)


# Cada eventType tiene su deviceType y su modelo de data
TIPOS = {
    "security.panic_button.pressed": ("panic_button", DatosPanico),
    "traffic.lpr.read": ("lpr_camera", DatosLPR),
    "traffic.speed.reading": ("speed_sensor", DatosVelocidad),
    "environment.acoustic.detected": ("acoustic_sensor", DatosAcustico),
    "citizen.report.created": ("citizen_app", DatosReporte),
}


def motivo_de_error(error, prefijo=""):
    """Convierte el primer error de Pydantic en un motivo corto y legible."""
    primero = error.errors()[0]
    campo = prefijo + ".".join(str(parte) for parte in primero["loc"])
    if campo.endswith("message"):
        return "MENSAJE_DEMASIADO_LARGO"
    if campo.endswith("plateHash"):
        return "PLATE_HASH_INVALIDO"
    return f"CAMPO_INVALIDO:{campo}"


def fecha_iso(fecha):
    """Formato estándar: ISO 8601 con milisegundos y zona horaria."""
    return fecha.isoformat(timespec="milliseconds")


def validar_evento(evento):
    """Devuelve (evento_estandarizado, None) si es válido o (None, motivo) si no."""
    if not isinstance(evento, dict):
        return None, "NO_ES_OBJETO_JSON"

    # Privacidad primero: una placa en claro nunca debe entrar al sistema
    datos = evento.get("data")
    if isinstance(datos, dict) and "plate" in datos:
        return None, "PLACA_EN_CLARO"

    try:
        sobre = Sobre.model_validate(evento)
    except ValidationError as error:
        return None, motivo_de_error(error)

    if sobre.eventType not in TIPOS:
        return None, "EVENT_TYPE_DESCONOCIDO"
    tipo_dispositivo, modelo_datos = TIPOS[sobre.eventType]
    if sobre.device.deviceType != tipo_dispositivo:
        return None, "DEVICE_TYPE_NO_CORRESPONDE"

    try:
        datos_validos = modelo_datos.model_validate(sobre.data)
    except ValidationError as error:
        return None, motivo_de_error(error, prefijo="data.")

    if sobre.location.zone not in ZONAS:
        return None, "ZONA_DESCONOCIDA"
    if not (LIMITES["latMin"] <= sobre.location.latitude <= LIMITES["latMax"]
            and LIMITES["lonMin"] <= sobre.location.longitude <= LIMITES["lonMax"]):
        return None, "COORDENADAS_FUERA_DE_LIMITES"

    # Una fecha sin zona horaria no se puede comparar de forma segura
    if sobre.timestamp.tzinfo is None:
        return None, "TIMESTAMP_SIN_ZONA_HORARIA"
    ahora = datetime.now(timezone.utc)
    if sobre.timestamp > ahora + timedelta(seconds=5):
        return None, "TIMESTAMP_EN_EL_FUTURO"
    if sobre.timestamp < ahora - timedelta(hours=24):
        return None, "TIMESTAMP_DEMASIADO_ANTIGUO"

    # Estandarizar: mismos campos, fechas con formato único y data limpia
    estandar = sobre.model_dump(mode="json")
    estandar["timestamp"] = fecha_iso(sobre.timestamp)
    estandar["emittedAt"] = fecha_iso(sobre.emittedAt)
    estandar["data"] = datos_validos.model_dump(mode="json")
    return estandar, None
