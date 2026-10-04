# Estado del correlador guardado en Redis (pasos 2, 3 y 7 del diagrama).
# Lo usan correlador.py y reglas.py. Redis guarda:
#   evento:{id}              eventos ya procesados (descartar duplicados)
#   ventana:{zone}:{tipo}    eventos recientes del sector (sorted set, score = hora del evento)
#   vehiculo:{plateHash}     último paso de un vehículo por una cámara (regla R05)
#   panicos_pendientes       pánicos que aún no tienen confirmación (falsos positivos)
#   usado:{tipo}:{id}        eventos ya usados en una alerta de ese tipo
#   cooldown:{regla}:{x}     evita repetir la misma alerta durante la ventana
#   placas_buscadas          hashes de placas robadas (los carga cargar_buscados.py)

import json
import os

import redis

VENTANA = int(os.getenv("CORRELATION_WINDOW_SECONDS", "300"))
TTL_VEHICULO = 3600   # un paso de vehículo sirve para R05 durante 1 hora

r = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=6379, decode_responses=True)


# ------------------------------------------------------------ Duplicados
def ya_procesado(event_id):
    return r.exists(f"evento:{event_id}") == 1


def marcar_procesados(ids):
    """Se llama DESPUÉS de guardar en PostgreSQL: si el proceso se cae antes,
    el evento se vuelve a procesar en lugar de perderse."""
    tuberia = r.pipeline()
    for event_id in ids:
        tuberia.set(f"evento:{event_id}", 1, ex=3600)
    tuberia.execute()


# ------------------------------------------------------ Ventana por sector
# Una ventana por sector y por tipo de evento. Al leer, solo se traen los
# MAXIMO_POR_TIPO más recientes de cada tipo: con mucha carga una ventana de
# 60 s puede tener miles de eventos, y leerlos todos con cada evento nuevo
# hacía al correlador cada vez más lento (medido en la Fase 8: 66 eventos/s).
TIPOS_VENTANA = ["panico", "lpr", "velocidad", "acustico", "reporte"]
MAXIMO_POR_TIPO = 20


def es_relevante(resumen):
    """Solo entran a la ventana los eventos que alguna regla busca en ella.
    Las lecturas normales de velocidad y las entradas de LPR no se necesitan
    (R05, R06 y R07 usan el evento nuevo, el vehículo o la lista de buscados)."""
    if resumen["tipo"] == "velocidad":
        return resumen["d"]["anomalyType"] != "NINGUNA"
    if resumen["tipo"] == "lpr":
        return resumen["d"]["direction"] == "SALIDA"
    return True


def agregar_a_ventana(resumen):
    if not es_relevante(resumen):
        return
    clave = f"ventana:{resumen['zone']}:{resumen['tipo']}"
    tuberia = r.pipeline()   # pipeline: varias órdenes en un solo viaje a Redis
    tuberia.zadd(clave, {json.dumps(resumen): resumen["ts"]})
    # Se recorta lo que ya salió de la ventana (según la hora del evento, no del reloj)
    tuberia.zremrangebyscore(clave, "-inf", resumen["ts"] - VENTANA)
    tuberia.expire(clave, 2 * VENTANA)   # si el sector deja de recibir eventos, la clave se borra sola
    tuberia.execute()


def eventos_en_ventana(zona, ts):
    tuberia = r.pipeline()
    for tipo in TIPOS_VENTANA:
        # Los más recientes primero, solo dentro de la ventana
        tuberia.zrevrangebyscore(f"ventana:{zona}:{tipo}", "+inf", ts - VENTANA, start=0, num=MAXIMO_POR_TIPO)
    return [json.loads(m) for miembros in tuberia.execute() for m in miembros]


# ------------------------------------------------- Eventos usados y cooldown
def libres(eventos, tipo_alerta):
    """Filtra los eventos que todavía no se usaron en una alerta de este tipo."""
    if not eventos:
        return []
    usados = r.mget([f"usado:{tipo_alerta}:{e['id']}" for e in eventos])
    return [e for e, usado in zip(eventos, usados) if usado is None]


def marcar_usados(tipo_alerta, ids):
    tuberia = r.pipeline()
    for event_id in ids:
        tuberia.set(f"usado:{tipo_alerta}:{event_id}", 1, ex=2 * VENTANA)
    tuberia.execute()


def tomar_cooldown(regla, sujeto):
    """True si no había una alerta igual reciente (y desde ahora la bloquea)."""
    return r.set(f"cooldown:{regla}:{sujeto}", 1, nx=True, ex=VENTANA) is True


# --------------------------------------------------------------- Vehículos
def ultimo_paso(plate_hash):
    datos = r.get(f"vehiculo:{plate_hash}")
    return json.loads(datos) if datos else None


def guardar_paso(resumen):
    r.set(f"vehiculo:{resumen['d']['plateHash']}", json.dumps(resumen), ex=TTL_VEHICULO)


def placa_buscada(plate_hash):
    return r.sismember("placas_buscadas", plate_hash) == 1


# ------------------------------------------------------ Pánicos pendientes
def agregar_panico_pendiente(resumen):
    r.set(f"panico:{resumen['id']}", json.dumps(resumen), ex=2 * VENTANA + 600)
    r.zadd("panicos_pendientes", {resumen["id"]: resumen["ts"]})


def confirmar_panico(event_id):
    r.zrem("panicos_pendientes", event_id)


def panicos_vencidos(ahora):
    """Saca y devuelve los pánicos cuya ventana ya cerró sin confirmación."""
    vencidos = []
    for event_id in r.zrangebyscore("panicos_pendientes", "-inf", ahora - VENTANA):
        # zrem devuelve 1 solo si lo quitamos nosotros: así nunca se genera dos veces
        if r.zrem("panicos_pendientes", event_id) == 1:
            datos = r.get(f"panico:{event_id}")
            if datos:
                vencidos.append(json.loads(datos))
    return vencidos
