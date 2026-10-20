# Explicación del proyecto: cómo está construido y cómo explicarlo

Este documento explica el sistema de punta a punta, en lenguaje sencillo, para que cualquier integrante del grupo pueda entenderlo y defenderlo frente al ingeniero. Primero da la idea general, luego sigue el recorrido de un evento paso a paso y al final explica cada pieza, las decisiones tomadas y las preguntas que probablemente hará el ingeniero.

---

## 1. La idea en un párrafo

La Municipalidad de San Juan Sacatepéquez quiere un centro de monitoreo que reciba datos de muchos sensores repartidos en el municipio (botones de pánico, cámaras que leen placas, radares de velocidad, sensores de sonido y reportes de vecinos), que **combine** esos datos para detectar situaciones graves (por ejemplo, un robo en curso) y que **avise** a la PNC, Bomberos, CONRED o una aseguradora. Como no tenemos sensores reales, los **simulamos con scripts de Python**. Todo corre en **contenedores de Docker** dentro de una sola laptop, pero cada pieza es un programa separado que se comunica con los demás por la red, por eso la arquitectura es **distribuida y orientada a eventos**.

### Una analogía para explicarlo

Pensemos en una central de emergencias:

| En la central de emergencias | En nuestro sistema |
|---|---|
| Vecinos y aparatos que avisan algo | Los **sensores simulados** |
| La recepcionista que reparte las llamadas entre varios operadores | **NGINX** (balanceador) |
| Los operadores que revisan que el aviso tenga sentido y lo anotan en un formato estándar | Los **3 ingestores** |
| Una banda transportadora donde se ponen todos los avisos en orden | **Kafka** |
| El analista que junta avisos relacionados y decide si es una emergencia real | El **correlador** |
| La libreta del analista con lo que pasó en los últimos minutos | **Redis** |
| El archivo con el historial de todo | **PostgreSQL** |
| La persona que llama a la PNC, Bomberos o CONRED | El **despachador** |
| Las pantallas de la sala de monitoreo | **Grafana** |
| El encargado de limpieza y reportes de cada día | **Airflow** |
| El buscador del archivo histórico | **Elasticsearch** |

---

## 2. El diagrama de la arquitectura

```
Simuladores (5 sensores + escenarios + reporte manual + JMeter)
        |  HTTP POST (JSON)
        v
     NGINX  (puerto 8080, reparte las peticiones)
        |
        v
  ingestor-1 / ingestor-2 / ingestor-3   (validan y estandarizan)
        |                     \
        v                      -> Kafka: events.invalid (eventos rechazados)
  Kafka: events.standardized
        |
        v
  correlador  <---->  Redis (memoria de corto plazo)
        |  guarda eventos y alertas en PostgreSQL
        v
  Kafka: correlated.alerts
        |
        +--> despachador --> Kafka: external.pnc / external.bomberos / external.conred
        |                --> aseguradora (REST)
        |                --> Elasticsearch (búsqueda histórica)
        +--> Grafana (lee PostgreSQL y Elasticsearch)
        +--> Airflow (tareas periódicas sobre PostgreSQL)
```

Este diagrama sigue el que entregó el catedrático. La diferencia principal: el catedrático dibujó ingestores en Java y Node; nosotros usamos solo Python para que el grupo pueda leer y explicar todo el código (ver la sección 12).

---

## 3. El recorrido de un evento, paso a paso

La mejor forma de explicar el sistema es seguir un caso real. Usamos el ejemplo del enunciado: **un robo en curso en Ciudad Quetzal** (escenario `ESC-ROBO-01`).

### Paso 1: el sensor genera el evento

Alguien presiona un botón de pánico en Ciudad Quetzal (sector `SJS-05`). El simulador (`simulador/generadores.py`) arma un mensaje JSON con este formato:

```json
{
  "schemaVersion": "1.0",
  "eventId": "8f14e45f-ceea-4e1b-9c2a-1a2b3c4d5e6f",
  "eventType": "security.panic_button.pressed",
  "timestamp": "2026-10-28T22:05:13.412-06:00",
  "emittedAt": "2026-10-28T22:05:13.430-06:00",
  "scenarioId": "ESC-ROBO-01",
  "device":   { "deviceId": "PAN-SJS05-012", "deviceType": "panic_button", "model": "SafeCity-PB100", "firmwareVersion": "1.2.0" },
  "location": { "zone": "SJS-05", "zoneName": "Ciudad Quetzal", "city": "San Juan Sacatepéquez", "latitude": 14.70313, "longitude": -90.58035 },
  "data":     { "alertType": "PANICO", "origin": "BOTON_FIJO", "userRef": null },
  "metadata": { "batteryLevel": 84, "signalStrength": -67, "protocol": "MQTT" }
}
```

Qué significa cada parte:
- `eventId`: identificador único (UUID). Sirve para rastrear el evento y para no procesarlo dos veces.
- `timestamp`: cuándo ocurrió. `emittedAt`: cuándo se envió. La diferencia entre `emittedAt` y la hora en que sale la alerta es la **latencia** que medimos.
- `device`, `location`, `data`, `metadata`: el mismo formato del ejemplo del catedrático. `data` cambia según el tipo de sensor.
- `scenarioId`: solo lo traen los escenarios de prueba. El correlador **nunca** lo usa para decidir; solo sirve para evaluar si acertamos.

### Paso 2: el evento llega a NGINX

El simulador hace un `POST http://nginx/events/batch` (envía los eventos en lotes de hasta 50). NGINX (`infra/nginx/nginx.conf`) es la **única puerta de entrada** y reparte las peticiones en **round robin** entre los 3 ingestores: una al 1, la siguiente al 2, la siguiente al 3, y así.

Si un ingestor falla 2 veces seguidas (`max_fails=2`), NGINX deja de usarlo durante 10 segundos (`fail_timeout=10s`) y reintenta la petición en otro (`proxy_next_upstream`). Por eso, si se cae un ingestor, el cliente ni se entera.

### Paso 3: el ingestor valida y estandariza

El ingestor (`servicios/ingestor/`) es una API hecha con **FastAPI**. Hace tres cosas:

1. **Valida** con Pydantic (`validacion.py`): revisa que estén todos los campos, que los tipos sean correctos y que los valores estén en su catálogo (por ejemplo, `alertType` solo puede ser `PANICO`, `EMERGENCIA_MEDICA` o `INCENDIO`). También revisa:
   - que el sector exista en `config/sectores.json`;
   - que las coordenadas estén dentro del municipio;
   - que la hora no esté más de 5 s en el futuro ni más de 24 h en el pasado;
   - que el mensaje del reporte no pase de 280 caracteres;
   - que el hash de la placa tenga 64 caracteres hexadecimales;
   - **privacidad:** si el evento trae la placa en claro (campo `plate`), lo rechaza con `PLACA_EN_CLARO`.
2. **Estandariza**: deja todas las fechas en el mismo formato y agrega quién lo recibió y cuándo:
   ```json
   "ingestion": { "ingestedAt": "2026-10-28T22:05:13.450-06:00", "ingestorId": "ingestor-2" }
   ```
3. **Publica en Kafka** (`kafka_productor.py`): los válidos van al topic `events.standardized` y los inválidos a `events.invalid` (la "dead-letter queue": un buzón de rechazados con el motivo). Ningún evento se descarta en silencio.

La respuesta HTTP dice cuántos se aceptaron y cuántos se rechazaron. Si Kafka no responde en 5 segundos, el ingestor contesta **HTTP 503** para que el cliente reintente.

### Paso 4: Kafka guarda y reparte

Kafka es un **registro de mensajes ordenado y persistente**. Los mensajes quedan guardados en disco 7 días, aunque alguien ya los haya leído. Es un "topic tonto": no toma decisiones, solo almacena y distribuye.

Cada mensaje lleva como **clave el sector** (`SJS-05`). Kafka manda todos los mensajes con la misma clave a la misma **partición**, así todos los eventos de Ciudad Quetzal llegan **en orden** al correlador. El orden importa porque la regla de robo depende de qué pasó primero.

### Paso 5: el correlador analiza (el cerebro)

El correlador (`servicios/correlador/`) sigue los 8 pasos del diagrama del catedrático:

| Paso del catedrático | Qué hace nuestro código | Archivo |
|---|---|---|
| 1. Recibo evento desde Kafka | Lee lotes de hasta 500 mensajes de `events.standardized` | `correlador.py` |
| 2. Busco el vehículo o el sector en Redis | Pide los eventos recientes del sector y, si es una placa, el último lugar donde se vio | `estado_redis.py` |
| 3. Redis responde con el último estado | Devuelve la "ventana" de los últimos 60 s (o 300 s) del sector | `estado_redis.py` |
| 4. Correlaciono A + B | Junta el evento nuevo con los anteriores del mismo sector | `reglas.py` |
| 5. Calculo velocidad (si aplica) | Distancia entre cámaras con la fórmula de haversine dividida entre el tiempo | `calculos.py` |
| 6. Aplico regla de negocio | Revisa las reglas R01 a R07 | `reglas.py` |
| 7. Guardo nuevo estado en Redis | Agrega el evento a la ventana y actualiza el vehículo | `estado_redis.py` |
| 8. Persisto el incidente y publico la alerta | Guarda en PostgreSQL y publica en `correlated.alerts` | `base_datos.py`, `correlador.py` |

En nuestro ejemplo:
- Llega el **pánico**. El correlador lo guarda en la ventana de Ciudad Quetzal y en la lista de "pánicos pendientes". Todavía no hay alerta.
- 40 segundos después llega una **cámara LPR** del mismo sector: un vehículo **SALIENDO** a 85 km/h (el límite del sector es 50).
- El correlador busca en la ventana y encuentra: pánico + vehículo saliendo + exceso de velocidad. Se cumple la **regla R01** y genera la alerta `ROBO_EN_CURSO` con severidad `CRITICA`.

La alerta se ve así:

```json
{
  "alertId": "uuid-de-la-alerta",
  "alertType": "ROBO_EN_CURSO",
  "severity": "CRITICA",
  "status": "NUEVA",
  "zone": "SJS-05",
  "zoneName": "Ciudad Quetzal",
  "score": 0.88,
  "rule": "R01_ROBO_EN_CURSO",
  "description": "Pánico + vehículo saliendo con exceso de velocidad en Ciudad Quetzal",
  "sourceEventIds": ["id-del-pánico", "id-de-la-cámara"],
  "scenarioId": "ESC-ROBO-01",
  "createdAt": "2026-10-28T22:05:53.540-06:00",
  "latencyMs": 140
}
```

- `sourceEventIds`: los eventos que originaron la alerta (**trazabilidad**: se puede llegar hasta cada sensor).
- `latencyMs`: cuánto tardó el sistema desde que se envió el último evento hasta que salió la alerta. Nuestro objetivo (SLO) es menos de 2000 ms.

### Paso 6: el despachador avisa a las entidades

El despachador (`servicios/despachador/despachador.py`) lee `correlated.alerts` y, según el tipo de alerta, la reenvía:

| Tipo de alerta | A quién se avisa |
|---|---|
| ROBO_EN_CURSO, ATAQUE_ARMADO, VEHICULO_REPORTADO, VELOCIDAD_ENTRE_CAMARAS, ZONA_ESCOLAR | PNC (topic `external.pnc`) |
| INCENDIO_CONFIRMADO | Bomberos y CONRED |
| ACCIDENTE_VIAL | PNC, Bomberos y la aseguradora (por REST) |
| PANICO_NO_CONFIRMADO | Nadie, solo se registra |

Así mostramos los dos protocolos del diagrama: **Kafka** (topics externos) y **REST** (la aseguradora simulada, `servicios/aseguradora_mock/`, que responde con un número de caso). Cada envío queda anotado en la tabla `dispatches`. En el perfil `full`, el despachador también guarda la alerta en **Elasticsearch** para la búsqueda histórica.

### Paso 7: el operador lo ve en Grafana

Grafana lee PostgreSQL cada 5 segundos. En el dashboard 1 aparece la alerta en rojo en la tabla y como un punto rojo en el mapa de Ciudad Quetzal.

### Paso 8: Airflow hace tareas periódicas

Cada día y cada hora, Airflow ejecuta tareas sobre la base de datos: borrar datos viejos, hacer el resumen diario y calcular la proporción de falsos positivos (sección 9).

### ¿Y si nada confirma el pánico? (falso positivo)

Si un pánico llega y en toda la ventana no aparece nada que lo confirme (escenario `ESC-FALSO-01` en Montúfar), el correlador revisa cada 10 segundos la lista de "pánicos pendientes". Cuando la ventana se cierra sin confirmación, genera `PANICO_NO_CONFIRMADO` con estado `FALSO_POSITIVO_PROBABLE` y severidad `BAJA`. Es el segundo ejemplo del enunciado.

---

## 4. Los sensores simulados

| Sensor | Tipo de evento | Datos principales (`data`) | Tasa normal |
|---|---|---|---|
| Botón de pánico | `security.panic_button.pressed` | tipo (PANICO, EMERGENCIA_MEDICA, INCENDIO), origen (botón fijo o app) | 1 por minuto |
| Cámara LPR (lectora de placas) | `traffic.lpr.read` | hash de la placa, placa parcial, velocidad estimada, ENTRADA o SALIDA, vehículo | 3 por segundo |
| Radar de velocidad | `traffic.speed.reading` | velocidad, límite, dirección, anomalía | 2 por segundo |
| Sensor acústico | `environment.acoustic.detected` | DISPARO, EXPLOSION o VIDRIO_ROTO, decibeles, probabilidad | 2 por minuto |
| Reporte ciudadano | `citizen.report.created` | ACCIDENTE, INCENDIO o ALTERCADO, mensaje libre de hasta 280 caracteres | 2 por minuto |

Detalles importantes:
- **7 sectores** del municipio en `config/sectores.json` (Casco urbano, Sajcavillá, Cruz Blanca, Lo de Mejía, Ciudad Quetzal, Villas del Quetzal y Montúfar), con coordenadas reales, un peso (probabilidad de que ocurra un evento ahí), un radio, si son zona escolar y su límite de velocidad.
- Los eventos se dispersan al azar dentro del radio de cada sector para que el mapa de calor se vea natural.
- Hay más tráfico de 6 a 9 y de 17 a 20 horas, y más pánicos y disparos de 19 a 2 horas.
- Entre 1 y 2 % de los eventos se dañan **a propósito** (placa en claro, sector inexistente, hora futura, etc.) para probar la validación.
- El simulador reintenta con espera creciente (1, 2, 4, 8 s) hasta 5 veces; si aun así falla, cuenta el lote como perdido.

Scripts del simulador (`simulador/`):

| Script | Para qué sirve |
|---|---|
| `simulador.py normal` | Tráfico continuo de los 5 sensores |
| `simulador.py pico --multiplicador 20` | Prueba de carga (20 veces más tráfico) |
| `simulador.py escenario todos` | Los 9 escenarios guionados al mismo tiempo |
| `reportar.py` | Reporte ciudadano escrito a mano (la "entrada manual" del enunciado) |
| `cargar_buscados.py` | Carga en Redis la lista de placas robadas |
| `evaluar.py` | Compara las alertas esperadas con las obtenidas y calcula los aciertos |

---

## 5. Las reglas de correlación

Todas las reglas, salvo R05, exigen el **mismo sector** y que los eventos estén dentro de la **ventana de tiempo** (`CORRELATION_WINDOW_SECONDS`: 300 s normalmente, 60 s en la demo).

| Regla | Alerta | Condición | Severidad | Escenario que la prueba |
|---|---|---|---|---|
| R01 | ROBO_EN_CURSO | Pánico + cámara LPR con vehículo SALIENDO + exceso de velocidad (radar o la misma cámara sobre el límite) | CRITICA | ESC-ROBO-01 |
| R02 | ATAQUE_ARMADO | Disparo con probabilidad de 0.7 o más + pánico | CRITICA | ESC-ARMADO-01 |
| R03 | ACCIDENTE_VIAL | Reporte de accidente + anomalía de velocidad, o 2 reportes de accidente | ALTA | ESC-ACCIDENTE-01 |
| R04 | INCENDIO_CONFIRMADO | Pánico de incendio + reporte de incendio o explosión | CRITICA | ESC-INCENDIO-01 |
| R05 | VELOCIDAD_ENTRE_CAMARAS | La misma placa en 2 cámaras distintas; velocidad promedio del tramo mayor a 80 km/h | MEDIA (ALTA en zona escolar y horario escolar) | ESC-TRAMO-01 |
| R06 | ZONA_ESCOLAR | Radar mayor a 30 km/h en zona escolar dentro del horario escolar | MEDIA | ESC-ESCOLAR-01 |
| R07 | VEHICULO_REPORTADO | La placa está en la lista de robados | ALTA | ESC-BUSCADO-01 |
| FP | PANICO_NO_CONFIRMADO | Pánico sin confirmación al cerrar la ventana | BAJA | ESC-FALSO-01 y ESC-RUIDO-01 |

### Cómo se calcula la velocidad entre cámaras (R05)

Ejemplo `ESC-TRAMO-01`: la misma placa pasa por una cámara en Lo de Mejía y 120 s después por otra en Sajcavillá.
1. Distancia entre las dos cámaras con la **fórmula de haversine** (la distancia sobre la superficie de la Tierra a partir de latitud y longitud): unos 3.33 km.
2. Velocidad = 3.33 km / (120 s / 3600) = **100 km/h**, mayor a 80, así que se genera la alerta.

### El score de la alerta

```
score = (condiciones cumplidas / condiciones de la regla) x promedio de confianza de los sensores
```

La confianza depende del sensor: el sensor acústico usa su propia probabilidad, el botón fijo vale 0.9, la app 0.8, los reportes entre 0.7 y 0.8, y las cámaras y radares 0.95. Está en `servicios/correlador/calculos.py`.

### Cómo se evita repetir alertas

- **Cooldown:** cuando una regla se dispara en un sector, no vuelve a dispararse ahí durante una ventana (en R05 y R07 el bloqueo es por placa, porque dos autos distintos son dos alertas distintas).
- **Eventos usados:** un evento que ya participó en una alerta de un tipo no se reutiliza para otra alerta del mismo tipo.
- **Duplicados:** si llega dos veces el mismo `eventId` (por un reintento), se ignora.

---

## 6. Las tecnologías y por qué cada una

| Pieza | Tecnología | Por qué |
|---|---|---|
| Contenedores | Docker + Docker Compose | Todo el sistema se levanta con un comando y funciona igual en cualquier máquina |
| Balanceador | NGINX | Reparte la carga entre los 3 ingestores y tolera la caída de uno |
| Ingestores | Python 3.12 + FastAPI + Pydantic | API REST sencilla; Pydantic valida el JSON con muy poco código |
| Broker de eventos | Apache Kafka 4.3 (modo KRaft, sin Zookeeper) | Guarda los mensajes en disco, en orden, y permite volver a leerlos si un servicio se cae |
| Estado de corto plazo | Redis 7.4 | Memoria rapidísima para las ventanas de tiempo, los vehículos y los duplicados |
| Base de datos | PostgreSQL 16 | Relacional, confiable; JSONB guarda los distintos formatos de `data` sin una tabla por sensor |
| Visualización | Grafana 12.4 | Mapas, tablas y gráficas conectadas directo a PostgreSQL y Elasticsearch |
| Tareas periódicas | Apache Airflow 3.3 (standalone) | Programa tareas, guarda su historial y permite ejecutarlas a mano desde la web |
| Búsqueda histórica | Elasticsearch 8.19 | Búsqueda de texto rápida sobre todas las alertas |
| Pruebas de carga | JMeter 5.5 | Herramienta estándar para medir cuántas peticiones aguanta el sistema |

---

## 7. Cómo está armado con Docker

Todo está en un solo archivo, `docker-compose.yml`. Cada servicio es un contenedor.

### Perfiles

| Perfil | Qué levanta | Para qué |
|---|---|---|
| `core` | Kafka, Kafka UI, PostgreSQL, Redis, 3 ingestores, NGINX, correlador, despachador, aseguradora y Grafana | Desarrollo y pruebas rápidas |
| `full` | Todo lo de `core` más Elasticsearch y Airflow | Pruebas finales y demo |

Comando para levantar todo: `docker compose --profile full up -d`.

### Reglas que se aplicaron a todos los servicios

- **`mem_limit`**: cada contenedor tiene un límite de memoria, para que uno descontrolado no tumbe a los demás.
- **`healthcheck`**: Kafka, PostgreSQL, Redis y Elasticsearch tienen una prueba de salud. Con `depends_on: condition: service_healthy`, un servicio no arranca hasta que aquellos de los que depende estén realmente listos.
- **`restart: unless-stopped`**: si un servicio se cae, Docker lo vuelve a levantar solo.
- **Puertos**: los ingestores no publican puertos (solo se llega a ellos por NGINX). PostgreSQL, Redis y Elasticsearch se publican solo en `127.0.0.1`, así que nadie en la red puede conectarse.
- **Contraseñas** en el archivo `.env`, que no se sube a git. Se entrega `.env.example` como plantilla.
- **Versiones fijas** de todas las imágenes (por ejemplo `postgres:16.15`), nunca `latest`, para que el sistema no cambie solo.
- **Volúmenes**: los datos de Kafka, PostgreSQL, Redis, Grafana, Elasticsearch y Airflow sobreviven aunque se borren los contenedores.

### Contenedores especiales

- **`kafka-init`**: corre una sola vez, crea los 6 topics (`infra/kafka/crear-topics.sh`) y termina. Por eso aparece como `Exited (0)`, que es normal.
- **`simulador`**: no queda corriendo; se ejecuta cuando se necesita con `docker compose run --rm simulador ...`.

### Los topics de Kafka

| Topic | Particiones | Contenido |
|---|---|---|
| `events.standardized` | 3 | Eventos válidos |
| `events.invalid` | 1 | Eventos rechazados con su motivo |
| `correlated.alerts` | 3 | Alertas del correlador |
| `external.pnc` | 1 | Avisos a la PNC |
| `external.bomberos` | 1 | Avisos a Bomberos |
| `external.conred` | 1 | Avisos a CONRED |

---

## 8. La base de datos

Los scripts de `infra/postgres/init/` se ejecutan solos la primera vez:
- `01_esquema.sql` crea las tablas e índices.
- `02_roles.sql` crea los usuarios (lee las contraseñas del `.env` con `\getenv`, así no quedan escritas en el código).
- `03_sectores.sql` carga los 7 sectores.

| Tabla | Qué guarda | Quién escribe |
|---|---|---|
| `zones` | Los 7 sectores | Script inicial |
| `events` | Cada evento válido procesado | Correlador |
| `alerts` | Cada alerta generada | Correlador |
| `slo_violations` | Alertas que tardaron más de 2000 ms | Correlador |
| `invalid_events` | Eventos rechazados (dead-letter) | Despachador |
| `dispatches` | Cada aviso enviado a una entidad | Despachador |
| `daily_stats` | Resumen diario de alertas | Airflow |
| `false_positive_stats` | Proporción de falsos positivos por sector | Airflow |
| `evaluation_results` | Resultado de cada evaluación de escenarios | `evaluar.py` |

Tres usuarios (principio de **mínimo privilegio**):

| Usuario | Puede | Lo usa |
|---|---|---|
| `admin` | Todo | Solo para crear la base |
| `app` | Leer y escribir | Correlador, despachador, Airflow y simulador |
| `grafana_ro` | Solo leer | Grafana |

Todas las fechas se guardan como `TIMESTAMPTZ` (fecha con zona horaria).

---

## 9. Las tareas de Airflow

Airflow corre en modo `standalone`: un solo contenedor con todo adentro. Los DAGs (flujos de tareas) están en `airflow/dags/`:

| DAG | Cuándo corre | Qué hace |
|---|---|---|
| `retencion_datos` | Cada día | Borra eventos e inválidos de más de 30 días y alertas de más de 365 días (privacidad) |
| `reporte_diario` | Cada día (y a mano) | Cuenta las alertas por día, sector, tipo y severidad en `daily_stats` |
| `analisis_falsos_positivos` | Cada hora | Calcula por sector qué porcentaje de pánicos no se confirmó, en `false_positive_stats` |

---

## 10. Los dashboards de Grafana

Los dashboards están guardados como archivos JSON en `infra/grafana/dashboards/` y se cargan solos al arrancar Grafana (aprovisionamiento).

1. **Centro de Monitoreo San Juan Sacatepéquez** (para el operador): contadores de eventos, alertas críticas y falsos positivos de la última hora; mapa de calor de los eventos de los últimos 15 minutos sobre el mapa real del municipio; mapa de alertas coloreadas por severidad; tabla de alertas recientes con las críticas en rojo; barras por tipo y por sector.
2. **Operación y SLOs** (para el técnico): eventos por segundo, latencia p50 y p95 con la línea roja en 2000 ms, porcentaje de inválidos con la línea en 2 %, eventos por ingestor (demuestra el balanceo de NGINX), violaciones de SLO y envíos por entidad.
3. **Histórico** (perfil `full`): búsqueda de alertas en Elasticsearch (por ejemplo `alertType:ROBO_EN_CURSO`), estadísticas diarias y falsos positivos por sector calculados por Airflow.
4. **Bandeja de instituciones**: una tabla para cada institución (PNC, Bomberos, CONRED y aseguradora) con las alertas que se le enviaron, las más recientes primero. Sale de la tabla `dispatches` y sirve para validar el enrutamiento del despachador: por ejemplo, CONRED solo debe recibir INCENDIO_CONFIRMADO.
5. **Despacho y falsos positivos**: arriba, cuántas alertas recibió cada institución, la regla de enrutamiento junto a los envíos reales (deben coincidir) y la lista de envíos, sin falsos positivos porque esos no se envían a nadie. Abajo, cómo un pánico pasa a alerta real: todo pánico queda pendiente en Redis durante la ventana; si llega al mismo sector el evento que completa R01, R02 o R04, se confirma y se envía; si la ventana cierra sin eso, queda como PANICO_NO_CONFIRMADO. Separa los falsos positivos reales (con los otros eventos del sector y el motivo) de los falsos positivos corregidos: pánicos marcados como falsos porque el correlador iba atrasado y que después sí se confirmaron. Con los datos de las pruebas de octubre: 2 272 pánicos, 273 pasaron a alerta real (10 de ellos corregidos) y 1 999 fueron falsos positivos reales.

---

## 11. Seguridad y privacidad

- **Las placas nunca viajan ni se guardan en claro.** La cámara (el simulador) calcula el **HMAC-SHA256** de la placa con una clave secreta (`PLATE_HMAC_KEY`). Sin la clave no se puede saber cuál era la placa. Para el operador solo se muestra una versión parcial: `P-***BCD`.
- Si algún evento trae la placa en claro, el ingestor lo **rechaza** con `PLACA_EN_CLARO`.
- El `userRef` de los pánicos desde la app es un seudónimo (nunca nombre ni teléfono).
- **Retención:** los datos se borran después de 30 días (alertas después de 365).
- Grafana solo puede leer (`grafana_ro`).
- Las bases de datos solo aceptan conexiones desde la misma máquina.
- Elasticsearch no tiene contraseña **solo** porque es un laboratorio; en producción se activaría su seguridad.

---

## 12. Las decisiones de arquitectura (resumen de los ADRs)

| # | Decisión | Alternativas que se descartaron | Por qué |
|---|---|---|---|
| 1 | On-premise en un solo servidor | Nube (AWS, Azure) | Presupuesto limitado del municipio; la carga normal (5 eventos/s) es unas 100 veces menor que lo que aguanta la laptop |
| 2 | Kafka como broker | RabbitMQ, SQS, Pub/Sub | Kafka guarda los mensajes y permite volver a leerlos si un servicio se cae; los servicios gestionados son en la nube y tienen costo mensual |
| 3 | PostgreSQL + Redis + Elasticsearch | TimescaleDB, InfluxDB, MongoDB | PostgreSQL es confiable y con JSONB guarda formatos distintos; Redis para estado rápido; Elasticsearch solo para búsqueda |
| 4 | Airflow standalone | `cron`, APScheduler | Lo pide el enunciado y da interfaz, historial y ejecución manual (aunque usa 1.3 GB de RAM) |
| 5 | Docker Compose | Kubernetes | En una sola máquina Kubernetes solo agrega complejidad y consumo, sin alta disponibilidad real |
| 6 | Resiliencia: dead-letter, reintentos, al menos una vez, duplicados | | Medido: 0 eventos perdidos al caerse un ingestor, el correlador o Redis |
| 7 | Privacidad: HMAC, rechazo de placa en claro, retención | | Protege datos personales de los vecinos |
| 8 | SLO de latencia menor a 2 s y menos de 2 % de inválidos | | Se mide en el dashboard 2 y se registra cada incumplimiento |

Otras dos decisiones:
- **Un solo lenguaje (Python)** en lugar de Java y Node como en el diagrama: el grupo puede leer y explicar todo el código.
- **JSON con `schemaVersion`** en lugar de Avro con Schema Registry: legible a simple vista y sin un servidor más; el campo `schemaVersion` permite cambiar el formato en el futuro.

El detalle completo con números está en `docs/notas-para-adrs.md`.

---

## 13. Resiliencia: qué pasa cuando algo se cae

| Si se cae... | Qué pasa | Resultado medido |
|---|---|---|
| Un ingestor | NGINX lo deja de usar y reintenta en otro | 0 eventos perdidos |
| El correlador | Los eventos esperan en Kafka; al volver, sigue desde donde quedó | 0 perdidos, 0 duplicados, vuelve en menos de 1 s |
| Redis | El correlador se reconecta solo; Redis recupera su estado del disco (AOF) | 0 perdidos |
| Kafka (32 s) | Los ingestores responden 503 y el simulador reintenta | 0 perdidos |
| Kafka (70 s) | Algunos lotes agotan sus 5 reintentos | unos 300 perdidos de 13 215 (2.3 %) |

Conceptos clave para explicarlo:
- **Entrega al menos una vez:** el correlador solo marca un mensaje como leído en Kafka **después** de guardarlo en PostgreSQL. Si se cae a la mitad, al volver lo procesa de nuevo: nunca se pierde, aunque podría llegar dos veces.
- **Deduplicación:** como un mensaje podría llegar dos veces, el correlador recuerda los `eventId` ya procesados en Redis y descarta las copias.
- **Dead-letter queue:** los eventos inválidos no se botan; van a `events.invalid` con su motivo.
- **Apagado ordenado:** cuando Docker detiene el correlador, este termina su lote y avisa a Kafka antes de salir, así otro puede tomar su trabajo de inmediato.

---

## 14. Números medidos (pruebas de carga)

| Indicador | Valor |
|---|---|
| Latencia de alertas con tráfico normal | 30 a 200 ms |
| Máximo sostenido cumpliendo el SLO de 2 s | unos 510 eventos/s |
| Capacidad del correlador | 660 eventos/s |
| Capacidad de ingestión (NGINX + ingestores) | al menos 26 500 eventos/s |
| Eventos inválidos | unos 1.5 % (por debajo del 2 %) |
| Escenarios acertados | 9 de 9 |
| Memoria usada bajo carga | unos 4.6 GB de 20 GB |

Un hallazgo para contar: la primera versión del correlador solo procesaba **66 eventos/s**, porque con cada evento leía toda la ventana del sector desde Redis (miles de eventos bajo carga). Se cambió para guardar solo los eventos que las reglas necesitan y leer como máximo los 20 más recientes de cada tipo, y para insertar en la base por lotes: subió a **660 eventos/s**, 10 veces más. El detalle está en `docs/resultados-pruebas.md`.

---

## 15. Preguntas que probablemente hará el ingeniero

**¿Por qué dicen que es distribuido si corre en una sola laptop?**
Porque cada pieza es un proceso independiente en su propio contenedor y solo se comunican por la red (HTTP y Kafka). Para pasar a varios servidores bastaría cambiar las direcciones: el código no cambia. La decisión de usar un solo servidor es de costo (ADR 1).

**¿Para qué sirve Kafka si podrían mandar los eventos directo al correlador?**
Kafka desacopla a quien produce de quien consume. Si el correlador se cae, los eventos esperan guardados en Kafka y no se pierden. Además permite que varios consumidores (correlador, despachador) lean lo mismo, y absorbe picos de carga.

**¿Para qué sirve Redis si ya tienen PostgreSQL?**
Redis está en memoria y responde en microsegundos. El correlador lo consulta varias veces por cada evento (ventanas, vehículos, duplicados). Hacer eso en PostgreSQL sería mucho más lento. PostgreSQL es el archivo permanente; Redis es la memoria de corto plazo.

**¿Qué pasa si llega el mismo evento dos veces?**
El correlador guarda cada `eventId` procesado en Redis durante 1 hora y descarta las copias. Además, la tabla `events` usa `ON CONFLICT DO NOTHING`, así que la base tampoco acepta duplicados.

**¿Cómo evitan falsos positivos?**
Una alerta grave necesita **varias fuentes** que coincidan (por ejemplo, pánico + cámara + velocidad). Un pánico solo, sin confirmación, se marca como `FALSO_POSITIVO_PROBABLE`. El escenario `ESC-RUIDO-01` demuestra que un vidrio roto con probabilidad baja no basta para una alerta crítica. Airflow calcula la proporción de falsos positivos por sector.

**¿Por qué la clave de Kafka es el sector?**
Para que todos los eventos de un sector lleguen en orden al correlador. El costo es que las particiones quedan desbalanceadas (los sectores no tienen el mismo tráfico), lo que limita escalar con más correladores.

**¿Cómo escalaría el sistema?**
La ingestión ya escala agregando ingestores detrás de NGINX. El correlador puede correr en varias copias en el mismo consumer group (medido: 3 copias procesan 743 eventos/s). Todo su estado está en Redis y las operaciones críticas son atómicas, así que las copias no se pisan.

**¿Cómo protegen las placas?**
Con HMAC-SHA256 y una clave secreta: se puede comparar si dos lecturas son la misma placa (mismo hash) sin saber cuál es. El ingestor rechaza cualquier placa en claro.

**¿Qué es el SLO y qué pasa si no se cumple?**
Es el objetivo de servicio: que una alerta salga en menos de 2 segundos (p95) y que haya menos de 2 % de eventos inválidos. Si una alerta tarda más, se escribe una advertencia en el log, se guarda en `slo_violations` y se ve en rojo en el dashboard 2.

**¿Por qué no usaron Kubernetes?**
En un solo servidor, Kubernetes agrega su propio plano de control (más memoria) y muchos conceptos nuevos sin dar alta disponibilidad real: si la máquina se apaga, todo se cae igual. Docker Compose cumple lo mismo de forma más simple.

**¿El código decide usando el `scenarioId`?**
No. El correlador nunca lo mira para decidir; solo se copia a la alerta para que `evaluar.py` pueda comprobar si acertamos.

---

## 16. Glosario

| Término | Significado |
|---|---|
| Evento | Un mensaje de un sensor (por ejemplo, "se presionó un botón de pánico") |
| Alerta | La conclusión del correlador al juntar varios eventos (por ejemplo, "robo en curso") |
| Correlación | Relacionar eventos distintos que ocurren cerca en tiempo y lugar |
| Ventana | El periodo de tiempo hacia atrás que se revisa para correlacionar (60 o 300 s) |
| Topic | Un "canal" de Kafka donde se publican mensajes de un tipo |
| Partición | Una división de un topic; mantiene el orden de los mensajes con la misma clave |
| Consumer group | Grupo de consumidores que se reparten las particiones de un topic |
| Offset | La posición de lectura en una partición; sirve para retomar donde se quedó |
| Dead-letter queue | Buzón donde van los mensajes rechazados con su motivo |
| Balanceador de carga | Reparte las peticiones entre varios servidores |
| Round robin | Repartir por turnos: uno a cada uno |
| Latencia | Tiempo entre que se envía un evento y sale su alerta |
| p50, p95, p99 | Percentiles: p95 = 1500 ms significa que el 95 % de las alertas tardó 1500 ms o menos |
| SLO | Objetivo de nivel de servicio (por ejemplo, latencia p95 menor a 2 s) |
| HMAC | Una "huella" de un dato calculada con una clave secreta |
| Haversine | Fórmula para calcular la distancia entre dos coordenadas sobre la Tierra |
| Cooldown | Tiempo durante el cual no se repite la misma alerta |
| Idempotente | Que hacerlo dos veces da el mismo resultado que hacerlo una vez |
| Contenedor | Un programa empaquetado con todo lo que necesita para correr igual en cualquier máquina |
| Healthcheck | Prueba periódica para saber si un servicio está sano |
| DAG | En Airflow, un flujo de tareas con su horario |

---

## 17. Mapa de archivos

```
docker-compose.yml               todos los servicios y los perfiles core y full
.env.example                     plantilla de contraseñas (se copia a .env)
config/sectores.json             los 7 sectores del municipio
infra/nginx/nginx.conf           balanceador
infra/kafka/crear-topics.sh      crea los 6 topics
infra/postgres/init/             tablas, usuarios y sectores
infra/grafana/                   datasources y los 3 dashboards
servicios/ingestor/
  app.py                         API: /events, /events/batch, /health
  validacion.py                  reglas de validación con Pydantic
  kafka_productor.py             publica en Kafka
servicios/correlador/
  correlador.py                  bucle principal (los 8 pasos)
  reglas.py                      reglas R01 a R07 y falso positivo
  calculos.py                    haversine, horario escolar y score
  estado_redis.py                todo lo que se guarda en Redis
  base_datos.py                  guardado en PostgreSQL
servicios/despachador/despachador.py   avisos a PNC, Bomberos, CONRED, aseguradora y Elasticsearch
servicios/aseguradora_mock/app.py      aseguradora simulada
simulador/
  generadores.py                 crea los eventos de cada sensor
  envio.py                       envía con reintentos
  simulador.py                   modos normal, pico y escenario
  escenarios.py                  los 9 escenarios y sus alertas esperadas
  reportar.py                    reporte ciudadano manual
  cargar_buscados.py             placas robadas en Redis
  evaluar.py                     tabla de aciertos
airflow/dags/                    retencion_datos, reporte_diario, analisis_falsos_positivos
pruebas/jmeter/carga.jmx         plan de carga
docs/notas-para-adrs.md          decisiones con sus números
docs/resultados-pruebas.md       resultados de carga y resiliencia
docs/guion-demo.md               guion de la jornada de simulación
README.md                        cómo instalar y usar todo
```

---

## 18. Guion corto para explicarlo en 2 minutos

> "Simulamos un centro de monitoreo para San Juan Sacatepéquez. Cinco tipos de sensores, simulados con Python, envían eventos en JSON a NGINX, que los reparte entre tres ingestores. Los ingestores validan cada evento, rechazan los malos (incluida cualquier placa en claro) y publican los buenos en Kafka. El correlador lee de Kafka, usa Redis como memoria de los últimos minutos y aplica siete reglas: por ejemplo, un pánico, más un vehículo saliendo, más exceso de velocidad en el mismo sector es un robo en curso. Las alertas se guardan en PostgreSQL y el despachador avisa a la PNC, Bomberos, CONRED o la aseguradora. Grafana muestra todo en mapas y tablas, Airflow hace la limpieza y los reportes, y Elasticsearch permite buscar en el histórico. Todo corre en Docker en un solo servidor por costo, pero cada pieza es independiente. Medimos alertas en menos de 200 ms con tráfico normal, unos 510 eventos por segundo sostenidos dentro del objetivo de 2 segundos, y cero eventos perdidos cuando se cae un ingestor, el correlador o Redis."
