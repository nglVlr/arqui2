# Notas para los ADRs

Este archivo se va llenando durante la implementación. Por cada tema: decisión, alternativas consideradas, por qué y números medidos.
No es el documento final de ADRs; es la materia prima para escribirlos.

## 1. Plataforma de despliegue
- Decisión: on-premise en un solo servidor (laptop con Windows 11 + Docker Desktop sobre WSL2).
- Hardware real medido en la Fase 0: Intel i7-13650HX (20 hilos lógicos), 31.7 GB de RAM.
- A WSL2 se le asignan 20 GB de RAM, 12 procesadores y 4 GB de swap (`.wslconfig`), para dejar memoria libre a Windows durante la demo.
- Verificado con `docker info`: Docker ve 20972171264 bytes (unos 19.5 GiB) y 12 CPUs, así que la configuración de WSL2 se aplicó.
- Pendiente: latencia extremo a extremo medida (Fase 8).

## 2. Broker de eventos
- Decisión: Apache Kafka 4.3.1 (imagen oficial `apache/kafka`) en modo KRaft, un solo broker que también es controller. Sin Zookeeper: un contenedor menos y menos memoria.
- 6 topics creados por `kafka-init` con `--if-not-exists`; `auto.create.topics.enable=false` para que nadie cree topics por error. Retención de 7 días.
- Dos listeners: INTERNO (kafka:9092) para los contenedores y EXTERNO (localhost:9094) solo para depurar desde Windows.
- Factor de replicación 1: con un solo servidor no hay a dónde replicar. Es un riesgo aceptado del ADR 1 (si el disco del servidor falla se pierde el historial de Kafka).
- Alternativas: RabbitMQ (cola clásica, borra el mensaje al consumirlo; no permite reprocesar el historial como hace Kafka con los offsets), SQS o Pub/Sub (servicios gestionados en la nube, contradicen la decisión on-premise y tienen costo mensual).
- Clave de partición = zona: todos los eventos de un sector caen en la misma partición y llegan en orden al correlador. Costo medido en la Fase 3: las 3 particiones quedan desbalanceadas (163 / 491 / 71 mensajes tras 2 minutos de tráfico), porque solo hay 7 zonas con pesos distintos. Se acepta: con un solo correlador no importa, y el orden por sector vale más que el balance.
- Pendiente: números medidos de rendimiento (Fase 8).

## 3. Persistencia
- Decisión: PostgreSQL 16.15. Todas las fechas en TIMESTAMPTZ. JSONB para `data` y `metadata` de los eventos (los 5 tipos de sensor tienen formatos distintos y JSONB permite guardarlos sin una tabla por tipo).
- Llave foránea de `events.zone` y `alerts.zone` hacia `zones`: la base rechaza un sector inexistente aunque fallara la validación del ingestor.
- Redis 7.4.11 con AOF (`appendonly yes`) para el estado del correlador: si Redis se reinicia, recupera ventanas, placas y duplicados.
- Pendiente: alternativas (TimescaleDB, InfluxDB, NoSQL) y números de volumen.

## 4. Orquestación de tareas periódicas
- Decisión: Apache Airflow 3.3.2 en modo `standalone` (un contenedor con scheduler, servidor web y procesador de DAGs; metadatos en SQLite). Aceptable para laboratorio; en producción iría con PostgreSQL de metadatos y un ejecutor distribuido.
- 3 DAGs con `psycopg2` directo, sin operadores especiales (más fácil de explicar): `retencion_datos` (diario), `reporte_diario` (diario, recalcula ayer y hoy con UPSERT, se puede correr a mano sin duplicar), `analisis_falsos_positivos` (cada hora).
- Verificado en la Fase 7: 0 errores de importación; las 3 ejecuciones manuales terminaron en `success`; `daily_stats` y `false_positive_stats` con datos. Retención: 0 filas borradas (no hay datos con más de 30 días).
- Costo medido: Airflow usa unos 1.33 GB de RAM sin hacer nada, el servicio más pesado junto con Elasticsearch (1.5 GB). Para 3 tareas SQL simples, un `cron` o un scheduler de Python (APScheduler) usaría unos 30 MB. Se eligió Airflow porque lo pide el enunciado y da interfaz, historial de ejecuciones, reintentos y ejecución manual desde la web.
- La contraseña del usuario `admin` viene del `.env` (el contenedor escribe el archivo de SimpleAuthManager antes de arrancar), en lugar de la contraseña aleatoria que Airflow genera en cada arranque.

## 5. Contenerización
- Decisión: Docker Compose con perfiles `core` y `full` en un solo servidor. Kubernetes no aporta en un solo nodo: agrega plano de control (más memoria), manifiestos y conceptos (pods, services, ingress) sin ganar alta disponibilidad real, porque si cae la máquina cae todo igual.
- Consumo medido en la Fase 7 (perfil full con tráfico normal, `docker stats`): 4.1 GB en total de los 20 GB de WSL2. Los más pesados: Elasticsearch 1.5 GB, Airflow 1.33 GB, Kafka 0.49 GB, Kafka UI 0.33 GB; cada servicio Python usa menos de 40 MB. Suma de los `mem_limit`: unos 12.4 GB, dentro de los 20 GB.
- `mem_limit` en cada servicio para que uno descontrolado no tumbe a los demás; `healthcheck` + `depends_on: service_healthy` para el orden de arranque; `restart: unless-stopped` para recuperarse solos.
- Cambiar de perfil: `docker compose --profile core down` y luego `docker compose --profile full up -d` (los volúmenes conservan los datos).

## Elasticsearch
- Elasticsearch 8.19.22 en un nodo, `-Xms1g -Xmx1g`, seguridad desactivada SOLO por ser un laboratorio en una sola máquina y con el puerto publicado solo en 127.0.0.1. En producción: TLS, usuarios y roles.
- Al pasar al perfil full se rebobinó el consumer group `despachador` en `correlated.alerts` para indexar el histórico: 200 alertas en PostgreSQL = 200 documentos en `alertas`. No se reenviaron avisos porque el despachador revisa `dispatches` antes de avisar.
- Hallazgo: Grafana escapa las variables para Lucene, así que `*` llegaba como `\*` y no encontraba nada. Se usa `${busqueda:raw}`.

## 6. Resiliencia
- Pool de 3 ingestores detrás de NGINX en round robin con `max_fails=2 fail_timeout=10s` y `proxy_next_upstream` (si un ingestor no responde, NGINX reintenta la misma petición en otro).
- Medido en la Fase 2: 30 eventos con los 3 ingestores = 10/10/10. Con `ingestor-2` detenido: 30 de 30 respondieron 200 (14 ingestor-1, 16 ingestor-3), cero errores para el cliente. Al volver a arrancarlo se reintegró solo (3/3/3).
- `zone` + `resolve` en el upstream de NGINX: si un ingestor se reinicia y Docker le da otra IP, NGINX la vuelve a resolver; además NGINX puede arrancar aunque falte un ingestor.
- Dead-letter: todo evento rechazado va a `events.invalid` con motivo, ingestor, hora y el original. Nada se descarta en silencio.
- Correlador con commit manual del offset al final de cada lote, en este orden: PostgreSQL, alertas a Kafka, marca de procesados en Redis y offset de Kafka. Si se cae en medio, el lote se reprocesa (al menos una vez) y no se pierde nada.
- Desviación consciente del plan: el plan sugería `SET evento:{id} NX` al inicio. Se cambió a "revisar al inicio y marcar al final" porque con NX al inicio, una caída entre la marca y el guardado haría que el evento se descarte como duplicado al reprocesar, es decir, se perdería. Además `INSERT ... ON CONFLICT DO NOTHING` en `events` y los cooldowns en Redis evitan duplicados en la base y en las alertas.
- Prueba de la Fase 4 (90 s de tráfico normal, reinicio del correlador a los 35 s): 448 eventos aceptados por los ingestores = 448 en PostgreSQL, 0 alertas duplicadas. Latencia de proceso (emitido a procesado) p50 111 ms, p95 199 ms, máximo 204 ms, incluido el reinicio.
- Apagado ordenado: el correlador atiende SIGTERM, termina el lote y cierra el consumidor (avisa a Kafka). Medido: detenido y de vuelta procesando en menos de 1 s. Sin esto, en la primera prueba Kafka tardó unos 32 s en reasignar las particiones (esperaba que venciera la sesión del consumidor muerto, 45 s por defecto). Se bajó `session.timeout.ms` a 10 s para el caso de caída abrupta (kill o falla).
- Errores de PostgreSQL o Redis dentro del correlador: el proceso termina y Docker lo reinicia (`restart: unless-stopped`). Es más simple y seguro que reintentar a mano: retoma desde el último offset confirmado.
- Productor con `acks=all` e idempotencia. Si Kafka no confirma en 5 s, el ingestor responde 503 y el cliente reintenta. Esto puede generar duplicados (el evento quedó en la cola interna y luego se entregó), por eso el correlador descarta duplicados por `eventId` (entrega al menos una vez + deduplicación).

## 7. Seguridad y privacidad
- Tres usuarios de PostgreSQL: `admin` (solo crea la base), `app` (lectura y escritura, lo usan los servicios) y `grafana_ro` (solo SELECT). Verificado en la Fase 1: `grafana_ro` recibe `permission denied` al intentar un INSERT.
- Las contraseñas de los usuarios se leen en `02_roles.sql` con `\getenv` de psql, así no quedan escritas en ningún archivo del repositorio.
- PostgreSQL, Redis y el puerto externo de Kafka se publican solo en 127.0.0.1: no son accesibles desde otras máquinas de la red.
- El ingestor revisa `data.plate` antes que cualquier otra validación y rechaza con `PLACA_EN_CLARO`. Verificado en la Fase 2.
- Los ingestores no publican puertos: la única puerta de entrada es NGINX (8080).
- Las contraseñas y la clave HMAC de placas se generan aleatoriamente (generador criptográfico) y viven solo en `.env`, que no se sube a git. Se entrega `.env.example` con valores de ejemplo.

## 8. Observabilidad y SLOs
- SLO de latencia: `latencyMs` = hora de creación de la alerta menos `emittedAt` del último evento que completó la regla; umbral 2000 ms (`SLO_LATENCIA_MS`). Si se supera: log WARNING e insert en `slo_violations`.
- Medido en la Fase 4 (escenarios con velocidad 4): latencias de 36 a 194 ms en las 7 reglas en tiempo real.
- Hallazgo: con `consume(timeout=1.0)` el correlador esperaba hasta 1 s para juntar un lote cuando hay poco tráfico, y la latencia rondaba los 800 ms. Con `timeout=0.2` bajó a unos 100 a 200 ms.
- Al procesar un atraso (por ejemplo tras una caída) las alertas salen con latencias altas y quedan registradas como violaciones de SLO. Es lo correcto: el SLO mide lo que vivió el operador.
- Grafana 12.4 con datasources y dashboards aprovisionados como archivos (versionados en git, se recrean idénticos en cualquier máquina). Usuario `grafana_ro`. Dashboard 1 (operador: mapas, tabla, contadores) y dashboard 2 (SLOs, balanceo, calidad de datos, envíos externos).
- Mapa base OpenStreetMap: lo descarga el navegador de quien mira el dashboard (requiere internet en esa máquina, no en el servidor). Riesgo para la demo: sin internet los mapas muestran los puntos sin calles de fondo.
- Hallazgo: Grafana no detectó cambios en los JSON de dashboards montados desde Windows (el sondeo de cambios no ve la modificación a través del montaje de Docker Desktop). Tras editar un dashboard hay que ejecutar `docker compose restart grafana`.
- Los falsos positivos (`PANICO_NO_CONFIRMADO`) no cuentan para el SLO: por definición esperan la ventana completa. Su `latencyMs` se mide desde el cierre de la ventana (hasta unos 10 s, porque la revisión es cada 10 s).

## Reglas de correlación
- Resultado de `evaluar.py` en la Fase 4: 9 de 9 escenarios con su alerta esperada (ventana de 60 s, `--velocidad 4`, `SCHOOL_HOURS=00:00-23:59` para poder probar ESC-ESCOLAR-01 a cualquier hora).
- Una regla solo se dispara si el evento recién llegado participa en ella; los eventos usados se marcan por tipo de alerta y hay cooldown por regla y sector.
- Cooldown por placa (no por sector) en R05 y R07: dos vehículos distintos en el mismo sector son dos alertas distintas.
- Ventana por hora del evento (no del reloj): al reprocesar un atraso, los eventos siguen correlacionándose entre sí.
- Caso límite encontrado: ESC-ACCIDENTE-01 separa sus eventos exactamente 60 s; con ventana de 60 s y velocidad 1, el primer evento queda fuera por milisegundos. En la demo con ventana de 60 s usar `--velocidad 2` o más (o una ventana de 90 s).
- ESC-ROBO-01 genera además ZONA_ESCOLAR cuando se ejecuta en horario escolar: el radar de 92 km/h está en Ciudad Quetzal, que es zona escolar. Es una alerta correcta, no un falso positivo; `evaluar.py` la muestra como "adicional".
- Interferencia observada: un escenario puede quedar bloqueado por el cooldown de una alerta igual del tráfico normal en el mismo sector (le pasó a ESC-ESCOLAR-01 en la primera corrida). Para evaluar escenarios conviene correrlos sin tráfico normal.
- Fórmula del score documentada en `servicios/correlador/calculos.py`.

## Integración con entidades externas (despachador)
- Dos protocolos, como en el diagrama: Kafka (topics `external.pnc`, `external.bomberos`, `external.conred`, uno por entidad) y REST (aseguradora, `POST /siniestros`).
- Cada envío queda en la tabla `dispatches` (destino, protocolo, resultado). `PANICO_NO_CONFIRMADO` se registra con destino `ninguno`.
- Verificado en la Fase 5: 185 alertas, 185 despachadas; offsets de los topics externos iguales a los conteos de `dispatches` (PNC 42, Bomberos 9, CONRED 5); la aseguradora devolvió números de caso.
- Idempotencia: antes de despachar se revisa si la alerta ya tiene envíos (reproceso tras una caída). Elasticsearch usa el `alertId` como id del documento, así que reindexar sobrescribe en lugar de duplicar.
- Si la aseguradora no responde, se registra `ERROR` en `dispatches` y se sigue con las demás alertas (no se bloquea el aviso a la PNC por una aseguradora caída).
- Perfiles: `despachador` (core, sin Elasticsearch) y `despachador-es` (full, con Elasticsearch) son el mismo código con distinta variable de entorno. Compose no permite cambiar variables según el perfil, por eso son dos definiciones del servicio. Hay que usar un perfil a la vez.

## Calidad de datos (validación en el ingestor)
- Pydantic valida campos obligatorios, tipos y catálogos (`Literal`). Reglas externas aparte: sector existente, límites del municipio, hora (no más de 5 s en el futuro ni 24 h en el pasado).
- Motivos de rechazo legibles: `PLACA_EN_CLARO`, `ZONA_DESCONOCIDA`, `COORDENADAS_FUERA_DE_LIMITES`, `TIMESTAMP_EN_EL_FUTURO`, `TIMESTAMP_DEMASIADO_ANTIGUO`, `MENSAJE_DEMASIADO_LARGO`, `PLATE_HASH_INVALIDO`, `CAMPO_INVALIDO:<campo>`, etc.
- Estandarización: las fechas se reescriben en un formato único (ISO 8601 con milisegundos y zona) y `data` se limpia con el modelo de su tipo (campos desconocidos se descartan).

## Simulación
- Tráfico normal medido en la Fase 3 (2 minutos): 610 eventos enviados, 597 aceptados, 13 rechazados (2.13 %, todos dañados a propósito: placa en claro, coordenadas, zona, hora futura, hash), 0 perdidos, 5.1 eventos/s.
- `emittedAt` se pone justo antes del envío HTTP (no al generar el evento), para que la latencia medida sea la del sistema y no la espera del lote.
- En tráfico normal las anomalías son raras a propósito (0.2 % de LPR sobre el límite, 0.2 % de lecturas con exceso, 0.1 % de trayectoria anómala, probabilidad acústica casi siempre bajo 0.7).
- Hallazgo de la Fase 6: con 0.5 % de excesos aparecieron 2 ROBO_EN_CURSO y 2 ACCIDENTE_VIAL por azar en un minuto (con doble tráfico). La regla R01 tal como está definida (pánico + cualquier LPR saliendo + cualquier anomalía de velocidad en el sector) es muy sensible: en tráfico normal siempre hay algún LPR saliendo, así que en la práctica basta pánico + exceso. Se mantuvo la regla del plan y se bajaron las anomalías del tráfico normal. Tema para el ADR: a mayor ventana de correlación, más correlaciones por azar (la ventana de 300 s es 5 veces más propensa que la de 60 s). Una mejora futura sería exigir que la anomalía sea del mismo vehículo (misma placa).

## Otras decisiones
- Un solo lenguaje (Python) para todos los servicios en lugar de ingestores en Java y Node como en el diagrama: pendiente de argumentar.
- JSON con `schemaVersion` frente a Avro con Schema Registry: pendiente de argumentar.
