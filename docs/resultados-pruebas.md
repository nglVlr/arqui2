# Resultados de las pruebas de carga y resiliencia (Fase 8)

Todas las pruebas se hicieron el 4 de octubre de 2026 en la laptop del proyecto (Intel i7-13650HX, 32 GB de RAM; WSL2 con 20 GB y 12 CPUs), con el perfil `full` levantado, ventana de correlación de 60 s y `SCHOOL_HOURS=00:00-23:59`.
Los números salen de: el resumen del simulador, el resumen de JMeter, el lag del consumer group `correlador` en Kafka y consultas a PostgreSQL (`processed_at - emitted_at` en `events`, `latency_ms` en `alerts`).

## 1. Resumen

| Indicador | Valor medido |
|---|---|
| Máximo sostenido de extremo a extremo cumpliendo el SLO de 2 s | **unos 510 eventos/s** (simulador x100: 508 eventos/s durante 2 min) |
| Capacidad del correlador (1 instancia, vaciando cola) | **660 eventos/s** |
| Capacidad del correlador (3 instancias, una por partición) | 743 eventos/s (limitada por la partición más cargada) |
| Capacidad de ingestión (NGINX + 3 ingestores + Kafka) | **al menos 26 500 eventos/s** (530 lotes de 50 por segundo; el límite fue JMeter, no el sistema) |
| Latencia de alertas a 508 eventos/s (p50 / p95 / p99) | 525 / 976 / 1110 ms |
| Latencia de proceso de eventos a 508 eventos/s (p50 / p95 / p99 / máx.) | 816 / 1328 / 1863 / 2174 ms |
| Latencia de alertas en tráfico normal y escenarios | 36 a 200 ms |
| Eventos inválidos | 1.43 a 1.51 % (todos dañados a propósito por el simulador; SLO menor a 2 %) |
| Eventos perdidos en caídas de un ingestor, del correlador o de Redis | 0 |
| Eventos perdidos con Kafka caído 32 s | 0 |
| Eventos perdidos con Kafka caído 70 s | unos 300 de 13 215 (2.3 % del tráfico de esa prueba) |
| Memoria total bajo carga (perfil full) | unos 4.6 GB de 20 GB |
| CPU total bajo carga x100 | unos 90 % de 1200 % (menos de 1 núcleo de 12) |

El diagrama del catedrático menciona más de 10K RPS. La ingestión supera ese número (al menos 26 500 eventos/s). El sistema completo de extremo a extremo se limita a unos 510 eventos/s por el correlador, que es un solo proceso Python que hace varias consultas a Redis por cada evento. No se inventó ningún número: todos salen de las mediciones de esta sección.

## 2. Pruebas de carga con el simulador

| Prueba | Eventos/s enviados | Perdidos | Cola del correlador al terminar | Latencia de alertas p50 / p95 / p99 | Cumple SLO |
|---|---|---|---|---|---|
| Pico x20, 60 s (antes de optimizar) | 101.6 | 0 | unos 200 | proceso p95 1072 ms | sí, en el límite |
| Pico x100, 120 s | 508.3 | 0 | 0 | 525 / 976 / 1110 ms | **sí** |
| Pico x130, 90 s | 660.8 | 0 | 7076 | 5650 / 13 668 / 14 359 ms | no |
| Pico x160, 90 s | 813.3 | 0 | 21 600 | 21 075 / 42 847 / 43 590 ms | no |

Cuando la carga supera la capacidad del correlador **no se pierde nada**: los eventos esperan en Kafka y se procesan después. Lo que se incumple es la latencia (el SLO), y queda registrado en `slo_violations` y en el dashboard 2.

## 3. Capacidad del correlador y optimización

Método: se detiene el correlador, se envían unos 15 000 eventos (pico x50 durante 60 s), se arranca y se mide cuánto tarda en dejar el lag en cero.

| Versión del correlador | Tiempo de vaciado | Eventos/s |
|---|---|---|
| Original: leía toda la ventana del sector desde Redis con cada evento | 225 s | 66 |
| Ventana por tipo de evento, solo eventos relevantes, máximo 20 por tipo, con pipeline de Redis | 27 s | 556 |
| Lo anterior más un INSERT por lote (`execute_values`) en lugar de uno por evento | 22.7 s | 660 |
| Lo anterior con 3 correladores (`--scale correlador=3`) | 20.2 s | 743 |

Por qué 3 correladores no triplican la capacidad: la clave de partición es la zona y solo hay 7 zonas con pesos distintos. En la prueba, la partición 1 tenía 9049 de 15 022 eventos (60 %), y su correlador terminó último. Para escalar más habría que usar más particiones o una clave más repartida, perdiendo el orden por sector.

## 4. Capacidad de ingestión con JMeter

Plan: `pruebas/jmeter/carga.jmx`, lotes de 50 eventos (LPR y radar, válidos) a `POST /events/batch` a través de NGINX, 30 s por prueba.

| Hilos | Lotes/s | Eventos/s | Tiempo de respuesta promedio | Errores |
|---|---|---|---|---|
| 4 (15 s) | 340.7 | unos 17 000 | 8 ms | 0 % |
| 8 | 522.5 | unos 26 100 | 13 ms | 0 % |
| 16 | 541.4 | unos 27 100 | 26 ms | 0 % |
| 32 | 528.5 | unos 26 400 | 54 ms | 0 % |

Con 16 hilos, JMeter usaba 2.2 núcleos (genera el JSON con Groovy) y cada ingestor entre 50 y 70 % de un núcleo, NGINX 16 % y Kafka 47 %. El techo de unos 26 500 eventos/s es del generador de carga; los ingestores todavía tenían margen.
Los 3.4 millones de eventos sintéticos de JMeter se descartaron para el correlador (reset del offset al final del topic) porque solo servían para medir la ingestión.

## 5. Pruebas de resiliencia

| Prueba | Qué pasó | Eventos aceptados | En PostgreSQL | Perdidos | Alertas duplicadas |
|---|---|---|---|---|---|
| Caída de `ingestor-2` 20 s (tráfico x20) | NGINX lo saca del grupo (`max_fails`) y reintenta en otro ingestor. El simulador vio 1 solo reintento. | 9003 | 9003 | 0 | 0 |
| Reinicio de Redis (tráfico x20, en la misma prueba) | El cliente de Redis se reconecta solo; el correlador no se cayó. Redis recupera su estado del AOF. Latencia máxima 3.2 s durante el reinicio. | (misma prueba) | (misma prueba) | 0 | 0 |
| Reinicio del correlador a mitad de tráfico (Fase 4) | Apagado ordenado: termina el lote, cierra el consumidor y vuelve en menos de 1 s. | 448 | 448 | 0 | 0 |
| Correlador detenido con 15 000 eventos en cola | Kafka guarda los eventos; al volver los procesa todos (660 eventos/s). | 15 012 | 15 012 | 0 | 0 |
| Kafka caído 32 s (tráfico x20) | Los ingestores responden 503 (32 veces); el simulador reintenta y acumula lotes en su cola mientras espera. | 9989 | 9989 | 0 | 0 |
| Kafka caído 70 s (tráfico x20) | Algunos lotes agotan sus 5 intentos (unos 40 s: 5 s de espera por intento más 1, 2, 4 y 8 s entre intentos). | 12 465 | 12 716 | unos 300 | 0 |

Detalle de la caída de Kafka de 70 s: el simulador contó 559 eventos perdidos (16 lotes), pero 251 de ellos sí llegaron a PostgreSQL. El ingestor respondió 503 porque Kafka no confirmó a tiempo, pero los mensajes quedaron en la cola interna del productor y se entregaron al volver Kafka. Por eso hay más eventos en la base (12 716) que aceptados (12 465). La deduplicación por `eventId` del correlador evitó que esas copias se contaran dos veces.
**Pérdida aceptable:** el sistema no pierde datos ante la caída de cualquier componente que no sea Kafka. Ante una caída de Kafka, el cliente aguanta unos 40 s por lote. Más allá de eso, la pérdida es proporcional a la duración de la caída (2.3 % del tráfico en una caída de 70 s dentro de una prueba de 130 s).

## 6. Uso de recursos bajo carga (x100, `docker stats`)

| Servicio | CPU | Memoria |
|---|---|---|
| correlador | 57 % | 56 MB |
| redis | 15 % | 182 MB |
| kafka | 6.5 % | 454 MB |
| postgres | 1.2 % | 237 MB |
| ingestor-1/2/3 | 1 a 2 % cada uno | 40 a 54 MB cada uno |
| nginx | 0.4 % | 28 MB |
| elasticsearch | 0.3 % | 1.6 GB |
| airflow | 2 % | 1.34 GB |
| kafka-ui | 0 % | 336 MB |
| grafana | 0.4 % | 94 MB |
| **Total** | **unos 90 % (de 1200 %)** | **unos 4.6 GB (de 20 GB)** |

El cuello de botella es el correlador: un solo proceso Python que espera respuestas de Redis y PostgreSQL. La máquina tiene mucho margen de CPU y memoria.
