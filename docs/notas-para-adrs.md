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
- Pendiente: números medidos de rendimiento (Fase 8).

## 3. Persistencia
- Decisión: PostgreSQL 16.15. Todas las fechas en TIMESTAMPTZ. JSONB para `data` y `metadata` de los eventos (los 5 tipos de sensor tienen formatos distintos y JSONB permite guardarlos sin una tabla por tipo).
- Llave foránea de `events.zone` y `alerts.zone` hacia `zones`: la base rechaza un sector inexistente aunque fallara la validación del ingestor.
- Redis 7.4.11 con AOF (`appendonly yes`) para el estado del correlador: si Redis se reinicia, recupera ventanas, placas y duplicados.
- Pendiente: alternativas (TimescaleDB, InfluxDB, NoSQL) y números de volumen.

## 4. Orquestación de tareas periódicas
- Pendiente.

## 5. Contenerización
- Pendiente.

## 6. Resiliencia
- Pendiente.

## 7. Seguridad y privacidad
- Tres usuarios de PostgreSQL: `admin` (solo crea la base), `app` (lectura y escritura, lo usan los servicios) y `grafana_ro` (solo SELECT). Verificado en la Fase 1: `grafana_ro` recibe `permission denied` al intentar un INSERT.
- Las contraseñas de los usuarios se leen en `02_roles.sql` con `\getenv` de psql, así no quedan escritas en ningún archivo del repositorio.
- PostgreSQL, Redis y el puerto externo de Kafka se publican solo en 127.0.0.1: no son accesibles desde otras máquinas de la red.
- Las contraseñas y la clave HMAC de placas se generan aleatoriamente (generador criptográfico) y viven solo en `.env`, que no se sube a git. Se entrega `.env.example` con valores de ejemplo.

## 8. Observabilidad y SLOs
- Pendiente.

## Otras decisiones
- Un solo lenguaje (Python) para todos los servicios en lugar de ingestores en Java y Node como en el diagrama: pendiente de argumentar.
- JSON con `schemaVersion` frente a Avro con Schema Registry: pendiente de argumentar.
