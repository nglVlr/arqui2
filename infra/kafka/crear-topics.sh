#!/bin/bash
# Crea los topics de Kafka del sistema de monitoreo y termina.
# Lo ejecuta el contenedor kafka-init una sola vez, cuando kafka ya está sano.
# Usamos --if-not-exists para que se pueda ejecutar varias veces sin error.
# La creación automática de topics está desactivada en el broker, así que
# este script es el único lugar donde se definen particiones y retención.
set -e

SERVIDOR="kafka:9092"
RETENCION_MS=604800000   # 7 días en milisegundos

crear_topic() {
  nombre=$1
  particiones=$2
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$SERVIDOR" --create --if-not-exists \
    --topic "$nombre" --partitions "$particiones" --replication-factor 1 \
    --config retention.ms=$RETENCION_MS
  echo "Topic listo: $nombre ($particiones particiones)"
}

# 3 particiones donde hay más volumen, para poder repartir el trabajo por zona
crear_topic events.standardized 3
crear_topic events.invalid 1
crear_topic correlated.alerts 3
crear_topic external.pnc 1
crear_topic external.bomberos 1
crear_topic external.conred 1

echo "Topics existentes:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$SERVIDOR" --list
