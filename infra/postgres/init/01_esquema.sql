-- Esquema de la base de datos del sistema de monitoreo.
-- PostgreSQL ejecuta este archivo solo la primera vez que se crea el volumen.
-- Lo usan: correlador (events, alerts, slo_violations), despachador
-- (dispatches, invalid_events), Airflow (daily_stats, false_positive_stats),
-- evaluar.py (evaluation_results) y Grafana (lectura de todo).
-- Todos los tiempos son TIMESTAMPTZ para no tener problemas de zona horaria.

-- Sectores del municipio (copia de config/sectores.json, para unir en consultas)
CREATE TABLE zones (
    zone         TEXT PRIMARY KEY,
    zone_name    TEXT NOT NULL,
    tipo         TEXT NOT NULL,
    latitude     DOUBLE PRECISION NOT NULL,
    longitude    DOUBLE PRECISION NOT NULL,
    peso         NUMERIC(4,2) NOT NULL,
    radio_m      INTEGER NOT NULL,
    zona_escolar BOOLEAN NOT NULL,
    limite_kmh   INTEGER NOT NULL
);

-- Cada evento válido procesado por el correlador
CREATE TABLE events (
    event_id     UUID PRIMARY KEY,
    event_type   TEXT NOT NULL,
    zone         TEXT NOT NULL REFERENCES zones(zone),
    device_id    TEXT NOT NULL,
    device_type  TEXT NOT NULL,
    latitude     DOUBLE PRECISION NOT NULL,
    longitude    DOUBLE PRECISION NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL,
    emitted_at   TIMESTAMPTZ,
    ingested_at  TIMESTAMPTZ,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ingestor_id  TEXT,
    scenario_id  TEXT,
    data         JSONB NOT NULL,
    metadata     JSONB
);

-- Alertas generadas por las reglas de correlación
CREATE TABLE alerts (
    alert_id         UUID PRIMARY KEY,
    alert_type       TEXT NOT NULL,
    severity         TEXT NOT NULL,
    status           TEXT NOT NULL,
    zone             TEXT NOT NULL REFERENCES zones(zone),
    latitude         DOUBLE PRECISION,
    longitude        DOUBLE PRECISION,
    score            NUMERIC(4,2),
    rule             TEXT NOT NULL,
    description      TEXT,
    source_event_ids UUID[] NOT NULL,
    scenario_id      TEXT,
    created_at       TIMESTAMPTZ NOT NULL,
    latency_ms       INTEGER
);

-- Eventos rechazados por los ingestores (dead-letter), para el panel de calidad
CREATE TABLE invalid_events (
    id          SERIAL PRIMARY KEY,
    reason      TEXT NOT NULL,
    ingestor_id TEXT,
    received_at TIMESTAMPTZ NOT NULL,
    zone        TEXT,
    original    JSONB
);

-- Cada envío de una alerta a una entidad externa
CREATE TABLE dispatches (
    id         SERIAL PRIMARY KEY,
    alert_id   UUID NOT NULL,
    destino    TEXT NOT NULL,
    protocolo  TEXT NOT NULL,
    enviado_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resultado  TEXT NOT NULL
);

-- Alertas cuya latencia superó el SLO
CREATE TABLE slo_violations (
    id           SERIAL PRIMARY KEY,
    alert_id     UUID NOT NULL,
    latency_ms   INTEGER NOT NULL,
    umbral_ms    INTEGER NOT NULL,
    detectado_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Resumen diario de alertas (lo llena el DAG reporte_diario)
CREATE TABLE daily_stats (
    fecha      DATE NOT NULL,
    zone       TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    severity   TEXT NOT NULL,
    cantidad   INTEGER NOT NULL,
    PRIMARY KEY (fecha, zone, alert_type, severity)
);

-- Proporción de pánicos no confirmados por sector (DAG analisis_falsos_positivos)
CREATE TABLE false_positive_stats (
    id                  SERIAL PRIMARY KEY,
    calculado_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    zone                TEXT NOT NULL,
    panicos_confirmados INTEGER NOT NULL,
    panicos_no_confirmados INTEGER NOT NULL,
    proporcion_fp       NUMERIC(5,4) NOT NULL
);

-- Resultado de evaluar.py: alerta esperada frente a la obtenida por escenario
CREATE TABLE evaluation_results (
    id              SERIAL PRIMARY KEY,
    evaluado_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    scenario_id     TEXT NOT NULL,
    alerta_esperada TEXT NOT NULL,
    alertas_obtenidas TEXT,
    resultado       TEXT NOT NULL
);

-- Índices para las consultas por tiempo y sector de Grafana y Airflow
CREATE INDEX idx_events_occurred_at ON events (occurred_at);
CREATE INDEX idx_events_zone_occurred_at ON events (zone, occurred_at);
CREATE INDEX idx_events_event_type ON events (event_type);
CREATE INDEX idx_alerts_created_at ON alerts (created_at);
CREATE INDEX idx_alerts_severity_created_at ON alerts (severity, created_at);
