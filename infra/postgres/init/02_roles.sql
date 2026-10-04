-- Usuarios de la base de datos (principio de mínimo privilegio).
-- app: lo usan correlador, despachador, Airflow y el simulador (lectura y escritura).
-- grafana_ro: lo usa Grafana, solo puede hacer SELECT.
-- Las contraseñas vienen de variables de entorno del contenedor (.env);
-- \getenv es un comando de psql que lee una variable de entorno, así no
-- escribimos contraseñas dentro de este archivo.

\getenv app_user APP_DB_USER
\getenv app_password APP_DB_PASSWORD
\getenv ro_password GRAFANA_RO_PASSWORD

CREATE ROLE :"app_user" LOGIN PASSWORD :'app_password';
CREATE ROLE grafana_ro LOGIN PASSWORD :'ro_password';

GRANT CONNECT ON DATABASE :"DBNAME" TO :"app_user", grafana_ro;
GRANT USAGE ON SCHEMA public TO :"app_user", grafana_ro;

-- La aplicación lee y escribe; también usa las secuencias de las columnas SERIAL
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO :"app_user";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO :"app_user";

-- Grafana solo lee
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_ro;
