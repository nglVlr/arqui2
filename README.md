# Monitoreo Urbano San Juan Sacatepéquez

Simulación funcional de un sistema de monitoreo perimetral urbano para la Municipalidad de San Juan Sacatepéquez: detección, procesamiento, correlación y visualización de eventos críticos sobre una arquitectura distribuida orientada a eventos.
Proyecto final de Arquitectura de Computadores II, Universidad Mariano Gálvez (2026).

```
Simuladores (5 sensores, escenarios, reporte manual, JMeter)
        |  HTTP POST
        v
     NGINX :8080  ->  ingestor-1 / ingestor-2 / ingestor-3  (FastAPI: valida y estandariza)
                             |                 \
                             v                  -> Kafka events.invalid (dead-letter)
                  Kafka events.standardized
                             |
                             v
                 correlador <-> Redis (ventanas, vehículos, duplicados)
                             |  guarda en PostgreSQL
                             v
                  Kafka correlated.alerts
                             |
          +------------------+-------------------+
          v                  v                   v
     despachador         Grafana :3000       Airflow :8090
   (PNC, Bomberos,     (PostgreSQL y        (tareas periódicas)
    CONRED, aseguradora  Elasticsearch)
    y Elasticsearch)
```

## Requisitos

- Windows 11 con **Docker Desktop** (motor WSL2). No hace falta instalar Python ni nada más: todo corre en contenedores.
- Git.
- Recomendado: 20 GB de RAM para WSL2. Archivo `%USERPROFILE%\.wslconfig`:
  ```ini
  [wsl2]
  memory=20GB
  processors=12
  swap=4GB
  ```
  Después de crearlo o cambiarlo: `wsl --shutdown` y volver a abrir Docker Desktop.
- Puertos libres: 3000, 5432, 6379, 8080, 8085, 8090, 9094 y 9200.
- Internet en el navegador que abre Grafana (los mapas descargan el fondo de OpenStreetMap).

## Puesta en marcha paso a paso

Todos los comandos se ejecutan en PowerShell, dentro de la carpeta del proyecto.

### 1. Clonar el repositorio

```powershell
git clone <url-del-repositorio> smartcity-sjs
cd smartcity-sjs
```

### 2. Crear el archivo `.env` con contraseñas aleatorias

El archivo `.env` guarda las contraseñas y la clave HMAC de las placas. No se sube a git. Este comando lo crea a partir de `.env.example` y reemplaza cada valor `CAMBIAR_...` por uno aleatorio:

```powershell
$t = Get-Content .env.example -Raw
while ($t -match 'CAMBIAR_[A-Z]+') { $b = New-Object byte[] 16; [Security.Cryptography.RandomNumberGenerator]::Fill($b); $t = $t.Replace($matches[0], -join ($b | % { '{0:x2}' -f $_ })) }
$b = New-Object byte[] 32; [Security.Cryptography.RandomNumberGenerator]::Fill($b); $t = $t -replace 'PLATE_HMAC_KEY=\w+', ('PLATE_HMAC_KEY=' + -join ($b | % { '{0:x2}' -f $_ }))
[IO.File]::WriteAllText("$PWD\.env", $t)
```

Para la demo conviene bajar la ventana de correlación a 60 s editando `.env`: `CORRELATION_WINDOW_SECONDS=60`.

### 3. Levantar el sistema

Hay dos perfiles (usar uno a la vez):

| Perfil | Qué levanta | Cuándo usarlo |
|---|---|---|
| `core` | Kafka, Kafka UI, PostgreSQL, Redis, 3 ingestores, NGINX, correlador, despachador, aseguradora y Grafana | Desarrollo y pruebas rápidas |
| `full` | Todo lo de `core` más Elasticsearch y Airflow | Pruebas finales y demo |

```powershell
docker compose --profile full up -d --build
docker compose ps
```

La primera vez descarga las imágenes (unos 5 GB) y tarda varios minutos. Cuando `kafka`, `postgres`, `redis` y `elasticsearch` aparecen como `healthy`, el sistema está listo.

### 4. Cargar la lista de placas robadas (una sola vez)

```powershell
docker compose run --rm simulador cargar_buscados.py
```

### 5. Generar tráfico

```powershell
docker compose run --rm simulador simulador.py normal --duracion 120
```

## Direcciones y usuarios

| Servicio | URL | Usuario | Contraseña |
|---|---|---|---|
| Grafana (dashboards) | http://localhost:3000 | `admin` (`GRAFANA_ADMIN_USER`) | `GRAFANA_ADMIN_PASSWORD` del `.env` |
| Kafka UI | http://localhost:8085 | sin usuario | |
| Airflow (perfil full) | http://localhost:8090 | `admin` | `AIRFLOW_ADMIN_PASSWORD` del `.env` |
| Entrada de eventos (NGINX) | http://localhost:8080/events y /events/batch | | |
| Elasticsearch (perfil full) | http://localhost:9200 (solo desde esta máquina) | | |
| PostgreSQL | localhost:5432, base `monitoreo` (solo desde esta máquina) | `admin`, `app` o `grafana_ro` | en el `.env` |

Para ver una contraseña: `Select-String -Path .env -Pattern GRAFANA_ADMIN_PASSWORD`.

Dashboards en Grafana (carpeta "Monitoreo SJS"):
1. **Centro de Monitoreo San Juan Sacatepéquez**: mapa de calor, alertas en el mapa, tabla de alertas y contadores.
2. **Operación y SLOs**: eventos por segundo, latencia p50 y p95, inválidos, balanceo por ingestor, violaciones de SLO y envíos externos.
3. **Histórico** (perfil full): búsqueda en Elasticsearch y estadísticas de Airflow.
4. **Bandeja de instituciones**: una tabla por institución (PNC, Bomberos, CONRED y aseguradora) con las alertas que recibió cada una, las más recientes primero (tabla `dispatches`). Clic en una celda para ver la descripción completa.

## Scripts del simulador

Todos se ejecutan con `docker compose run --rm simulador <script> [opciones]`:

| Comando | Qué hace |
|---|---|
| `simulador.py normal [--duracion 120]` | Tráfico continuo de los 5 sensores (unos 5 eventos/s, entre 1 y 2 % inválidos a propósito). Sin `--duracion` corre hasta Ctrl+C. |
| `simulador.py pico --multiplicador 20 --duracion 60 [--hilos 16]` | Prueba de carga: multiplica las tasas. |
| `simulador.py escenario todos --velocidad 2` | Ejecuta los 9 escenarios guionados a la vez (`--velocidad` divide las esperas). |
| `simulador.py escenario ESC-ROBO-01` | Un escenario: ESC-ROBO-01, ESC-ARMADO-01, ESC-ACCIDENTE-01, ESC-INCENDIO-01, ESC-TRAMO-01, ESC-ESCOLAR-01, ESC-BUSCADO-01, ESC-FALSO-01, ESC-RUIDO-01. |
| `evaluar.py [--minutos 15]` | Compara las alertas obtenidas con las esperadas por escenario y guarda el resultado en `evaluation_results`. |
| `reportar.py` | Reporte ciudadano manual e interactivo (pregunta sector, tipo y mensaje). |
| `cargar_buscados.py` | Carga en Redis los hashes de las placas reportadas como robadas. |

Notas sobre los escenarios:
- Con ventana de 60 s usar `--velocidad 2` o más: ESC-ACCIDENTE-01 separa sus eventos exactamente 60 s.
- ESC-ESCOLAR-01 solo genera alerta dentro del horario escolar (`SCHOOL_HOURS`, por defecto 06:30-08:00 y 12:00-13:30). Para probarlo a cualquier hora: `SCHOOL_HOURS=00:00-23:59` en `.env` y luego `docker compose --profile full up -d correlador`.
- Los falsos positivos (ESC-FALSO-01, ESC-RUIDO-01) aparecen al cerrar la ventana: esperar la ventana más 10 s antes de `evaluar.py`.
- Para evaluar escenarios conviene correrlos sin tráfico normal en paralelo: una alerta igual del tráfico normal en el mismo sector activa el cooldown y bloquea la del escenario.

## Pruebas de carga con JMeter

```powershell
docker run --rm --network monitoreo-sjs_default -v "${PWD}\pruebas\jmeter:/pruebas" justb4/jmeter:5.5 `
  -n -t /pruebas/carga.jmx -Jhost=nginx -Jpuerto=80 -Jhilos=16 -Jduracion=60 -Jtamano_lote=50 -l /pruebas/resultados.jtl
```

Parámetros opcionales: `hilos`, `rampa`, `duracion`, `tamano_lote` (máximo 500), `host` y `puerto`. Los eventos llevan `scenarioId = JMETER`. Resultados medidos: `docs/resultados-pruebas.md`.

## Comandos útiles

```powershell
docker compose ps                                   # estado de los servicios
docker compose logs -f correlador                   # ver alertas en vivo
docker compose logs -f despachador-es               # avisos a entidades (perfil full; en core: despachador)
docker compose stop ingestor-2                      # simular la caída de un ingestor
docker compose start ingestor-2
docker compose restart grafana                      # después de editar un dashboard JSON
docker stats                                        # CPU y memoria por contenedor
```

## Detener todo

```powershell
docker compose --profile full down       # detiene y borra contenedores; los datos quedan en los volúmenes
docker compose --profile full down -v    # además borra TODOS los datos (Kafka, PostgreSQL, Redis, Elasticsearch, Grafana, Airflow)
```

Para cambiar de perfil: `docker compose --profile core down` y luego `docker compose --profile full up -d` (o al revés).

## Estructura del proyecto

```
docker-compose.yml        servicios y perfiles
.env.example              variables de ejemplo (copiar a .env)
config/sectores.json      los 7 sectores del municipio
infra/                    NGINX, creación de topics de Kafka, SQL de PostgreSQL, Grafana
servicios/ingestor/       API REST de ingestión (FastAPI)
servicios/correlador/     reglas de correlación R01 a R07 y falsos positivos
servicios/despachador/    envío a PNC, Bomberos, CONRED, aseguradora y Elasticsearch
servicios/aseguradora_mock/  aseguradora simulada (REST)
simulador/                generadores de eventos, escenarios, reporte manual y evaluación
airflow/dags/             retención de datos, reporte diario y análisis de falsos positivos
pruebas/jmeter/           plan de carga
docs/                     notas para los ADRs, resultados de pruebas y guion de la demo
```

## Privacidad

- Las placas nunca viajan ni se guardan en claro: la cámara (simulador) envía el HMAC-SHA256 con la clave `PLATE_HMAC_KEY` y una versión parcial (`P-***BCD`). El ingestor rechaza con `PLACA_EN_CLARO` cualquier evento que traiga una placa.
- Retención: eventos e inválidos 30 días, alertas 365 días (DAG `retencion_datos`).
- Grafana usa el usuario `grafana_ro`, que solo puede leer.
- Elasticsearch corre sin seguridad **solo** porque es un laboratorio en una sola máquina (puerto publicado únicamente en 127.0.0.1).
