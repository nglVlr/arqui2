# Guion de la demo (jornada de simulación)

Duración estimada: 20 a 25 minutos. Todos los comandos se ejecutan en PowerShell dentro de la carpeta del proyecto.

## 0. Preparación (antes de que llegue el catedrático)

1. Abrir Docker Desktop y esperar a que diga "Engine running".
2. En `.env`, dejar `CORRELATION_WINDOW_SECONDS=60`. Si la demo cae fuera del horario escolar, poner también `SCHOOL_HOURS=00:00-23:59` para mostrar ESC-ESCOLAR-01.
3. Levantar todo y revisar que esté sano:
   ```powershell
   docker compose --profile full up -d
   docker compose ps
   ```
   `kafka`, `postgres`, `redis` y `elasticsearch` deben estar `healthy`; `kafka-init` termina con `Exited (0)` (es normal: solo crea los topics).
4. Cargar las placas robadas: `docker compose run --rm simulador cargar_buscados.py`
5. Abrir en el navegador, cada una en una pestaña:
   - Grafana dashboard 1: http://localhost:3000 (carpeta "Monitoreo SJS")
   - Grafana dashboard 2
   - Kafka UI: http://localhost:8085
   - Airflow: http://localhost:8090
6. Tener 3 terminales abiertas: una para el tráfico, una para escenarios y una para logs.
7. Ensayo rápido: `docker compose run --rm simulador simulador.py escenario ESC-BUSCADO-01` y confirmar que aparece VEHICULO_REPORTADO en la tabla del dashboard 1.

## 1. Presentar la arquitectura (3 min)

- Mostrar el diagrama del README y relacionarlo con el del catedrático: sensores simulados, NGINX, 3 ingestores, Kafka, correlador con Redis, PostgreSQL, despachador, Grafana, Airflow y Elasticsearch.
- `docker compose ps`: cada caja del diagrama es un contenedor. Un solo servidor (ADR 1), pero los servicios solo se comunican por la red, así que la arquitectura es distribuida.

## 2. Tráfico normal y entrada de datos (4 min)

Terminal 1:
```powershell
docker compose run --rm simulador simulador.py normal
```
(Corre hasta Ctrl+C. Dejarlo corriendo durante toda la demo.)

Mostrar:
- **Kafka UI**, topic `events.standardized`: mensajes llegando, con la clave = sector. Abrir uno y señalar `device`, `location`, `data`, `metadata` y `ingestion.ingestorId` (trazabilidad).
- **Kafka UI**, topic `events.invalid`: el dead-letter con el motivo (`PLACA_EN_CLARO`, `ZONA_DESCONOCIDA`, etc.).
- **Dashboard 1**: el mapa de calor se va llenando sobre San Juan Sacatepéquez y los contadores suben.
- **Dashboard 2**: eventos por segundo, porcentaje de inválidos (unos 1.5 %, bajo el umbral de 2 %) y el panel "Eventos por ingestor", que muestra el reparto de NGINX entre los 3 ingestores.

## 3. Escenarios guionados y correlación (6 min)

Terminal 2:
```powershell
docker compose run --rm simulador simulador.py escenario todos --velocidad 2
```

Mientras corre (unos 60 s), en la terminal 3:
```powershell
docker compose logs -f correlador
```
Se ven líneas como `Alerta ROBO_EN_CURSO generada en Ciudad Quetzal (latencia 140 ms)`.

Mostrar en el **dashboard 1**: la tabla de alertas con las críticas en rojo y los puntos en el mapa coloreados por severidad.

Explicar los dos ejemplos del enunciado:
- **ESC-ROBO-01** ("Zona 4, 22:05, placa XYZ123, 85 km/h = robo en curso"): pánico en Ciudad Quetzal, 40 s después una cámara LPR ve un vehículo SALIENDO a 85 km/h (límite 50): regla R01, alerta ROBO_EN_CURSO CRÍTICA. Señalar que la placa nunca aparece en claro: en el evento LPR (Kafka UI) solo están `platePartial` (por ejemplo `P-***BCD`) y `plateHash`.
- **ESC-FALSO-01** ("Zona 10, 17:22, sin eventos complementarios = falso positivo"): un pánico en Montúfar sin nada más. Al cerrar la ventana de 60 s aparece PANICO_NO_CONFIRMADO con estado FALSO_POSITIVO_PROBABLE.

Mostrar los envíos a entidades externas:
```powershell
docker compose logs despachador-es --tail 20
docker compose logs aseguradora-mock --tail 5
```
- En **Kafka UI**: `external.pnc`, `external.bomberos` y `external.conred` con las alertas enrutadas.
- La aseguradora recibió el accidente por REST y respondió con un número de caso.
- En el **dashboard 5** (Despacho y falsos positivos): la tabla "A quién se envía cada tipo de alerta" muestra la regla del despachador junto a los envíos reales, que deben coincidir. En la lista de envíos, filtrar por institución con el embudo del encabezado "Institución".

Esperar unos 70 s después del último escenario (para que se cierren los falsos positivos) y evaluar:
```powershell
docker compose run --rm simulador evaluar.py --minutos 5
```
Mostrar la tabla: 9 de 9 aciertos con su latencia. Explicar la columna "Adicionales": por ejemplo, ESC-ROBO-01 también genera ZONA_ESCOLAR porque el radar marcó 92 km/h en una zona escolar en horario escolar. Es una alerta correcta, no un error.

Luego, en la parte de abajo del **dashboard 5**, explicar cómo un pánico pasa a alerta real (el texto de la izquierda lo resume):
- "Pánicos que pasaron a alerta real": los pánicos de ESC-ROBO-01, ESC-ARMADO-01 y ESC-INCENDIO-01, con el evento que los confirmó, la regla y a quién se avisó.
- "Falsos positivos reales": ESC-FALSO-01 ("ninguno": ningún otro sensor reportó algo) y ESC-RUIDO-01 (hubo un vidrio roto con probabilidad 0.4, que no alcanza el mínimo de 0.7). No se avisó a ninguna institución.
- "Falsos positivos corregidos": pánicos que se marcaron como falsos porque el correlador iba atrasado y que después se confirmaron. No cuentan como falsos.

## 4. Reporte ciudadano manual (2 min)

```powershell
docker compose run --rm simulador reportar.py
```
Elegir un sector, "ACCIDENTE" y escribir un mensaje libre (dato no estructurado). Si se envía dos veces en el mismo sector dentro de la ventana, la regla R03 genera ACCIDENTE_VIAL (dos reportes ciudadanos de accidente).

## 5. Resiliencia: caída y recuperación (4 min)

Con el tráfico normal todavía corriendo:

1. **Caída de un ingestor:**
   ```powershell
   docker compose stop ingestor-2
   ```
   El simulador sigue enviando sin errores. En el dashboard 2, en "Eventos por ingestor", el ingestor-2 deja de recibir. Luego:
   ```powershell
   docker compose start ingestor-2
   ```
   Vuelve a recibir su parte en segundos.

2. **Caída del correlador:**
   ```powershell
   docker compose stop correlador
   ```
   En Kafka UI, el consumer group `correlador` acumula lag: los eventos se guardan en Kafka y no se pierden. Luego:
   ```powershell
   docker compose start correlador
   ```
   El lag vuelve a 0 en segundos. Mostrar en el dashboard 2 el pico de latencia y la violación de SLO registrada: el sistema detecta y registra cuando incumple su objetivo.

3. (Opcional) **Caída de Kafka:** `docker compose stop kafka`. Los ingestores responden 503 y el simulador muestra que reintenta. Después de unos 20 s, `docker compose start kafka`: el simulador continúa sin perder datos (medido: 0 perdidos con 32 s de caída).

## 6. Tareas periódicas e histórico (3 min)

- **Airflow** (http://localhost:8090): mostrar los 3 DAGs. Ejecutar a mano `reporte_diario` y `analisis_falsos_positivos` con el botón de ejecutar (Trigger).
- **Dashboard 3 (Histórico)**: buscar en el cuadro de texto, por ejemplo `alertType:ROBO_EN_CURSO` o `zoneName:Quetzal`; mostrar la tabla de estadísticas diarias y la proporción de falsos positivos por sector (mérito adicional del enunciado).

## 7. Números medidos (2 min)

Mostrar `docs/resultados-pruebas.md`:
- Máximo sostenido de extremo a extremo cumpliendo el SLO de 2 s: unos 510 eventos/s (unas 100 veces el tráfico normal del municipio).
- Ingestión: al menos 26 500 eventos/s con JMeter.
- Caídas de un ingestor, del correlador o de Redis: 0 eventos perdidos y 0 alertas duplicadas.

## Si algo falla durante la demo

| Síntoma | Qué hacer |
|---|---|
| Un contenedor aparece como `Restarting` | `docker compose logs <servicio> --tail 50` y luego `docker compose up -d <servicio>` |
| Grafana sin datos | Revisar el rango de tiempo (arriba a la derecha) y que el simulador siga corriendo |
| Mapas sin fondo de calles | Falta internet en la laptop; los puntos y el mapa de calor siguen funcionando |
| Un escenario no genera su alerta | Probablemente un cooldown activo por tráfico normal en ese sector: esperar 60 s y repetir el escenario solo |
| Todo lento | `docker stats` para ver memoria y CPU; en el peor caso `docker compose --profile full restart` |
