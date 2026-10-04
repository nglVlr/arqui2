# Imagen del simulador: un solo contenedor con todos los scripts de simulación.
# Se usa con: docker compose run --rm simulador python <script>.py ...
FROM python:3.12.15-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .

# Sin argumentos muestra la ayuda del simulador principal
ENTRYPOINT ["python"]
CMD ["simulador.py", "--help"]
