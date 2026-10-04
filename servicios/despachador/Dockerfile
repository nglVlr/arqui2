# Imagen del despachador: proceso Python que consume Kafka (no expone puertos).
FROM python:3.12.15-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY despachador.py .

CMD ["python", "despachador.py"]
