# Imagen del ingestor: Python 3.12 + FastAPI servido con uvicorn.
FROM python:3.12.15-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

# Primero las dependencias: Docker reutiliza esta capa si solo cambia el código
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .

EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
