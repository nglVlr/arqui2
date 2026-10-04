# Notas para los ADRs

Este archivo se va llenando durante la implementación. Por cada tema: decisión, alternativas consideradas, por qué y números medidos.
No es el documento final de ADRs; es la materia prima para escribirlos.

## 1. Plataforma de despliegue
- Decisión: on-premise en un solo servidor (laptop con Windows 11 + Docker Desktop sobre WSL2).
- Hardware real medido en la Fase 0: Intel i7-13650HX (20 hilos lógicos), 31.7 GB de RAM.
- A WSL2 se le asignan 20 GB de RAM, 12 procesadores y 4 GB de swap (`.wslconfig`), para dejar memoria libre a Windows durante la demo.
- Pendiente: latencia extremo a extremo medida (Fase 8).

## 2. Broker de eventos
- Pendiente.

## 3. Persistencia
- Pendiente.

## 4. Orquestación de tareas periódicas
- Pendiente.

## 5. Contenerización
- Pendiente.

## 6. Resiliencia
- Pendiente.

## 7. Seguridad y privacidad
- Las contraseñas y la clave HMAC de placas se generan aleatoriamente (generador criptográfico) y viven solo en `.env`, que no se sube a git. Se entrega `.env.example` con valores de ejemplo.

## 8. Observabilidad y SLOs
- Pendiente.

## Otras decisiones
- Un solo lenguaje (Python) para todos los servicios en lugar de ingestores en Java y Node como en el diagrama: pendiente de argumentar.
- JSON con `schemaVersion` frente a Avro con Schema Registry: pendiente de argumentar.
