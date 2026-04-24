# Auditoría y Ranking de Datos Abiertos de Canarias

Este proyecto es un sistema hiperautomatizado construido en Python diseñado para auditar, puntuar y generar un informe de calidad sobre los portales de Datos Abiertos de la Comunidad Autónoma de Canarias (Gobierno, Cabildos y Ayuntamientos). 

Utiliza orquestación en contenedores y PostgreSQL para medir la *frescura*, *volatilidad* y cantidad de *registros* por cada conjunto de datos en base a las APIs de CKAN y otras fuentes de metadatos estandarizadas.

## Requisitos previos

- Docker y Docker Compose instalados (`sudo apt install docker-compose` o plugin de `docker compose`).
- (Opcional): Python 3.10+ para ejecución local.

## Puesta en marcha rápida (Docker Integrado)

1. **Configurar el entorno:**
   Copia el archivo de configuración de ejemplo y ajústalo si fuese necesario (especialmente útil configurar el `MAX_RECORDS_DOWNLOAD=1000` si quieres acotar la velocidad):
   ```bash
   cp .env.example .env
   ```

2. **Levantar todos los servicios:**
   Asegúrate de que el puerto `5432` de tu máquina esté disponible. Luego levanta tanto la base de datos como el pipeline:
   ```bash
   # Opción A (Para versiones de docker recientes):
   docker compose up -d db
   docker compose up app
   
   # Opción B (Si dispones del paquete antiguo docker-compose):
   docker-compose up -d db
   docker-compose up app
   ```

El archivo final auto-contenido aparecerá en la ruta `data/report.html`.

## Ejecución local sin el contenedor App

Si tu servicio de Docker se encuentra tras un firewall estricto o un error temporal de red te impide usar la imagen `python:slim`, puedes levantar únicamente la Base de Datos y ejecutar el pipeline localmente:

1. Levanta la Base de datos:
   ```bash
   docker compose up -d db
   ```
2. Instala dependencias y arranca el entorno:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
3. Ejecutar el código base:
   ```bash
   PYTHONPATH=. python src/main.py
   ```

## Resiliencia y Control de Ejecución
La aplicación está diseñada para retener el progreso (_checkpointing_). Si necesitas paralizar una descarga masiva (`Ctrl+C`), el sistema guardará su estado en el archivo `execution_state.json`.

Si deseas relanzar la recolección desde 0, primero debes vaciar este caché y resetear la Base de Datos:
```bash
docker compose down -v
rm -f execution_state.json
docker compose up -d db
```
