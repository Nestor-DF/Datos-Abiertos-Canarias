import os
import sys

# Permite ejecutar este archivo tanto como módulo (`python -m src.main`)
# como directamente (`python src/main.py` o `/app/src/main.py`).
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import logging
import time
from sqlalchemy.exc import SQLAlchemyError
from src.database.connection import engine, Base
from src.database.models import DatasetContentMeta  # necesario para que Base registre la tabla
from src.database.audit import log_database_status
from src.pipeline.phase_02_extraction import run_extraction
from src.pipeline.phase_03_metrics import calculate_metrics
from src.pipeline.phase_04_visualization import generate_report

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logging.info("Iniciando auditoría de Datos Abiertos de Canarias")
    
    # Asegurar base de datos y esquema
    Base.metadata.create_all(bind=engine)

    # Mostrar estado inicial de la base de datos.
    # Si la BD contiene miles de tablas dinámicas, la inspección completa puede
    # superar los límites de memoria compartida de PostgreSQL. En ese caso,
    # registramos el fallo y continuamos con la ejecución.
    try:
        log_database_status()
    except SQLAlchemyError as exc:
        logging.warning(
            "No se pudo completar la auditoría inicial de la base de datos. "
            "La ejecución continuará igualmente. Detalle: %s",
            exc,
        )
    
    # Fase 1 y Fase 2: Extracción y recuento
    logging.info("--- Comenzando Extracción de Datos (Fase 1 y 2) ---")
    start_time = time.time()
    run_extraction()
    logging.info(f"Extracción completada en {time.time() - start_time:.2f}s")
    
    # Fase 3: KPIs
    logging.info("--- Calculando Métricas (Fase 3) ---")
    calculate_metrics()
    
    # Fase 4: Reporte HTML
    logging.info("--- Generando Visualización (Fase 4) ---")
    generate_report()
    
    logging.info("Ejecución finalizada con éxito. El reporte está disponible en data/report.html")

if __name__ == "__main__":
    main()