

import logging

from sqlalchemy import inspect, text

from src.database.connection import engine

logger = logging.getLogger(__name__)


def log_database_status():
    """
    Registra el estado inicial de la base de datos antes de comenzar la auditoría.

    Muestra:
    - número total de tablas
    - nombre de cada tabla
    - columnas de cada tabla
    - número de registros de cada tabla
    """
    inspector = inspect(engine)
    table_names = inspector.get_table_names()

    logger.info("========== ESTADO INICIAL DE LA BASE DE DATOS ==========")
    logger.info(f"Número total de tablas: {len(table_names)}")

    if not table_names:
        logger.info("La base de datos no contiene tablas.")
        logger.info("========================================================")
        return

    with engine.connect() as connection:
        for table_name in table_names:
            logger.info(f"Tabla: {table_name}")

            columns = inspector.get_columns(table_name)
            column_names = [column["name"] for column in columns]
            logger.info(f"Columnas: {', '.join(column_names)}")

            try:
                result = connection.execute(
                    text(f'SELECT COUNT(*) FROM "{table_name}"')
                )
                total_records = result.scalar()
                logger.info(f"Registros: {total_records}")
            except Exception as exc:
                logger.warning(
                    f"No se pudo contar registros de la tabla {table_name}: {exc}"
                )

    logger.info("========================================================")