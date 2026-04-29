import logging

from sqlalchemy import text

from src.database.connection import engine

logger = logging.getLogger(__name__)


def log_database_status():
    """
    Registra un resumen seguro del estado inicial de la base de datos antes de
    comenzar la auditoría.

    Muestra:
    - número total de tablas
    - detalle completo de tablas principales
    - resumen limitado de tablas dinámicas `ds_*`

    No inspecciona columnas de todas las tablas dinámicas porque, cuando hay
    miles de tablas `ds_*`, PostgreSQL puede agotar memoria compartida/locks
    durante la reflexión de SQLAlchemy.
    """
    logger.info("========== ESTADO INICIAL DE LA BASE DE DATOS ==========")

    with engine.connect() as connection:
        table_names = connection.execute(text("""
            SELECT tablename
            FROM pg_catalog.pg_tables
            WHERE schemaname = 'public'
            ORDER BY tablename
        """)).scalars().all()

        logger.info(f"Número total de tablas: {len(table_names)}")

        if not table_names:
            logger.info("La base de datos no contiene tablas.")
            logger.info("========================================================")
            return

        regular_tables = [table_name for table_name in table_names if not table_name.startswith("ds_")]
        dynamic_tables = [table_name for table_name in table_names if table_name.startswith("ds_")]

        logger.info(f"Tablas principales: {len(regular_tables)}")
        logger.info(f"Tablas dinámicas de datasets (ds_*): {len(dynamic_tables)}")

        # Las tablas principales sí se auditan con todo el detalle.
        for table_name in regular_tables:
            logger.info(f"Tabla: {table_name}")

            columns = connection.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = :table_name
                ORDER BY ordinal_position
            """), {"table_name": table_name}).scalars().all()
            logger.info(f"Columnas: {', '.join(columns) if columns else '(sin columnas)'}")

            try:
                total_records = connection.execute(
                    text(f'SELECT COUNT(*) FROM "{table_name}"')
                ).scalar()
                logger.info(f"Registros: {total_records}")
            except Exception as exc:
                logger.warning(
                    f"No se pudo contar registros de la tabla {table_name}: {exc}"
                )

        # Las tablas dinámicas se resumen para evitar miles de consultas de
        # reflexión y un log inmanejable.
        if dynamic_tables:
            sample_size = 20
            shown_tables = dynamic_tables[:sample_size]

            logger.info(
                f"Mostrando resumen de las primeras {len(shown_tables)} "
                f"tablas dinámicas de {len(dynamic_tables)}."
            )

            for table_name in shown_tables:
                try:
                    total_records = connection.execute(
                        text(f'SELECT COUNT(*) FROM "{table_name}"')
                    ).scalar()
                    logger.info(f"Tabla dinámica: {table_name} | Registros: {total_records}")
                except Exception as exc:
                    logger.warning(
                        f"No se pudo contar registros de la tabla dinámica {table_name}: {exc}"
                    )

            remaining = len(dynamic_tables) - len(shown_tables)
            if remaining > 0:
                logger.info(
                    f"Se omiten {remaining} tablas dinámicas adicionales "
                    "para evitar una auditoría inicial excesiva."
                )

    logger.info("========================================================")