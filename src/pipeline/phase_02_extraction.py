import json
import logging
import re
import pandas as pd
import requests
import datetime
import io
from sqlalchemy.orm import Session
from sqlalchemy import Table, Column, Text, MetaData, inspect, text

from src.database.connection import SessionLocal, engine
from src.database.models import Source, Dataset, Resource, ExecutionLog, DatasetContentMeta
from src.utils.state import CheckpointManager
from src.extractors.ckan import CkanExtractor
from src.config import DEBUG_MODE, MAX_RECORDS_DOWNLOAD, SOURCES

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_error(db: Session, source_id: str, message: str, dataset_id: str = None):
    logging.error(f"[{source_id}] {message}")
    error = ExecutionLog(source_id=source_id, dataset_id=dataset_id, level="ERROR", message=message)
    db.add(error)
    db.commit()

def count_records(url: str, format_type: str) -> int:
    """Descarga el contenido de forma limitada y cuenta los registros."""
    format_type = format_type.upper() if format_type else ""
    
    # Archivos no tabulares tendrán valor de 1 (una fila x una columna)
    if format_type not in ["CSV", "JSON", "GEOJSON", "XLS", "XLSX"]:
        return 1

    try:
        # Petición de descarga (usamos stream para no colapsar la memoria)
        headers = {"User-Agent": "AuditorDatosabiertosCanarias/1.0"}
        
        # En caso de JSON usamos carga normal pero podemos capturar excepciones
        if format_type in ["JSON", "GEOJSON"]:
            response = requests.get(url, headers=headers, timeout=20)
            response.raise_for_status()
            data = response.json()
            
            if isinstance(data, list):
                count = len(data)
            elif isinstance(data, dict): # TODO: Revisar esta parte, formatos de los datos cuando venga de un JSON.
                # Caso común en CKAN (datastore_search) o GeoJSON
                if "result" in data and "records" in data["result"]:
                    count = len(data["result"]["records"])
                elif "features" in data:
                    count = len(data["features"])
                else:
                    count = 1 # Json complejo
            else:
                count = 1
                
            return min(count, MAX_RECORDS_DOWNLOAD) if DEBUG_MODE else count

        elif format_type in ["CSV", "TXT", "TSV"]:
            response = requests.get(url, headers=headers, stream=True, timeout=20)
            response.raise_for_status()
            
            count = 0
            # Leer por líneas
            for line in response.iter_lines():
                if line:
                    count += 1
                # LÍMITE por configuración
                if DEBUG_MODE and count >= MAX_RECORDS_DOWNLOAD:
                    break
            
            # Restamos 1 por la cabecera, si count > 0
            return max(1, count - 1)
            
        elif format_type in ["XLS", "XLSX"]:
            # Excel no se puede stremear fácilmente con requests puro sin volcarlo,
            # lo bajamos y contamos con pandas.
            response = requests.get(url, headers=headers, timeout=20)
            response.raise_for_status()
            df = pd.read_excel(io.BytesIO(response.content))
            count = len(df)
            return min(count, MAX_RECORDS_DOWNLOAD) if DEBUG_MODE else count

    except Exception as e:
        # Fallo en la descarga (timeout, 404, etc.)
        # Retornamos 0 para indicar que no se pudo acceder a los datos
        logging.warning(f"Error count_records para {url}: {str(e)}")
        return 0

TABULAR_FORMATS = {"CSV", "TSV", "JSON", "GEOJSON", "XLS", "XLSX"}

def _safe_table_name(dataset_id: str) -> str:
    """Convierte un dataset_id a un nombre de tabla válido en PostgreSQL."""
    name = re.sub(r"[^a-z0-9]", "_", dataset_id.lower())
    name = re.sub(r"_+", "_", name).strip("_")
    # Los nombres de tabla en PG tienen límite de 63 caracteres
    return f"ds_{name}"[:63]


def download_resource_content(url: str, format_type: str) -> pd.DataFrame:
    """
    Descarga el recurso más reciente y devuelve su contenido como DataFrame.
    Soporta CSV, TSV, JSON, GeoJSON, XLS y XLSX.
    Lanza excepción si el formato no es tabular o la descarga falla.
    """
    fmt = format_type.upper() if format_type else ""
    if fmt not in TABULAR_FORMATS:
        raise ValueError(f"Formato no tabular: {fmt!r} — se omite")

    headers = {"User-Agent": "AuditorDatosabiertosCanarias/1.0"}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    if fmt in ("CSV", "TSV"):
        sep = "\t" if fmt == "TSV" else ","
        df = pd.read_csv(io.BytesIO(response.content), sep=sep, dtype=str)

    elif fmt in ("XLS", "XLSX"):
        df = pd.read_excel(io.BytesIO(response.content), dtype=str)

    elif fmt in ("JSON", "GEOJSON"):
        data = response.json()
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            if "result" in data and "records" in data["result"]:
                df = pd.DataFrame(data["result"]["records"])
            elif "features" in data:
                # GeoJSON: aplanamos propiedades
                rows = [f.get("properties", {}) for f in data["features"]]
                df = pd.DataFrame(rows)
            else:
                df = pd.DataFrame([data])
        else:
            df = pd.DataFrame()
    else:
        df = pd.DataFrame()

    # Normalizar nombres de columna para que sean válidos en SQL
    df.columns = [
        re.sub(r"[^a-z0-9]", "_", str(c).lower().strip())[:60] or f"col_{i}"
        for i, c in enumerate(df.columns)
    ]
    return df


def save_dataset_content(db: Session, dataset_id: str, resource: dict) -> int:
    """
    Crea (o actualiza) la tabla de contenido para un dataset.

    Lógica de actualización:
    - Si no existe la tabla → crearla e insertar todas las filas.
    - Si existe y la fecha del recurso es la misma → skip.
    - Si existe y la fecha del recurso es más nueva → append de las filas nuevas.

    Devuelve el número de filas insertadas (0 si se ha saltado).
    """
    table_name = _safe_table_name(dataset_id)
    res_id = resource["id"]
    res_modified = resource.get("last_modified")  # puede ser None

    # Comprobar metadatos existentes
    meta_row = db.query(DatasetContentMeta).filter_by(dataset_id=dataset_id).first()

    if meta_row:
        # Misma fecha (o ambas None) → skip
        if meta_row.resource_last_modified == res_modified:
            logging.info(f"[{dataset_id}] Contenido ya actualizado, se omite.")
            return 0
        # Fecha anterior → se va a hacer append; si la fecha nueva es None
        # y la existente también lo era ya lo capturamos arriba; si la nueva
        # es None pero la existente no, tampoco actualizamos por seguridad.
        if res_modified is None:
            logging.info(f"[{dataset_id}] Recurso sin fecha — no se puede comparar, se omite.")
            return 0

    # Descargar contenido
    df = download_resource_content(resource["url"], resource["format"])
    if df.empty:
        logging.warning(f"[{dataset_id}] DataFrame vacío tras descargar {resource['url']}")
        return 0

    # Añadir columna de trazabilidad
    df["_resource_id"] = res_id
    df["_ingested_at"] = datetime.datetime.now().isoformat()

    insp = inspect(engine)
    table_exists = insp.has_table(table_name)

    if not table_exists:
        # Crear tabla con columnas Text para máxima compatibilidad
        meta = MetaData()
        cols = [Column("_row_id", Text, primary_key=True)] + [
            Column(c, Text) for c in df.columns
        ]
        tbl = Table(table_name, meta, *cols)
        meta.create_all(engine)
        logging.info(f"[{dataset_id}] Tabla '{table_name}' creada.")

    # Insertar filas (append)
    df.to_sql(table_name, engine, if_exists="append", index=True, index_label="_row_id")
    inserted = len(df)
    logging.info(f"[{dataset_id}] {inserted} filas insertadas en '{table_name}'.")

    # Actualizar metadatos
    now = datetime.datetime.now()
    if meta_row:
        meta_row.resource_id = res_id
        meta_row.resource_last_modified = res_modified
        meta_row.row_count = (meta_row.row_count or 0) + inserted
        meta_row.updated_at = now
    else:
        meta_row = DatasetContentMeta(
            dataset_id=dataset_id,
            resource_id=res_id,
            resource_last_modified=res_modified,
            table_name=table_name,
            row_count=inserted,
            created_at=now,
            updated_at=now,
        )
        db.add(meta_row)
    db.commit()

    return inserted


def run_extraction():
    state = CheckpointManager()
    # Resetear la lista de contenido comprobado al inicio de cada ejecución,
    # para que los datasets ya procesados en ejecuciones anteriores vuelvan
    # a comprobarse en busca de contenido más nuevo.
    state.reset_content_checked()

    db: Session = SessionLocal()

    # Pre-cargar las fuentes en la BD
    for s in SOURCES:
        if not db.query(Source).filter_by(id=s["id"]).first():
            new_source = Source(id=s["id"], name=s["name"], url=s["url"], type=s["type"])
            db.add(new_source)
    db.commit()

    for scfg in SOURCES:
        source_id = scfg["id"]
        
        if state.is_source_completed(source_id):
            logging.info(f"Saltando fuente ya completada: {source_id}")
            continue
            
        state.set_current_source(source_id)
        logging.info(f"Procesando fuente: {scfg['name']}")
        
        extractor = CkanExtractor(source_id, scfg["url"])
        
        try:
            dataset_ids = extractor.get_datasets_list()
        except Exception as e:
            log_error(db, source_id, f"Error listando datasets: {str(e)}")
            state.mark_source_completed(source_id)
            continue
            
        logging.info(f"Encontrados {len(dataset_ids)} datasets en {source_id}")
        
        for ds_id in dataset_ids:
            already_processed = state.is_dataset_processed(ds_id)
            content_checked = state.is_dataset_content_checked(ds_id)

            # Si ya fue procesado Y el contenido ya fue comprobado en esta ejecución → skip total
            if already_processed and content_checked:
                continue

            try:
                ds_info = extractor.get_dataset_details(ds_id)
                if not ds_info:
                    state.mark_dataset_processed(ds_id)
                    state.mark_dataset_content_checked(ds_id)
                    continue

                # Guardar / actualizar Dataset y Recursos solo si no estaba procesado
                dataset = db.query(Dataset).filter_by(id=ds_info["id"]).first()
                if not already_processed:
                    if not dataset:
                        dataset = Dataset(
                            id=ds_info["id"],
                            source_id=source_id,
                            title=ds_info["title"],
                            last_updated=ds_info["last_updated"]
                        )
                        db.add(dataset)
                    else:
                        dataset.last_updated = ds_info["last_updated"]

                    # Procesar Recursos (guardar todos en BD para métricas)
                    for res_info in ds_info["resources"]:
                        resource = db.query(Resource).filter_by(id=res_info["id"]).first()
                        # TODO 2: Añadir el conteo de formatos diferentes y total de recursos por fuente y dataset
                        if not resource:
                            resource = Resource(
                                id=res_info["id"],
                                dataset_id=dataset.id,
                                title=res_info["title"],
                                format=res_info["format"],
                                url=res_info["url"]
                            )
                            db.add(resource)

                        if res_info.get("url"):
                            records = count_records(res_info["url"], res_info["format"])
                            resource.records_count = records

                # --- TODO 1: Guardar contenido del recurso más reciente ---
                # Se ejecuta siempre (incluso si el dataset ya estaba procesado)
                # porque save_dataset_content compara fechas y decide si actualizar o no.
                if not content_checked:
                    tabular_resources = [
                        r for r in ds_info["resources"]
                        if r.get("format", "").upper() in TABULAR_FORMATS and r.get("url")
                    ]
                    if tabular_resources:
                        # Si ninguno tiene fecha, usamos el primero de la lista como fallback
                        latest_resource = max(
                            tabular_resources,
                            key=lambda r: r.get("last_modified") or datetime.datetime.min
                        )
                        try:
                            save_dataset_content(db, dataset.id, latest_resource)
                        except ValueError as e:
                            logging.info(f"[{ds_id}] {e}")
                        except Exception as e:
                            logging.warning(f"[{ds_id}] Error guardando contenido: {e}")
                    else:
                        logging.info(f"[{ds_id}] Sin recursos tabulares, no se guarda contenido.")
                    state.mark_dataset_content_checked(ds_id)

                db.commit()
                if not already_processed:
                    state.mark_dataset_processed(ds_id)
                    logging.info(f"Guardado DS {ds_id} con {len(ds_info['resources'])} recursos.")

            except Exception as e:
                db.rollback()
                log_error(db, source_id, f"Error procesando dataset {ds_id}: {str(e)}", dataset_id=ds_id)
                state.mark_dataset_processed(ds_id)
                state.mark_dataset_content_checked(ds_id)

        # Fuente completada
        state.mark_source_completed(source_id)
        logging.info(f"Fuente {source_id} procesada al 100%.")

    db.close()