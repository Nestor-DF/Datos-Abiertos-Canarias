import json
import logging
import pandas as pd
import requests
import datetime
import io
from sqlalchemy.orm import Session

from src.database.connection import SessionLocal
from src.database.models import Source, Dataset, Resource, ExecutionLog
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
            elif isinstance(data, dict):
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

def run_extraction():
    state = CheckpointManager()
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
        
        # Actualmente sólo instanciamos modelo CKAN. Si hubiese DCAT sería otra clase.
        extractor = CkanExtractor(source_id, scfg["url"])
        
        try:
            dataset_ids = extractor.get_datasets_list()
        except Exception as e:
            log_error(db, source_id, f"Error listando datasets: {str(e)}")
            state.mark_source_completed(source_id)
            continue
            
        logging.info(f"Encontrados {len(dataset_ids)} datasets en {source_id}")
        
        for ds_id in dataset_ids:
            if state.is_dataset_processed(ds_id):
                continue
                
            try:
                ds_info = extractor.get_dataset_details(ds_id)
                if not ds_info:
                    state.mark_dataset_processed(ds_id)
                    continue
                    
                # Guardar Dataset
                dataset = db.query(Dataset).filter_by(id=ds_info["id"]).first()
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
                
                # Procesar Recursos
                for res_info in ds_info["resources"]:
                    resource = db.query(Resource).filter_by(id=res_info["id"]).first()
                    if not resource:
                        resource = Resource(
                            id=res_info["id"],
                            dataset_id=dataset.id,
                            title=res_info["title"],
                            format=res_info["format"],
                            url=res_info["url"]
                        )
                        db.add(resource)
                    
                    # Siempre re-contamos para actualizar (o no, si queremos ser rápidos)
                    # Aquí es donde descargamos y contamos de verdad
                    if res_info.get("url"):
                        records = count_records(res_info["url"], res_info["format"])
                        resource.records_count = records
                
                db.commit()
                state.mark_dataset_processed(ds_id)
                logging.info(f"Guardado DS {ds_id} con {len(ds_info['resources'])} recursos.")
                
            except Exception as e:
                db.rollback()
                log_error(db, source_id, f"Error procesando dataset {ds_id}: {str(e)}", dataset_id=ds_id)
                # Omitimos el dataset y marcamos como procesado para no encallarnos para siempre
                state.mark_dataset_processed(ds_id)

        # Fuente completada
        state.mark_source_completed(source_id)
        logging.info(f"Fuente {source_id} procesada al 100%.")

    db.close()
