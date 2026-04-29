import datetime
import logging
import calendar
import re
from sqlalchemy import func, Table, MetaData, inspect
from sqlalchemy.orm import Session
from src.database.connection import SessionLocal, engine
from src.database.models import Source, Dataset, Resource, SummaryMetrics, DatasetContentMeta
from src.config import DEBUG_MODE
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def get_date_converter(db: Session, table_name: str):
    """
    Inspecciona la tabla, encuentra la columna de fecha, detecta su formato
    y devuelve una función para convertir los valores a datetime, junto a la columna detectada.
    """
    try:
        inspector = inspect(engine)
        columns = [col['name'] for col in inspector.get_columns(table_name)]
        # Inconveniente: Si una columna tiene parte de una keyword, se va interpretar como la columna escogida de manera errónea
        keywords = ['fecha', 'date', 'año','anios', 'ano', 'anyo', 'mes', 'dia', 'trimestre', 'cuatrimestre', 'tiempo', 'time', 'changed']
        date_col = None
        for col in columns:
            col_lower = col.lower()
            for keyword in keywords:
                if keyword in col_lower:
                    print("Matched ", keyword, "with", col_lower, "table", table_name)
                    date_col = col
                    break
            if date_col is not None:
                break
        if date_col is None:
            if DEBUG_MODE:
                print("No se encontró columna de fecha para la tabla", table_name, "date_col era", date_col)
            return None

        # Obtener una muestra para detectar el formato
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)
        
        sample = db.query(table.c[date_col]).filter(table.c[date_col] != None).limit(1).scalar()

        if not sample:
            if DEBUG_MODE:
                print("sample in table", table_name, "is empty")
            return None

        sample_str = str(sample).strip()
        
        meses_map = {
            "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
            "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
        }
                
        # Formato: DD/MM/AAAA o DD-MM-AAAA
        if re.match(r"^\d{2}[/-]\d{2}[/-]\d{4}$", sample_str):
            sep = "/" if "/" in sample_str else "-"
            return lambda x: datetime.datetime.strptime(x, f"%d{sep}%m{sep}%Y"), date_col

        # Formato: MM/AAAA o MM-AAAA
        if re.match(r"^\d{2}[/-]\d{4}$", sample_str):
            sep = "/" if "/" in sample_str else "-"
            def month_year_converter(date_str):
                partial_date = datetime.datetime.strptime(date_str, f"%m{sep}%Y")
                return datetime.datetime(partial_date.year, partial_date.month, calendar.monthrange(partial_date.year, partial_date.month)[1])
            return month_year_converter, date_col

        # Formato: AAAA (Solo año)
        if re.match(r"^\d{4}$", sample_str):
            return lambda x: datetime.datetime(int(x), 12, 31), date_col

        # Formato: NombreMes AAAA
        if any(m in sample_str.lower() for m in meses_map.keys()):
            def month_year_converter(date_str: str):
                date_str = date_str.lower()
                month, year = date_str.split(" ")
                month = meses_map[month]
                year = int(year)
                return datetime.datetime(year, month, calendar.monthrange(year, month)[1])
            
            return month_year_converter, date_col

        # Formato: Cuatrimestre
        if "cuatrimestre" in sample_str.lower():
            mapping = {"primer": 4, "segundo": 8, "tercer": 12}
            def converter(date_str):
                date_str = date_str.lower()
                ordinal, _, year = date_str.split(" ")
                month = mapping[ordinal]
                year = int(year)
                return datetime.datetime(year, month, calendar.monthrange(year, month)[1])
            return converter, date_col

        # Formato: Trimestre
        if "trimestre" in sample_str.lower():
            mapping = {"primer": 3, "segundo": 6, "tercer": 9, "cuarto": 12}
            def converter(date_str):
                date_str = date_str.lower()
                ordinal, _, year = date_str.split(" ")
                month = mapping[ordinal]
                year = int(year)
                return datetime.datetime(year, month, calendar.monthrange(year, month)[1])
            return converter, date_col

        # Formato: ISO
        iso_regex = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$"
        if re.match(iso_regex, sample_str):
            return lambda x: datetime.datetime.fromisoformat(x), date_col

        # Formato: UNIX timestamp
        try: 
            datetime.datetime.fromtimestamp(float(sample_str.replace("[", "").replace("]", "")))
            return lambda x: datetime.datetime.fromtimestamp(float(x.replace("[", "").replace("]", ""))), date_col
        except:
            pass

        if DEBUG_MODE:
            print("No se encontró un formato de fecha para la tabla", table_name, "con la columna para fecha", date_col)

        return None

    except Exception as error:
        if DEBUG_MODE:
            print("Se produjo un error al intentar encontrar fecha conversor de fecha en la tabla", table_name, ". Error:", error)
        return None

def get_most_recent_date(db: Session, table_name: str):
    """Obtiene la fecha más reciente de la tabla"""
    try:
        converter = get_date_converter(db, table_name)
        if converter and DEBUG_MODE:
            print("Encontrada la columna de fecha en la tabla", table_name, "con nombre", converter[1])
        if converter == None:
            return None

        converter, date_col = converter
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)
        
        all_values = db.query(table.c[date_col]).all()

        # Convertir todos los valores (all devuelve tuplas). No asumimos que
        # todos los registros tengan exactamente el mismo formato que la muestra:
        # algunos datasets mezclan valores como "2025" con otros formatos.
        dates = []
        for value_tuple in all_values:
            if not value_tuple or value_tuple[0] is None:
                continue

            value = str(value_tuple[0]).strip()
            if not value:
                continue

            try:
                dates.append(converter(value))
            except Exception:
                # Fallback para años sueltos: "2025", "2018", etc.
                if re.match(r"^\d{4}$", value):
                    dates.append(datetime.datetime(int(value), 12, 31))
                elif DEBUG_MODE:
                    print(
                        "No se pudo convertir el valor de fecha",
                        value,
                        "en la tabla",
                        table_name,
                        "columna",
                        date_col,
                    )
        
        return max(dates) if dates else None

    except Exception as error:
        print("Error al intentar calcular la fecha más reciente de la tabla", table_name, ". Error:", error)
        return None

def calculate_metrics():
    db: Session = SessionLocal()
    
    sources = db.query(Source).all()
    
    # 1. Agregaciones base por fuente
    stats_per_source = {}
    
    # Para la lógica MAX por tipo institucional
    max_v_by_type = {"Ayuntamiento": 0, "Cabildo": 0, "Especializado": 0}
    max_r_by_type = {"Ayuntamiento": 0, "Cabildo": 0, "Especializado": 0}
    
    now = datetime.datetime.now()
    # Conteo de datasets sin fechas como columna
    number_of_dates_not_found = 0
    number_of_datasets = 0
    for src in sources:
        # V = Volumen (datasets totales)
        v = db.query(func.count(Dataset.id)).filter(Dataset.source_id == src.id).scalar() or 0
            
        r = db.query(func.sum(DatasetContentMeta.row_count))\
              .join(Dataset)\
              .filter(Dataset.source_id == src.id).scalar() or 0
        
        # Calcular Frescura por dataset y luego la agregación A
        datasets = db.query(Dataset).filter(Dataset.source_id == src.id).all()
        
        sum_frescura_x_registros = 0
        total_resources_source = 0
        reusable_formats_source = 0
        source_last_ingestion = None
        
        for ds in datasets:
            table_name = db.query(DatasetContentMeta.table_name)\
                           .filter(DatasetContentMeta.dataset_id == ds.id)\
                           .scalar()
            if not table_name:
                continue
            number_of_datasets += 1
            # Días de antigüedad, si falta: asumir 0
            most_recent_entry_datetime = get_most_recent_date(db, table_name)
            if most_recent_entry_datetime is not None:
                # Normalizar zona horaria antes de restar fechas.
                # PostgreSQL/SQLAlchemy puede devolver timestamps con tzinfo,
                # mientras que `now` es naive. Python no permite restarlos.
                if most_recent_entry_datetime.tzinfo is not None and now.tzinfo is None:
                    most_recent_entry_datetime = most_recent_entry_datetime.replace(tzinfo=None)
                elif most_recent_entry_datetime.tzinfo is None and now.tzinfo is not None:
                    now = now.replace(tzinfo=None)

                dias_antiguedad = max(0, (now - most_recent_entry_datetime).days)
            else:
                number_of_dates_not_found += 1
                dias_antiguedad = 0
                
            # MAX(0, 100 - (Dias_Antiguedad / 730) * 100)
            nota_frescura = max(0.0, 100.0 - (dias_antiguedad / 730.0) * 100.0)
            
            # Registros del dataset
            resources_ds = db.query(Resource).filter(Resource.dataset_id == ds.id).all()
            reg_ds = db.query(DatasetContentMeta.row_count)\
                       .filter(DatasetContentMeta.dataset_id == ds.id).scalar() or 0
            
            ds.total_resources = len(resources_ds)
            unique_formats = set(res.format.upper() for res in resources_ds if res.format)
            ds.available_formats = ", ".join(sorted(list(unique_formats))) if unique_formats else None
            ds.reusable_formats = len(unique_formats)

            ds_meta = db.query(DatasetContentMeta).filter(DatasetContentMeta.dataset_id == ds.id).first()
            if ds_meta and ds_meta.updated_at:
                ds.last_ingestion = ds_meta.updated_at
            else:
                ds.last_ingestion = None

            if ds.last_ingestion:
                if not source_last_ingestion or ds.last_ingestion > source_last_ingestion:
                    source_last_ingestion = ds.last_ingestion
            
            total_resources_source += ds.total_resources
            reusable_formats_source += ds.reusable_formats

            sum_frescura_x_registros += nota_frescura * reg_ds
            
        a_score = 100.0 # Por si r == 0
        if r > 0:
            a_score = sum_frescura_x_registros / r
            
        ratio_reusable = reusable_formats_source / len(datasets) if len(datasets) > 0 else 0.0
            
        stats_per_source[src.id] = {
            "v": v,
            "r": r,
            "a": a_score,
            "type": src.type,
            "total_resources": total_resources_source,
            "reusable_formats": ratio_reusable,
            "last_ingestion": source_last_ingestion
        }
        
        # Búsqueda de máximos
        if v > max_v_by_type[src.type]: max_v_by_type[src.type] = v
        if r > max_r_by_type[src.type]: max_r_by_type[src.type] = r

    # 2. Normalización, cálculo Global Score y Persistencia
    for src_id, stats in stats_per_source.items():
        type_ = stats["type"]
        v = stats["v"]
        r = stats["r"]
        a = stats["a"]
        
        max_v = max_v_by_type[type_] or 1
        max_r = max_r_by_type[type_] or 1
        
        norm_v = (v / max_v) * 100.0
        norm_r = (r / max_r) * 100.0
        
        # Fórmula global obligatoria: Nota = (0.3 * V) + (0.3 * R) + (0.4 * A)
        global_score = (0.3 * norm_v) + (0.3 * norm_r) + (0.4 * a)
        
        logging.info(f"Fuente {src_id}: V={v} ({norm_v:.1f}), R={r} ({norm_r:.1f}), A={a:.1f} -> Global: {global_score:.2f}")
        
        summary = db.query(SummaryMetrics).filter_by(source_id=src_id).first()
        if not summary:
            summary = SummaryMetrics(source_id=src_id)
            db.add(summary)
            
        # Esto son métricas de una fuente
        summary.volume_datasets = v
        summary.total_records = r
        summary.normalized_v = norm_v
        summary.normalized_r = norm_r
        summary.freshness_score_a = a
        summary.global_score = global_score
        summary.calculated_at = datetime.datetime.now()
        summary.total_resources = stats["total_resources"]
        summary.reusable_formats = stats["reusable_formats"]
        summary.last_ingestion = stats["last_ingestion"]
        
    db.commit()
    db.close()
    
    if DEBUG_MODE:
        print("Número de datasets con tabla:", number_of_datasets)
        print("Número de datasets sin columna de tiempo:", number_of_dates_not_found)
if __name__ == "__main__":
    calculate_metrics()
