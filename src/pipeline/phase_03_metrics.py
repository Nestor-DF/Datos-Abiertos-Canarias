import datetime
import logging
from sqlalchemy import func
from sqlalchemy.orm import Session
from src.database.connection import SessionLocal
from src.database.models import Source, Dataset, Resource, SummaryMetrics, DatasetContentMeta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def calculate_metrics():
    db: Session = SessionLocal()
    
    sources = db.query(Source).all()
    
    # 1. Agregaciones base por fuente
    stats_per_source = {}
    
    # Para la lógica MAX por tipo institucional
    max_v_by_type = {"Ayuntamiento": 0, "Cabildo": 0, "Especializado": 0}
    max_r_by_type = {"Ayuntamiento": 0, "Cabildo": 0, "Especializado": 0}
    
    now = datetime.datetime.now()
    
    for src in sources:
        # V = Volumen (datasets totales)
        v = db.query(func.count(Dataset.id)).filter(Dataset.source_id == src.id).scalar() or 0
        
        # R = Registros (sumando de todos los recursos del source)
        r = db.query(func.sum(Resource.records_count))\
              .join(Dataset)\
              .filter(Dataset.source_id == src.id).scalar() or 0
        
        # Calcular Frescura por dataset y luego la agregación A
        datasets = db.query(Dataset).filter(Dataset.source_id == src.id).all()
        
        sum_frescura_x_registros = 0
        total_resources_source = 0
        reusable_formats_source = 0
        source_last_ingestion = None
        
        for ds in datasets:
            # Días de antigüedad, si falta: asumir 0
            if ds.last_updated:
                dias_antiguedad = max(0, (now - ds.last_updated).days)
            else:
                dias_antiguedad = 0
                
            # MAX(0, 100 - (Dias_Antiguedad / 730) * 100)
            nota_frescura = max(0.0, 100.0 - (dias_antiguedad / 730.0) * 100.0)
            
            # Recursos del dataset
            resources_ds = db.query(Resource).filter(Resource.dataset_id == ds.id).all()
            reg_ds = sum((res.records_count or 0) for res in resources_ds)
            
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
    
if __name__ == "__main__":
    calculate_metrics()
