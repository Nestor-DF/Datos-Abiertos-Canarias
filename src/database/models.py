from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship
import datetime
from src.database.connection import Base

# TODO 1: Cambiar el modelo dataset (y la lógica del código en la fase de extracción) para que guarde el contenido completo
# de este, los datasets están asociados a una fuente y cada dataset tiene uno o varios recursos (archivos o endpoints)
# guardar solo el contenido de la última versión del dataset (recurso más actualizado o eso es lo que entiendo)

# TODO 2: Añadir número total de recursos y número total de formatos reutilizables por fuente y dataset

class Source(Base):
    __tablename__ = "sources"
    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    url = Column(String, nullable=False)
    type = Column(String, nullable=False)

    datasets = relationship("Dataset", back_populates="source", cascade="all, delete")
    summaries = relationship("SummaryMetrics", back_populates="source", cascade="all, delete")

class Dataset(Base):
    __tablename__ = "datasets"
    id = Column(String, primary_key=True, index=True)
    source_id = Column(String, ForeignKey("sources.id"))
    title = Column(String)
    last_updated = Column(DateTime)
    total_resources = Column(Integer, default=0)
    reusable_formats = Column(Integer, default=0)
    available_formats = Column(String, nullable=True)
    last_ingestion = Column(DateTime, nullable=True)
    
    source = relationship("Source", back_populates="datasets")
    resources = relationship("Resource", back_populates="dataset", cascade="all, delete")

class Resource(Base):
    __tablename__ = "resources"
    id = Column(String, primary_key=True, index=True)
    dataset_id = Column(String, ForeignKey("datasets.id"))
    title = Column(String)
    format = Column(String)
    url = Column(Text)
    records_count = Column(Integer, default=0)
    
    dataset = relationship("Dataset", back_populates="resources")

class SummaryMetrics(Base):
    __tablename__ = "summary_metrics"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    source_id = Column(String, ForeignKey("sources.id"))
    calculated_at = Column(DateTime, default=datetime.datetime.now)
    
    volume_datasets = Column(Integer, default=0)
    total_records = Column(Integer, default=0)
    total_resources = Column(Integer, default=0)
    reusable_formats = Column(Float, default=0.0)
    last_ingestion = Column(DateTime, nullable=True)
    
    normalized_v = Column(Float, default=0.0)
    normalized_r = Column(Float, default=0.0)
    freshness_score_a = Column(Float, default=0.0)
    
    global_score = Column(Float, default=0.0)
    
    source = relationship("Source", back_populates="summaries")

class DatasetContentMeta(Base):
    """
    Registra qué datasets tienen tabla de contenido creada en la BD,
    junto con la fecha del recurso que se usó para poblarla.
    Permite saber si hay que actualizar (fecha más nueva) o saltar (misma fecha).
    """
    __tablename__ = "dataset_content_meta"
    dataset_id = Column(String, ForeignKey("datasets.id"), primary_key=True)
    resource_id = Column(String, nullable=False)          # recurso más reciente usado
    resource_last_modified = Column(DateTime, nullable=True)  # fecha de ese recurso
    table_name = Column(String, nullable=False)            # nombre real de la tabla en BD
    row_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.datetime.now)
    updated_at = Column(DateTime, default=datetime.datetime.now)

class ExecutionLog(Base):
    __tablename__ = "execution_log"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.datetime.now)
    source_id = Column(String, nullable=True)
    dataset_id = Column(String, nullable=True)
    level = Column(String)
    message = Column(Text)