from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship
import datetime
from src.database.connection import Base

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
    
    normalized_v = Column(Float, default=0.0)
    normalized_r = Column(Float, default=0.0)
    freshness_score_a = Column(Float, default=0.0)
    
    global_score = Column(Float, default=0.0)
    
    source = relationship("Source", back_populates="summaries")

class ExecutionLog(Base):
    __tablename__ = "execution_log"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.datetime.now)
    source_id = Column(String, nullable=True)
    dataset_id = Column(String, nullable=True)
    level = Column(String)
    message = Column(Text)
