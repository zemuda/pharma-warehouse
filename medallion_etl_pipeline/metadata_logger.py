# metadata_logger.py
"""
PostgreSQL metadata and logging management
"""
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any
import sqlalchemy as sa
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Boolean, Float, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
import logging

Base = declarative_base()


class PipelineRun(Base):
    __tablename__ = 'pipeline_runs'
    __table_args__ = {'schema': 'etl_logs'}
    
    run_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    pipeline_name = Column(String(200), nullable=False)
    layer = Column(String(50))  # bronze, silver, gold
    feature_name = Column(String(200))
    started_at = Column(DateTime, nullable=False, default=datetime.now)
    completed_at = Column(DateTime)
    status = Column(String(50))  # STARTED, RUNNING, COMPLETED, FAILED
    records_processed = Column(Integer, default=0)
    records_valid = Column(Integer, default=0)
    records_invalid = Column(Integer, default=0)
    files_processed = Column(Integer, default=0)
    error_message = Column(Text)
    execution_metadata = Column(JSON)


class FileProcessing(Base):
    __tablename__ = 'file_processing'
    __table_args__ = {'schema': 'etl_logs'}
    
    file_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    run_id = Column(String(36), nullable=False)
    file_path = Column(String(1000), nullable=False)
    file_hash = Column(String(64))
    file_size = Column(Integer)
    sheet_name = Column(String(200))
    records_count = Column(Integer)
    processed_at = Column(DateTime, default=datetime.now)
    status = Column(String(50))
    error_message = Column(Text)
    processing_time_ms = Column(Integer)


class SchemaEvolution(Base):
    __tablename__ = 'schema_evolution'
    __table_args__ = {'schema': 'etl_logs'}
    
    evolution_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    run_id = Column(String(36), nullable=False)
    layer = Column(String(50))
    table_name = Column(String(200), nullable=False)
    change_type = Column(String(50))  # ADD_COLUMN, DROP_COLUMN, TYPE_CHANGE
    column_name = Column(String(200))
    old_data_type = Column(String(100))
    new_data_type = Column(String(100))
    change_date = Column(DateTime, default=datetime.now)
    resolved = Column(Boolean, default=False)


class DataQualityCheck(Base):
    __tablename__ = 'data_quality_checks'
    __table_args__ = {'schema': 'etl_logs'}
    
    check_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    run_id = Column(String(36), nullable=False)
    layer = Column(String(50))
    table_name = Column(String(200), nullable=False)
    check_type = Column(String(100))  # null_check, duplicate_check, range_check, etc.
    column_name = Column(String(200))
    check_status = Column(String(50))  # PASSED, FAILED, WARNING
    records_affected = Column(Integer)
    check_details = Column(JSON)
    checked_at = Column(DateTime, default=datetime.now)


class SCD2History(Base):
    __tablename__ = 'scd2_history'
    __table_args__ = {'schema': 'etl_logs'}
    
    history_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    run_id = Column(String(36), nullable=False)
    table_name = Column(String(200), nullable=False)
    operation_type = Column(String(50))  # INSERT, UPDATE, EXPIRE
    records_inserted = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    records_expired = Column(Integer, default=0)
    operation_timestamp = Column(DateTime, default=datetime.now)


class TableStatistics(Base):
    __tablename__ = 'table_statistics'
    __table_args__ = {'schema': 'etl_logs'}
    
    stat_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    run_id = Column(String(36), nullable=False)
    layer = Column(String(50))
    table_name = Column(String(200), nullable=False)
    record_count = Column(Integer)
    file_size_mb = Column(Float)
    partition_count = Column(Integer)
    last_updated = Column(DateTime, default=datetime.now)
    statistics_json = Column(JSON)


class MetadataLogger:
    """Manages all metadata logging to PostgreSQL"""
    
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string, pool_pre_ping=True)
        self.SessionLocal = sessionmaker(bind=self.engine)
        self.logger = logging.getLogger(__name__)
        self._init_schema()
    
    def _init_schema(self):
        """Initialize database schema"""
        try:
            # Create schema if not exists
            with self.engine.connect() as conn:
                conn.execute(sa.text("CREATE SCHEMA IF NOT EXISTS etl_logs"))
                conn.commit()
            
            # Create all tables
            Base.metadata.create_all(self.engine)
            self.logger.info("Metadata schema initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize schema: {e}")
            raise
    
    @contextmanager
    def get_session(self) -> Session:
        """Provide a transactional scope for database operations"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(f"Session error: {e}")
            raise
        finally:
            session.close()
    
    def start_pipeline_run(self, pipeline_name: str, layer: str, 
                          feature_name: Optional[str] = None,
                          metadata: Optional[Dict] = None) -> str:
        """Start a new pipeline run"""
        with self.get_session() as session:
            run = PipelineRun(
                pipeline_name=pipeline_name,
                layer=layer,
                feature_name=feature_name,
                status='STARTED',
                execution_metadata=metadata
            )
            session.add(run)
            session.flush()
            run_id = run.run_id
            self.logger.info(f"Started pipeline run: {run_id}")
            return run_id
    
    def update_pipeline_run(self, run_id: str, status: str,
                           records_processed: Optional[int] = None,
                           records_valid: Optional[int] = None,
                           records_invalid: Optional[int] = None,
                           files_processed: Optional[int] = None,
                           error_message: Optional[str] = None):
        """Update pipeline run status"""
        with self.get_session() as session:
            run = session.query(PipelineRun).filter_by(run_id=run_id).first()
            if run:
                run.status = status
                run.completed_at = datetime.now()
                if records_processed is not None:
                    run.records_processed = records_processed
                if records_valid is not None:
                    run.records_valid = records_valid
                if records_invalid is not None:
                    run.records_invalid = records_invalid
                if files_processed is not None:
                    run.files_processed = files_processed
                if error_message:
                    run.error_message = error_message
                self.logger.info(f"Updated pipeline run {run_id}: {status}")
    
    def log_file_processing(self, run_id: str, file_path: str, 
                           sheet_name: Optional[str] = None,
                           status: str = 'SUCCESS',
                           records_count: Optional[int] = None,
                           file_size: Optional[int] = None,
                           file_hash: Optional[str] = None,
                           processing_time_ms: Optional[int] = None,
                           error_message: Optional[str] = None) -> str:
        """Log file processing details"""
        with self.get_session() as session:
            file_record = FileProcessing(
                run_id=run_id,
                file_path=file_path,
                sheet_name=sheet_name,
                status=status,
                records_count=records_count,
                file_size=file_size,
                file_hash=file_hash,
                processing_time_ms=processing_time_ms,
                error_message=error_message
            )
            session.add(file_record)
            session.flush()
            return file_record.file_id
    
    def log_schema_evolution(self, run_id: str, layer: str, table_name: str,
                            change_type: str, column_name: str,
                            old_data_type: Optional[str] = None,
                            new_data_type: Optional[str] = None):
        """Log schema evolution events"""
        with self.get_session() as session:
            evolution = SchemaEvolution(
                run_id=run_id,
                layer=layer,
                table_name=table_name,
                change_type=change_type,
                column_name=column_name,
                old_data_type=old_data_type,
                new_data_type=new_data_type
            )
            session.add(evolution)
            self.logger.info(f"Logged schema evolution: {change_type} on {table_name}.{column_name}")
    
    def log_data_quality_check(self, run_id: str, layer: str, table_name: str,
                               check_type: str, check_status: str,
                               column_name: Optional[str] = None,
                               records_affected: Optional[int] = None,
                               check_details: Optional[Dict] = None):
        """Log data quality check results"""
        with self.get_session() as session:
            check = DataQualityCheck(
                run_id=run_id,
                layer=layer,
                table_name=table_name,
                check_type=check_type,
                column_name=column_name,
                check_status=check_status,
                records_affected=records_affected,
                check_details=check_details
            )
            session.add(check)
            self.logger.info(f"Logged data quality check: {check_type} - {check_status}")
    
    def log_scd2_operation(self, run_id: str, table_name: str,
                          operation_type: str,
                          records_inserted: int = 0,
                          records_updated: int = 0,
                          records_expired: int = 0):
        """Log SCD2 operations"""
        with self.get_session() as session:
            scd2 = SCD2History(
                run_id=run_id,
                table_name=table_name,
                operation_type=operation_type,
                records_inserted=records_inserted,
                records_updated=records_updated,
                records_expired=records_expired
            )
            session.add(scd2)
            self.logger.info(f"Logged SCD2 operation: {operation_type} on {table_name}")
    
    def log_table_statistics(self, run_id: str, layer: str, table_name: str,
                            record_count: int, file_size_mb: float,
                            partition_count: int = 0,
                            statistics_json: Optional[Dict] = None):
        """Log table statistics"""
        with self.get_session() as session:
            stats = TableStatistics(
                run_id=run_id,
                layer=layer,
                table_name=table_name,
                record_count=record_count,
                file_size_mb=file_size_mb,
                partition_count=partition_count,
                statistics_json=statistics_json
            )
            session.add(stats)
    
    def get_processed_files(self, status: str = 'SUCCESS') -> List[str]:
        """Get list of successfully processed files"""
        with self.get_session() as session:
            files = session.query(FileProcessing.file_path)\
                          .filter_by(status=status)\
                          .distinct()\
                          .all()
            return [f[0] for f in files]
    
    def get_file_hash(self, file_path: str) -> Optional[str]:
        """Get file hash for already processed file"""
        with self.get_session() as session:
            result = session.query(FileProcessing.file_hash)\
                           .filter_by(file_path=file_path, status='SUCCESS')\
                           .order_by(FileProcessing.processed_at.desc())\
                           .first()
            return result[0] if result else None
    
    def close(self):
        """Close database connections"""
        self.engine.dispose()