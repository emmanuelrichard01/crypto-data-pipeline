import uuid
import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Any
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert

from models.base import Base
from models.schemas import PipelineRun
from config.settings import DatabaseConfig

logger = logging.getLogger(__name__)


class WarehouseLoader:
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.engine = create_engine(
            db_config.connection_string,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            echo=False,
        )
        self.SessionLocal = sessionmaker(bind=self.engine)

    def create_tables(self):
        """Create all tables based on Base metadata"""
        Base.metadata.create_all(bind=self.engine)
        logger.info("✅ Database tables created or already exist.")

    @contextmanager
    def get_session(self):
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.exception("❌ Session rollback due to error")
            raise e
        finally:
            session.close()

    def bulk_insert_crypto_prices(self, data: List[Dict]) -> int:
        """Bulk insert crypto price records into raw table"""
        if not data:
            logger.info("No data to insert")
            return 0

        try:
            for record in data:
                record["id"] = str(uuid.uuid4())
                record["created_at"] = datetime.utcnow()

            df = pd.DataFrame(data)
            df.to_sql(
                "crypto_prices_raw",
                self.engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=self.db_config.batch_size or 100,
            )
            logger.info(f"✅ Inserted {len(data)} crypto price records.")
            return len(data)

        except SQLAlchemyError as e:
            logger.exception("❌ Database error during bulk insert.")
            raise
        except Exception as e:
            logger.exception("❌ Unexpected error during bulk insert.")
            raise

    def log_pipeline_run(
        self,
        run_id: str,
        stage: str,
        status: str,
        records_processed: int,
        error_message: Optional[str] = None,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
    ):
        """Insert or update a pipeline run log"""
        run_data = {
            "id": uuid.uuid4(),  # You may use uuid5 for idempotency
            "run_id": run_id,
            "stage": stage,
            "status": status,
            "records_processed": records_processed,
            "error_message": error_message,
            "started_at": started_at or datetime.utcnow(),
            "completed_at": completed_at or datetime.utcnow(),
        }

        try:
            with self.get_session() as session:
                stmt = (
                    insert(PipelineRun)
                    .values(**run_data)
                    .on_conflict_do_update(
                        index_elements=["run_id", "stage"],
                        set_={
                            "status": status,
                            "records_processed": records_processed,
                            "error_message": error_message,
                            "completed_at": run_data["completed_at"],
                        },
                    )
                )
                session.execute(stmt)
                logger.info(f"📝 Logged pipeline run: {run_id} [{stage} - {status}]")
        except Exception as e:
            logger.exception("❌ Failed to log pipeline run.")
            raise
