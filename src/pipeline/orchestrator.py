import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from config.settings import DatabaseConfig, PipelineConfig
from extractors.crypto_extractor import CryptoDataExtractor
from loaders.warehouse_loader import WarehouseLoader
from models.schemas import PipelineRun

logger = logging.getLogger(__name__)


class CryptoPipelineOrchestrator:
    def __init__(self, config: PipelineConfig, db_config: DatabaseConfig):
        self.config = config
        self.db_config = db_config
        self.loader = WarehouseLoader(db_config)

    async def run_extraction_pipeline(self) -> Dict[str, Any]:
        """Run the complete extraction and loading pipeline"""
        run_id = f"crypto_extract_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        start_time = datetime.utcnow()

        logger.info(f"Starting pipeline run: {run_id}")
        logger.info(
            f"Extracting data for cryptocurrencies: {self.config.cryptocurrencies}"
        )

        try:
            # Log extract → running
            self.loader.log_pipeline_run(
                run_id=run_id,
                stage="extract",
                status="running",
                records_processed=0,
                started_at=start_time,
            )

            # Extract data
            logger.info("Starting data extraction from CoinGecko API")
            async with CryptoDataExtractor(self.config) as extractor:
                crypto_data = await extractor.fetch_crypto_prices()
            logger.info(
                f"Successfully extracted {len(crypto_data)} records from CoinGecko API"
            )

            # Log extract → success
            self.loader.log_pipeline_run(
                run_id=run_id,
                stage="extract",
                status="success",
                records_processed=len(crypto_data),
                started_at=start_time,
                completed_at=datetime.utcnow(),
            )

            # Log load → running
            self.loader.log_pipeline_run(
                run_id=run_id,
                stage="load",
                status="running",
                records_processed=0,
                started_at=datetime.utcnow(),
            )

            # Load data
            logger.info(f"Starting data loading for {len(crypto_data)} records")
            records_loaded = self.loader.bulk_insert_crypto_prices(crypto_data)
            logger.info(f"Successfully loaded {records_loaded} records into database")

            # Log load → success
            self.loader.log_pipeline_run(
                run_id=run_id,
                stage="load",
                status="success",
                records_processed=records_loaded,
                completed_at=datetime.utcnow(),
            )

            result = {
                "run_id": run_id,
                "status": "success",
                "records_processed": records_loaded,
                "timestamp": datetime.utcnow().isoformat(),
            }

            logger.info(f"Pipeline run {run_id} completed successfully")
            return result

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Pipeline run {run_id} failed: {error_msg}")

            # Log extract → failed (regardless of where it failed)
            self.loader.log_pipeline_run(
                run_id=run_id,
                stage="extract",
                status="failed",
                records_processed=0,
                error_message=error_msg,
                completed_at=datetime.utcnow(),
            )

            result = {
                "run_id": run_id,
                "status": "failed",
                "error": error_msg,
                "timestamp": datetime.utcnow().isoformat(),
            }

            return result
