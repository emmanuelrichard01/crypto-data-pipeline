import logging
from datetime import datetime, timedelta
from typing import Any, Dict

from sqlalchemy import text

from loaders.warehouse_loader import WarehouseLoader

logger = logging.getLogger(__name__)


class PipelineHealthMonitor:
    def __init__(self, loader: WarehouseLoader):
        self.loader = loader

    def check_pipeline_health(self) -> Dict[str, Any]:
        """Check overall pipeline health with clear error/warning separation."""
        try:
            with self.loader.get_session() as session:
                # Recent pipeline runs
                recent_runs = session.execute(
                    text(
                        """
                        SELECT stage, status, COUNT(*) as count
                        FROM pipeline_runs
                        WHERE started_at >= :since
                        GROUP BY stage, status
                        """
                    ),
                    {"since": datetime.utcnow() - timedelta(hours=24)},
                ).fetchall()

                # Data freshness
                latest_data = session.execute(
                    text(
                        """
                        SELECT MAX(extracted_at) as latest_extraction,
                               COUNT(*) as total_records
                        FROM crypto_prices_raw
                        WHERE extracted_at >= :since
                        """
                    ),
                    {"since": datetime.utcnow() - timedelta(hours=24)},
                ).fetchone()

                # Data quality
                data_quality = session.execute(
                    text(
                        """
                        SELECT
                            COUNT(*) as total_records,
                            COUNT(*) FILTER (WHERE current_price > 0) as valid_prices,
                            AVG(current_price) as avg_price
                        FROM crypto_prices_raw
                        WHERE extracted_at >= :since
                        """
                    ),
                    {"since": datetime.utcnow() - timedelta(hours=1)},
                ).fetchone()

                # Base health status
                health_status = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "status": "healthy",
                    "pipeline_runs": [
                        {"stage": r.stage, "status": r.status, "count": r.count}
                        for r in recent_runs
                    ],
                    "data_freshness": {
                        "latest_extraction": (
                            latest_data.latest_extraction.isoformat()
                            if latest_data and latest_data.latest_extraction
                            else None
                        ),
                        "records_last_24h": (
                            latest_data.total_records if latest_data else 0
                        ),
                    },
                    "data_quality": {
                        "total_records_last_hour": (
                            data_quality.total_records if data_quality else 0
                        ),
                        "valid_price_records": (
                            data_quality.valid_prices if data_quality else 0
                        ),
                        "average_price": (
                            float(data_quality.avg_price)
                            if data_quality and data_quality.avg_price
                            else 0
                        ),
                    },
                }

                # Health rules
                if (
                    latest_data
                    and latest_data.latest_extraction
                    and (datetime.utcnow() - latest_data.latest_extraction)
                    > timedelta(hours=2)
                ):
                    health_status["status"] = "unhealthy"
                    health_status["issues"] = ["Data is stale - no recent extractions"]
                elif not recent_runs:
                    health_status["status"] = "warning"
                    health_status["issues"] = ["No pipeline run data available"]

                return health_status

        except Exception as e:
            # Any DB-level or query failure is a hard error
            logger.error(f"Health check failed: {e}", exc_info=True)
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "status": "error",
                "error": str(e),
            }
