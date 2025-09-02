#!/usr/bin/env python3
"""
Main entry point for the Crypto Data Pipeline.
"""
import asyncio
import logging
import os
import sys
from pathlib import Path

from src.config.settings import DatabaseConfig, PipelineConfig
from src.pipeline.orchestrator import CryptoPipelineOrchestrator
from src.scheduler.job_scheduler import PipelineScheduler


def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    return logging.getLogger(__name__)


async def run_manual_extraction():
    """Run a single manual extraction"""
    logger = logging.getLogger(__name__)
    logger.info("Starting manual extraction...")

    orchestrator = CryptoPipelineOrchestrator(PipelineConfig(), DatabaseConfig())
    result = await orchestrator.run_extraction_pipeline()

    logger.info(f"Extraction completed with result: {result}")
    return result


def run_scheduled_pipeline():
    """Run the scheduled pipeline"""
    logger = logging.getLogger(__name__)
    logger.info("Starting scheduled pipeline...")

    config = PipelineConfig()
    db_config = DatabaseConfig()
    orchestrator = CryptoPipelineOrchestrator(config, db_config)

    scheduler = PipelineScheduler(orchestrator, config.extraction_interval_minutes)
    scheduler.schedule_pipeline()

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Pipeline scheduler stopped by user")
        scheduler.stop()


def main():
    """Main entry point"""
    logger = setup_logging()

    # Check command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "manual":
            asyncio.run(run_manual_extraction())
        elif command == "schedule":
            run_scheduled_pipeline()
        else:
            logger.error(f"Unknown command: {command}")
            logger.info("Usage: python main.py [manual|schedule]")
            sys.exit(1)
    else:
        asyncio.run(run_manual_extraction())


if __name__ == "__main__":
    main()
