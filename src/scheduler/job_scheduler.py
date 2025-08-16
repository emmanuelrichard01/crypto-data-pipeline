import schedule
import time
import asyncio
from datetime import datetime
import logging
import json
from pipeline.orchestrator import CryptoPipelineOrchestrator

logger = logging.getLogger(__name__)

class PipelineScheduler:
    def __init__(self, orchestrator: CryptoPipelineOrchestrator, 
                 interval_minutes: int = 60):
        self.orchestrator = orchestrator
        self.interval_minutes = interval_minutes
        self.is_running = False
    
    def schedule_pipeline(self):
        """Schedule the pipeline to run at specified intervals"""
        schedule.every(self.interval_minutes).minutes.do(self._run_pipeline_job)
        logger.info(f"Pipeline scheduled to run every {self.interval_minutes} minutes")
    
    def _run_pipeline_job(self):
        """Wrapper to run async pipeline in sync scheduler"""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(self.orchestrator.run_extraction_pipeline())
            logger.info(f"Scheduled pipeline run completed: {json.dumps(result, indent=2)}")
        except Exception as e:
            logger.error(f"Scheduled pipeline run failed: {e}", exc_info=True)
        finally:
            loop.close()
    
    def start(self):
        """Start the scheduler"""
        self.is_running = True
        logger.info("Starting pipeline scheduler...")
        logger.info(f"Pipeline will run every {self.interval_minutes} minutes")
        
        # Run once immediately
        self._run_pipeline_job()
        
        # Then run on schedule
        while self.is_running:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def stop(self):
        """Stop the scheduler"""
        self.is_running = False
        logger.info("Pipeline scheduler stopped")