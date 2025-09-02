import asyncio
import sys
from pipeline.orchestrator import CryptoPipelineOrchestrator
from config.settings import PipelineConfig, DatabaseConfig


async def main():
    orchestrator = CryptoPipelineOrchestrator(PipelineConfig(), DatabaseConfig())
    result = await orchestrator.run_extraction_pipeline()
    print("Extraction Result:", result)


if __name__ == "__main__":
    asyncio.run(main())
