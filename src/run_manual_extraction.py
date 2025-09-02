import asyncio
import sys

from config.settings import DatabaseConfig, PipelineConfig
from pipeline.orchestrator import CryptoPipelineOrchestrator


async def main():
    orchestrator = CryptoPipelineOrchestrator(PipelineConfig(), DatabaseConfig())
    result = await orchestrator.run_extraction_pipeline()
    print("Extraction Result:", result)


if __name__ == "__main__":
    asyncio.run(main())
