import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("C:/pharma_warehouse/medallion_etl_pipeline/logs/pipeline.log"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)

# Import pipeline
from main_pipeline import medallion_pipeline


def main():
    """Run the complete pipeline"""
    try:
        logger.info("=" * 60)
        logger.info("Starting Medallion Architecture ETL Pipeline")
        logger.info("=" * 60)

        # Run pipeline for all features
        result = medallion_pipeline()

        logger.info("=" * 60)
        logger.info("Pipeline completed successfully!")
        logger.info(f"Results: {result}")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        return 1


def run_specific_features(feature_names: list):
    """Run pipeline for specific features"""
    try:
        logger.info(f"Running pipeline for features: {feature_names}")
        result = medallion_pipeline(feature_names=feature_names)
        logger.info(f"Pipeline completed: {result}")
        return 0
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run Medallion Architecture Pipeline")
    parser.add_argument("--features", nargs="+", help="Specific features to process")
    parser.add_argument(
        "--bronze-only", action="store_true", help="Run Bronze layer only"
    )
    parser.add_argument(
        "--silver-only", action="store_true", help="Run Silver layer only"
    )
    parser.add_argument("--gold-only", action="store_true", help="Run Gold layer only")

    args = parser.parse_args()

    if args.features:
        exit_code = run_specific_features(args.features)
    else:
        exit_code = main()

    sys.exit(exit_code)
