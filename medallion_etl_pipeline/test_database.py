# test_database.py
from config import config
from metadata_logger import MetadataLogger

print("Testing PostgreSQL connection...")

try:
    logger = MetadataLogger(config.database.sqlalchemy_url)
    print("✓ Successfully connected to PostgreSQL")
    print(f"✓ Database: {config.database.database}")
    print(f"✓ Schema initialized: etl_logs")
    logger.close()
    print("\n✅ Database connection test passed!")
except Exception as e:
    print(f"\n✗ Database connection failed: {e}")
    print("\nOptions:")
    print("1. Start PostgreSQL with: docker-compose up -d")
    print("2. Or update .env with your PostgreSQL credentials")
    print("3. Or skip database logging for now (I can help modify code)")