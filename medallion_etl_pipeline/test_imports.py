# test_imports.py
print("Testing imports...")

try:
    import pyspark
    print("✓ PySpark OK")
except ImportError as e:
    print(f"✗ PySpark failed: {e}")

try:
    import delta
    print("✓ Delta Lake OK")
except ImportError as e:
    print(f"✗ Delta Lake failed: {e}")

try:
    import pandas
    print("✓ Pandas OK")
except ImportError as e:
    print(f"✗ Pandas failed: {e}")

try:
    import psycopg
    print("✓ Psycopg3 OK")
except ImportError as e:
    print(f"✗ Psycopg3 failed: {e}")

try:
    import sqlalchemy
    print("✓ SQLAlchemy OK")
except ImportError as e:
    print(f"✗ SQLAlchemy failed: {e}")

try:
    import prefect
    print("✓ Prefect OK")
except ImportError as e:
    print(f"✗ Prefect failed: {e}")

print("\n✅ All imports successful!")