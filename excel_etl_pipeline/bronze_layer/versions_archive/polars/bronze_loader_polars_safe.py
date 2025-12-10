#!/usr/bin/env python3
"""
Safe version of Bronze Loader - SIMD disabled
"""

import os
import sys

# DISABLE ALL SIMD/AVX FEATURES BEFORE IMPORTING ANYTHING
os.environ["POLARS_NO_SIMD"] = "1"
os.environ["POLARS_SKIP_CPU_CHECK"] = "1"
os.environ["POLARS_FORCE_STREAMING"] = "1"
os.environ["ARROW_NO_SIMD"] = "1"

# Now import the original module
sys.path.insert(0, os.path.dirname(__file__))

try:
    from bronze_loader_polars import main
except ImportError:
    # If import fails, run directly
    exec(open("bronze_layer/bronze_loader_polars.py").read())
    sys.exit(0)

if __name__ == "__main__":
    print("🚀 Running Bronze Loader in SAFE MODE (SIMD disabled)")
    success = main()
    sys.exit(0 if success else 1)
