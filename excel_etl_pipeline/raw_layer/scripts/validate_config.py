#!/usr/bin/env python3
"""
Configuration validation script
Run this to validate your config.yaml before running the pipeline.
"""

import sys
from pathlib import Path

# Add scripts directory to path
scripts_dir = Path(__file__).parent
sys.path.append(str(scripts_dir))

try:
    from config_manager import validate_configuration, get_config_manager

    print("=" * 60)
    print("Configuration Validation Tool")
    print("=" * 60)

    # Get config manager (will load config.yaml)
    try:
        cm = get_config_manager()
        print(f"✅ Configuration loaded from: {cm.config_path}")
    except Exception as e:
        print(f"❌ Failed to load configuration: {e}")
        sys.exit(1)

    # Validate configuration
    print("\nValidating configuration...")

    if validate_configuration():
        print("\n✅ Configuration is valid!")

        # Show summary
        print("\nConfiguration Summary:")
        print(
            f"  Database: {cm.config['database']['user']}@{cm.config['database']['host']}:{cm.config['database']['port']}"
        )
        print(f"  Input Dir: {cm.config['raw_layer']['input_dir']}")
        print(f"  Output Dir: {cm.config['raw_layer']['output_dir']}")
        print(f"  State File: {cm.config['raw_layer']['state_file']}")
        print(f"  Log Level: {cm.config['pipeline']['log_level']}")

    else:
        print("\n❌ Configuration validation failed!")
        sys.exit(1)

except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure config_manager.py is in the scripts directory")
    sys.exit(1)
except Exception as e:
    print(f"❌ Unexpected error: {e}")
    sys.exit(1)
