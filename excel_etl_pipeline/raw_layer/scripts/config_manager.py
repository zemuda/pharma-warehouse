#!/usr/bin/env python3
"""
Configuration Manager for YAML configuration files
Handles configuration loading, validation, and path management.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, List
import logging

# Setup logging
logger = logging.getLogger(__name__)


class ConfigManager:
    """Manage YAML configuration files with validation and path management"""

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration manager

        Args:
            config_path: Optional path to configuration file
        """
        self.config_path = (
            Path(config_path) if config_path else self._find_config_file()
        )
        self.config = self._load_and_validate_config()
        self._ensure_directories()
        self._setup_logging()

    def _find_config_file(self) -> Path:
        """
        Find configuration file in common locations

        Returns:
            Path to configuration file

        Raises:
            FileNotFoundError: If no configuration file is found
        """
        # Define possible configuration file locations
        possible_paths = [
            # In scripts directory (current location)
            Path(__file__).parent / "config.yaml",
            # In parent directory (raw_layer)
            Path(__file__).parent.parent / "config.yaml",
            # In project root
            Path(__file__).parent.parent.parent / "config.yaml",
            # In current working directory
            Path.cwd() / "config.yaml",
            # Alternative extension
            Path(__file__).parent / "config.yml",
        ]

        # Check each possible location
        for path in possible_paths:
            if path.exists():
                logger.info(f"Found configuration file at: {path}")
                return path

        # If not found, raise error with helpful message
        error_msg = (
            "Could not find config.yaml file. Please create one in one of these locations:\n"
            "1. C:/pharma_warehouse/excel_etl_pipeline/raw_layer/scripts/config.yaml\n"
            "2. C:/pharma_warehouse/excel_etl_pipeline/raw_layer/config.yaml\n"
            "3. C:/pharma_warehouse/excel_etl_pipeline/config.yaml\n"
            "\nOr specify the path using --config argument."
        )
        raise FileNotFoundError(error_msg)

    def _load_and_validate_config(self) -> Dict[str, Any]:
        """
        Load and validate YAML configuration

        Returns:
            Validated configuration dictionary

        Raises:
            RuntimeError: If configuration is invalid
        """
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            if config is None:
                raise RuntimeError("Configuration file is empty")

            # Validate required sections
            self._validate_config_structure(config)

            # Normalize paths
            self._normalize_paths(config)

            # Set default values for missing optional sections
            config = self._set_defaults(config)

            logger.info(f"Configuration loaded successfully from {self.config_path}")
            return config

        except yaml.YAMLError as e:
            raise RuntimeError(f"Failed to parse YAML configuration: {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to load configuration: {e}")

    def _validate_config_structure(self, config: Dict[str, Any]):
        """
        Validate configuration structure

        Args:
            config: Configuration dictionary

        Raises:
            ValueError: If required sections are missing
        """
        required_sections = ["database", "raw_layer", "pipeline"]

        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing required configuration section: '{section}'")

        # Validate database configuration
        db_required = ["host", "port", "name", "user", "password"]
        for key in db_required:
            if key not in config["database"]:
                raise ValueError(f"Missing database configuration key: '{key}'")

        # Validate raw_layer configuration
        raw_required = ["input_dir", "output_dir", "state_file"]
        for key in raw_required:
            if key not in config["raw_layer"]:
                raise ValueError(f"Missing raw_layer configuration key: '{key}'")

    def _normalize_paths(self, config: Dict[str, Any]):
        """
        Normalize paths to use system-specific separators

        Args:
            config: Configuration dictionary to normalize
        """

        def process_value(value: Any) -> Any:
            if isinstance(value, dict):
                return {k: process_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [process_value(item) for item in value]
            elif isinstance(value, str):
                # Check if it looks like a path
                if "/" in value or "\\" in value or "." in value:
                    try:
                        # Convert to Path object and back to string for normalization
                        return str(Path(value))
                    except Exception:
                        return value
                else:
                    return value
            else:
                return value

        process_value(config)

    def _set_defaults(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Set default values for optional configuration sections

        Args:
            config: Configuration dictionary

        Returns:
            Configuration with defaults set
        """
        # Default for file_watcher section
        if "file_watcher" not in config:
            config["file_watcher"] = {}

        file_watcher_defaults = {
            "watch_dir": config["raw_layer"]["input_dir"],
            "pipeline_container": "excel-parquet-pipeline",
            "processing_delay": 30,
        }

        for key, default in file_watcher_defaults.items():
            if key not in config["file_watcher"]:
                config["file_watcher"][key] = default

        # Default for paths section
        if "paths" not in config:
            config["paths"] = {}

        # Calculate paths based on configuration
        project_root = Path(
            config.get("paths", {}).get(
                "project_root", Path(__file__).parent.parent.parent
            )
        )

        paths_defaults = {
            "project_root": str(project_root),
            "raw_layer_src": str(project_root / "raw_layer" / "src"),
            "raw_data": str(project_root / "raw_layer" / "raw_data"),
            "scripts_dir": str(project_root / "raw_layer" / "scripts"),
        }

        for key, default in paths_defaults.items():
            if key not in config["paths"]:
                config["paths"][key] = default

        # Default for advanced section
        if "advanced" not in config:
            config["advanced"] = {}

        advanced_defaults = {
            "max_file_size_mb": 100,
            "batch_size": 1000,
            "timeout_seconds": 300,
            "retry_attempts": 3,
            "retry_delay_seconds": 5,
        }

        for key, default in advanced_defaults.items():
            if key not in config["advanced"]:
                config["advanced"][key] = default

        # Default for notifications section
        if "notifications" not in config:
            config["notifications"] = {"enabled": False}

        return config

    def _ensure_directories(self):
        """Ensure all directories specified in configuration exist"""
        directories_to_create = []

        # Add directories from raw_layer configuration
        if "raw_layer" in self.config:
            raw_cfg = self.config["raw_layer"]

            # Input directory
            if "input_dir" in raw_cfg:
                directories_to_create.append(Path(raw_cfg["input_dir"]))

            # Output directory
            if "output_dir" in raw_cfg:
                directories_to_create.append(Path(raw_cfg["output_dir"]))

            # State file directory
            if "state_file" in raw_cfg:
                state_file_path = Path(raw_cfg["state_file"])
                directories_to_create.append(state_file_path.parent)

        # Add watch directory from file_watcher
        if "file_watcher" in self.config:
            watch_dir = self.config["file_watcher"].get("watch_dir")
            if watch_dir:
                directories_to_create.append(Path(watch_dir))

        # Create directories
        for directory in set(directories_to_create):  # Remove duplicates
            try:
                directory.mkdir(parents=True, exist_ok=True)
                logger.debug(f"Ensured directory exists: {directory}")
            except Exception as e:
                logger.warning(f"Could not create directory {directory}: {e}")

    def _setup_logging(self):
        """Setup logging based on configuration"""
        log_level = self.config["pipeline"].get("log_level", "INFO")
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("config_manager.log"),
                logging.StreamHandler(),
            ],
        )

    def get_database_config(self) -> Dict[str, Any]:
        """
        Get database configuration

        Returns:
            Database configuration dictionary
        """
        return self.config.get("database", {})

    def get_raw_layer_config(self) -> Dict[str, Any]:
        """
        Get raw layer configuration

        Returns:
            Raw layer configuration dictionary
        """
        return self.config.get("raw_layer", {})

    def get_pipeline_config(self) -> Dict[str, Any]:
        """
        Get pipeline configuration

        Returns:
            Pipeline configuration dictionary
        """
        return self.config.get("pipeline", {})

    def get_file_watcher_config(self) -> Dict[str, Any]:
        """
        Get file watcher configuration

        Returns:
            File watcher configuration dictionary
        """
        return self.config.get("file_watcher", {})

    def get_paths_config(self) -> Dict[str, Any]:
        """
        Get paths configuration

        Returns:
            Paths configuration dictionary
        """
        return self.config.get("paths", {})

    def get_advanced_config(self) -> Dict[str, Any]:
        """
        Get advanced configuration

        Returns:
            Advanced configuration dictionary
        """
        return self.config.get("advanced", {})

    def get_notifications_config(self) -> Dict[str, Any]:
        """
        Get notifications configuration

        Returns:
            Notifications configuration dictionary
        """
        return self.config.get("notifications", {})

    def get_all(self) -> Dict[str, Any]:
        """
        Get all configuration

        Returns:
            Complete configuration dictionary
        """
        return self.config

    def get_absolute_path(self, path_key: str, section: str = "raw_layer") -> Path:
        """
        Get absolute path for a configuration key

        Args:
            path_key: Configuration key containing the path
            section: Configuration section (default: 'raw_layer')

        Returns:
            Absolute Path object

        Raises:
            KeyError: If path_key is not found in section
        """
        if section not in self.config:
            raise KeyError(f"Configuration section '{section}' not found")

        if path_key not in self.config[section]:
            raise KeyError(f"Path key '{path_key}' not found in section '{section}'")

        path_str = self.config[section][path_key]
        return Path(path_str).absolute()

    def validate_paths(self) -> List[str]:
        """
        Validate all paths in configuration

        Returns:
            List of validation errors (empty if all valid)
        """
        errors = []

        # Check raw_layer paths
        raw_paths = ["input_dir", "output_dir", "state_file"]
        for path_key in raw_paths:
            try:
                path = self.get_absolute_path(path_key, "raw_layer")
                if not path.parent.exists() and path_key != "state_file":
                    errors.append(f"Parent directory does not exist: {path}")
            except Exception as e:
                errors.append(f"Error with path '{path_key}': {e}")

        # Check file_watcher watch directory
        try:
            watch_dir = self.get_absolute_path("watch_dir", "file_watcher")
            if not watch_dir.exists():
                errors.append(f"Watch directory does not exist: {watch_dir}")
        except Exception as e:
            errors.append(f"Error with watch directory: {e}")

        return errors

    def update_config(self, updates: Dict[str, Any]):
        """
        Update configuration with new values

        Args:
            updates: Dictionary of updates to apply
        """

        def deep_update(original: Dict, update: Dict):
            for key, value in update.items():
                if (
                    key in original
                    and isinstance(original[key], dict)
                    and isinstance(value, dict)
                ):
                    deep_update(original[key], value)
                else:
                    original[key] = value

        deep_update(self.config, updates)
        self._normalize_paths(self.config)
        logger.info(f"Configuration updated with {len(updates)} changes")

    def save_config(self, path: Optional[str] = None):
        """
        Save configuration to file

        Args:
            path: Optional path to save configuration (default: original path)

        Raises:
            RuntimeError: If save fails
        """
        save_path = Path(path) if path else self.config_path

        try:
            # Ensure parent directory exists
            save_path.parent.mkdir(parents=True, exist_ok=True)

            with open(save_path, "w", encoding="utf-8") as f:
                yaml.dump(self.config, f, default_flow_style=False, sort_keys=False)

            logger.info(f"Configuration saved to: {save_path}")
        except Exception as e:
            raise RuntimeError(f"Failed to save configuration: {e}")


# Global configuration instance (lazy initialization)
_config_manager_instance = None


def get_config_manager(config_path: Optional[str] = None) -> ConfigManager:
    """
    Get or create global configuration manager instance

    Args:
        config_path: Optional path to configuration file

    Returns:
        ConfigManager instance
    """
    global _config_manager_instance

    if _config_manager_instance is None:
        _config_manager_instance = ConfigManager(config_path)
    elif config_path and Path(config_path) != _config_manager_instance.config_path:
        # If different config path requested, create new instance
        _config_manager_instance = ConfigManager(config_path)

    return _config_manager_instance


# Convenience functions for common operations
def get_database_config() -> Dict[str, Any]:
    """Get database configuration"""
    return get_config_manager().get_database_config()


def get_raw_layer_config() -> Dict[str, Any]:
    """Get raw layer configuration"""
    return get_config_manager().get_raw_layer_config()


def get_pipeline_config() -> Dict[str, Any]:
    """Get pipeline configuration"""
    return get_config_manager().get_pipeline_config()


def get_file_watcher_config() -> Dict[str, Any]:
    """Get file watcher configuration"""
    return get_config_manager().get_file_watcher_config()


def get_all_config() -> Dict[str, Any]:
    """Get all configuration"""
    return get_config_manager().get_all()


def validate_configuration() -> bool:
    """
    Validate configuration and return status

    Returns:
        True if configuration is valid, False otherwise
    """
    try:
        config_manager = get_config_manager()
        errors = config_manager.validate_paths()

        if errors:
            logger.error("Configuration validation failed:")
            for error in errors:
                logger.error(f"  - {error}")
            return False

        logger.info("Configuration validation passed")
        return True

    except Exception as e:
        logger.error(f"Configuration validation error: {e}")
        return False


# Default global instance
config_manager = get_config_manager()


if __name__ == "__main__":
    # Test script when run directly
    print("=== Configuration Manager Test ===")

    try:
        # Get configuration manager
        cm = get_config_manager()

        # Print configuration summary
        print(f"Configuration file: {cm.config_path}")
        print(
            f"Database: {cm.config['database']['user']}@{cm.config['database']['host']}"
        )
        print(f"Input directory: {cm.config['raw_layer']['input_dir']}")
        print(f"Output directory: {cm.config['raw_layer']['output_dir']}")

        # Validate paths
        errors = cm.validate_paths()
        if errors:
            print("\nValidation errors:")
            for error in errors:
                print(f"  ❌ {error}")
        else:
            print("\n✅ All paths are valid")

    except Exception as e:
        print(f"Error: {e}")
