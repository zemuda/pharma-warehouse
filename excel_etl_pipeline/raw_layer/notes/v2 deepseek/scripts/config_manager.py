#!/usr/bin/env python3
"""
Configuration Manager for YAML configuration files
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional


class ConfigManager:
    """Manage YAML configuration files"""

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = (
            Path(config_path) if config_path else self._find_config_file()
        )
        self.config = self._load_config()
        self._ensure_directories()

    def _find_config_file(self) -> Path:
        """Find configuration file in common locations"""
        possible_paths = [
            Path("config.yaml"),
            Path("config.yml"),
            Path(__file__).parent.parent / "config.yaml",  # Project root
            Path(__file__).parent / "config.yaml",  # Same directory
            Path.cwd() / "config.yaml",  # Current working directory
        ]

        for path in possible_paths:
            if path.exists():
                print(f"Found configuration file at: {path}")
                return path

        raise FileNotFoundError(
            "Could not find config.yaml file. Please create one or specify the path."
        )

    def _load_config(self) -> Dict[str, Any]:
        """Load YAML configuration file"""
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            # Normalize path separators
            self._normalize_paths(config)
            return config
        except yaml.YAMLError as e:
            raise RuntimeError(f"Failed to parse YAML configuration: {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to load configuration: {e}")

    def _normalize_paths(self, config: Dict[str, Any]):
        """Normalize paths to use system-specific separators"""

        def process_value(value):
            if isinstance(value, dict):
                for k, v in value.items():
                    value[k] = process_value(v)
                return value
            elif isinstance(value, list):
                return [process_value(item) for item in value]
            elif isinstance(value, str) and ("/" in value or "\\" in value):
                # Convert to Path object and back to string for normalization
                return str(Path(value))
            else:
                return value

        process_value(config)

    def _ensure_directories(self):
        """Ensure necessary directories exist"""
        directories = []

        # Add directories from configuration
        if "raw_layer" in self.config:
            raw_cfg = self.config["raw_layer"]
            directories.extend(
                [
                    Path(raw_cfg.get("input_dir", "./raw_data/input")),
                    Path(raw_cfg.get("output_dir", "./raw_data/output")),
                    Path(
                        raw_cfg.get(
                            "state_file", "./raw_data/state/processing_state.json"
                        )
                    ).parent,
                ]
            )

        if "file_watcher" in self.config:
            watch_dir = self.config["file_watcher"].get("watch_dir", "./raw_data/input")
            directories.append(Path(watch_dir))

        # Create directories
        for directory in directories:
            try:
                directory.mkdir(parents=True, exist_ok=True)
                print(f"Ensured directory exists: {directory}")
            except Exception as e:
                print(f"Warning: Could not create directory {directory}: {e}")

    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration"""
        return self.config.get("database", {})

    def get_raw_layer_config(self) -> Dict[str, Any]:
        """Get raw layer configuration"""
        return self.config.get("raw_layer", {})

    def get_pipeline_config(self) -> Dict[str, Any]:
        """Get pipeline configuration"""
        return self.config.get("pipeline", {})

    def get_file_watcher_config(self) -> Dict[str, Any]:
        """Get file watcher configuration"""
        return self.config.get("file_watcher", {})

    def get_paths(self) -> Dict[str, Any]:
        """Get paths configuration"""
        return self.config.get("paths", {})

    def get_all(self) -> Dict[str, Any]:
        """Get all configuration"""
        return self.config

    def update_config(self, updates: Dict[str, Any]):
        """Update configuration with new values"""

        def update_dict(original, new):
            for key, value in new.items():
                if (
                    key in original
                    and isinstance(original[key], dict)
                    and isinstance(value, dict)
                ):
                    update_dict(original[key], value)
                else:
                    original[key] = value

        update_dict(self.config, updates)
        self._normalize_paths(self.config)

    def save_config(self, path: Optional[str] = None):
        """Save configuration to file"""
        save_path = Path(path) if path else self.config_path
        try:
            with open(save_path, "w", encoding="utf-8") as f:
                yaml.dump(self.config, f, default_flow_style=False, sort_keys=False)
            print(f"Configuration saved to: {save_path}")
        except Exception as e:
            raise RuntimeError(f"Failed to save configuration: {e}")


# Global configuration instance (lazy initialization)
_config_manager_instance = None


def get_config_manager(config_path: Optional[str] = None) -> ConfigManager:
    """Get or create global configuration manager instance"""
    global _config_manager_instance

    if _config_manager_instance is None:
        _config_manager_instance = ConfigManager(config_path)
    elif config_path and Path(config_path) != _config_manager_instance.config_path:
        # If different config path requested, create new instance
        _config_manager_instance = ConfigManager(config_path)

    return _config_manager_instance


# Convenience functions
def get_database_config():
    """Get database configuration"""
    return get_config_manager().get_database_config()


def get_raw_layer_config():
    """Get raw layer configuration"""
    return get_config_manager().get_raw_layer_config()


def get_pipeline_config():
    """Get pipeline configuration"""
    return get_config_manager().get_pipeline_config()


def get_file_watcher_config():
    """Get file watcher configuration"""
    return get_config_manager().get_file_watcher_config()


def get_all_config():
    """Get all configuration"""
    return get_config_manager().get_all()


# Default global instance
config_manager = get_config_manager()
