import json
import os
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class ConfigManager:
    def __init__(self, properties_path: str, profile_path: str):
        self._validate_file_exists(properties_path, "Properties")
        self._validate_file_exists(profile_path, "Profile")
        self.properties = self._load_properties(properties_path)
        self._profile_path = profile_path
        self.profile_config = self._load_profile(profile_path)
        self._validate_required_properties()
        self._validate_required_profile_fields()
    
    def _validate_file_exists(self, path: str, file_type: str) -> None:
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"{file_type} file not found: {path}\n"
                f"Please ensure the configuration file exists.\n"
                f"Run with: python parallel_streaming_orchestrator.py <orders> <instances> <config_file> <profile_file>"
            )
    
    def _validate_required_properties(self) -> None:
        required_keys = [
            "pipe.orders.name",
            "pipe.order_items.name",
            "channel.orders.name",
            "channel.order_items.name"
        ]
        missing = [key for key in required_keys if key not in self.properties]
        if missing:
            raise ValueError(
                f"Required properties missing from config file: {', '.join(missing)}\n"
                f"Please ensure all pipe and channel names are configured."
            )
    
    def _validate_required_profile_fields(self) -> None:
        required_fields = ["user", "account", "url", "database", "schema", "warehouse"]
        missing = [field for field in required_fields if field not in self.profile_config]
        if missing:
            raise ValueError(
                f"Required profile fields missing: {', '.join(missing)}\n"
                f"Please ensure your profile.json contains all required Snowflake connection details."
            )
        if "private_key" not in self.profile_config and "private_key_file" not in self.profile_config:
            raise ValueError(
                "Profile must contain either 'private_key' (inline PEM string) "
                "or 'private_key_file' (path to .p8 file)."
            )

    def _load_properties(self, path: str) -> Dict[str, str]:
        props = {}
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    if "=" in line:
                        key, value = line.split("=", 1)
                        props[key.strip()] = value.strip()
        logger.info(f"Loaded configuration from: {path}")
        return props

    def _load_profile(self, path: str) -> Dict[str, Any]:
        with open(path, "r") as f:
            profile = json.load(f)
        logger.info(f"Loaded Snowflake profile from: {path}")
        return profile

    def get_property(self, key: str, default: str = None) -> str:
        return self.properties.get(key, default)

    def get_int_property(self, key: str, default: int = None) -> int:
        value = self.get_property(key)
        return int(value) if value is not None else default

    def get_snowflake_user(self) -> str:
        return self.profile_config["user"]

    def get_snowflake_account(self) -> str:
        return self.profile_config["account"]

    def get_snowflake_url(self) -> str:
        return self.profile_config["url"]

    def get_private_key(self) -> str:
        if "private_key_file" in self.profile_config:
            key_path = self.profile_config["private_key_file"]
            if not os.path.isabs(key_path):
                key_path = os.path.join(os.path.dirname(os.path.abspath(self._profile_path)), key_path)
            with open(key_path, "r") as f:
                return f.read()
        return self.profile_config["private_key"]

    def get_database(self) -> str:
        return self.profile_config["database"]

    def get_schema(self) -> str:
        return self.profile_config["schema"]

    def get_warehouse(self) -> str:
        return self.profile_config["warehouse"]

    def get_role(self) -> str:
        return self.profile_config.get("role", "AUTOMATED_INTELLIGENCE_ADMIN")
