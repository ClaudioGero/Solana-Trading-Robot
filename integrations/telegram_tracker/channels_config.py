#!/usr/bin/env python3
"""
Channels Config - Module for loading and managing channel configuration
"""

import json
import logging
from typing import Dict, List, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

_BASE_DIR = Path(__file__).resolve().parent


def _resolve_config_path(config_path: str) -> Path:
    """
    Resolve config path robustly:
    - absolute paths are used as-is
    - relative paths are resolved relative to this module directory
      (so the script can be launched from any CWD)
    """
    p = Path(config_path)
    if p.is_absolute():
        return p
    return (_BASE_DIR / p).resolve()


def load_channels_config(config_path: str = "channels_config.json") -> Dict:
    """
    Load channel configuration from the JSON file.

    Args:
        config_path: Path to the configuration file

    Returns:
        Dictionary containing the loaded configuration
    """
    try:
        config_file = _resolve_config_path(config_path)

        if not config_file.exists():
            logger.error(f"Configuration file not found: {config_path}")
            return _get_default_config()

        with open(config_file, "r", encoding="utf-8") as f:
            config = json.load(f)

        # Validate basic configuration
        if not _validate_config(config):
            logger.warning("Invalid configuration, using default configuration")
            return _get_default_config()

        logger.info(f"Configuration loaded successfully: {config_path}")
        return config

    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        return _get_default_config()


def _validate_config(config: Dict) -> bool:
    """
    Validate whether the configuration is correct.

    Args:
        config: Configuration to validate

    Returns:
        True if the configuration is valid
    """
    required_fields = ["bot_token", "control_chat_id", "monitored_channels"]

    for field in required_fields:
        if field not in config:
            logger.error(f"Missing required field: {field}")
            return False

    # Check that the bot token is not still the placeholder
    if config["bot_token"] == "YOUR_BOT_TOKEN_HERE":
        logger.error("Bot token not configured")
        return False

    # Check that the control chat ID is not still the placeholder
    if config["control_chat_id"] == "YOUR_CONTROL_CHAT_ID_HERE":
        logger.error("Control chat ID not configured")
        return False

    return True


def _get_default_config() -> Dict:
    """
    Return the default configuration.

    Returns:
        Default configuration
    """
    return {
        "bot_token": "YOUR_BOT_TOKEN_HERE",
        "control_chat_id": "YOUR_CONTROL_CHAT_ID_HERE",
        "monitored_channels": [],
        "filters": {
            "min_message_length": 10,
            "exclude_keywords": ["scam", "fake", "test"],
            "include_keywords": ["token", "launch", "new"],
            "require_solana_address": True,
        },
        "notification_settings": {
            "send_to_control_chat": True,
            "send_to_executor": True,
            "log_all_messages": False,
        },
        "bot_settings": {
            "webhook_enabled": False,
            "polling_interval": 1,
            "max_retries": 3,
            "timeout": 30,
        },
    }


def get_monitored_channels(config: Dict) -> List[Dict]:
    """
    Get the list of monitored channels.

    Args:
        config: Loaded configuration

    Returns:
        List of monitored channels
    """
    channels = config.get("monitored_channels", [])
    return [channel for channel in channels if channel.get("enabled", True)]


def get_channel_by_id(config: Dict, channel_id: int) -> Optional[Dict]:
    """
    Get a channel by ID.

    Args:
        config: Loaded configuration
        channel_id: Channel ID

    Returns:
        Channel data or None
    """
    channels = get_monitored_channels(config)

    for channel in channels:
        if channel.get("id") == channel_id:
            return channel

    return None


def get_channel_by_name(config: Dict, channel_name: str) -> Optional[Dict]:
    """
    Get a channel by name.

    Args:
        config: Loaded configuration
        channel_name: Channel name

    Returns:
        Channel data or None
    """
    channels = get_monitored_channels(config)

    for channel in channels:
        if channel.get("name") == channel_name:
            return channel

    return None


def should_monitor_channel(config: Dict, channel_id: int, channel_name: str) -> bool:
    """
    Check whether a channel should be monitored.

    Args:
        config: Loaded configuration
        channel_id: Channel ID
        channel_name: Channel name

    Returns:
        True if the channel should be monitored
    """
    # Check by ID
    channel = get_channel_by_id(config, channel_id)
    if channel:
        return True

    # Check by name
    channel = get_channel_by_name(config, channel_name)
    if channel:
        return True

    return False


def get_filters(config: Dict) -> Dict:
    """
    Get message filters.

    Args:
        config: Loaded configuration

    Returns:
        Configured filters
    """
    return config.get("filters", {})


def get_notification_settings(config: Dict) -> Dict:
    """
    Get notification settings.

    Args:
        config: Loaded configuration

    Returns:
        Notification settings
    """
    return config.get("notification_settings", {})


def get_bot_settings(config: Dict) -> Dict:
    """
    Get bot settings.

    Args:
        config: Loaded configuration

    Returns:
        Bot settings
    """
    return config.get("bot_settings", {})
