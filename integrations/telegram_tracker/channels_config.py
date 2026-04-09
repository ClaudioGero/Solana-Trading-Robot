#!/usr/bin/env python3
"""
Channels Config - Módulo para carregar e gerenciar configurações dos canais
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
    Carrega a configuração dos canais do arquivo JSON

    Args:
        config_path: Caminho para o arquivo de configuração

    Returns:
        Dicionário com as configurações carregadas
    """
    try:
        config_file = _resolve_config_path(config_path)

        if not config_file.exists():
            logger.error(f"Arquivo de configuração não encontrado: {config_path}")
            return _get_default_config()

        with open(config_file, "r", encoding="utf-8") as f:
            config = json.load(f)

        # Valida configuração básica
        if not _validate_config(config):
            logger.warning("Configuração inválida, usando configuração padrão")
            return _get_default_config()

        logger.info(f"Configuração carregada com sucesso: {config_path}")
        return config

    except Exception as e:
        logger.error(f"Erro ao carregar configuração: {e}")
        return _get_default_config()


def _validate_config(config: Dict) -> bool:
    """
    Valida se a configuração está correta

    Args:
        config: Configuração para validar

    Returns:
        True se a configuração é válida
    """
    required_fields = ["bot_token", "control_chat_id", "monitored_channels"]

    for field in required_fields:
        if field not in config:
            logger.error(f"Campo obrigatório ausente: {field}")
            return False

    # Verifica se o bot token não é o padrão
    if config["bot_token"] == "YOUR_BOT_TOKEN_HERE":
        logger.error("Bot token não configurado")
        return False

    # Verifica se o chat ID não é o padrão
    if config["control_chat_id"] == "YOUR_CONTROL_CHAT_ID_HERE":
        logger.error("Chat ID de controle não configurado")
        return False

    return True


def _get_default_config() -> Dict:
    """
    Retorna configuração padrão

    Returns:
        Configuração padrão
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
    Obtém lista de canais monitorados

    Args:
        config: Configuração carregada

    Returns:
        Lista de canais monitorados
    """
    channels = config.get("monitored_channels", [])
    return [channel for channel in channels if channel.get("enabled", True)]


def get_channel_by_id(config: Dict, channel_id: int) -> Optional[Dict]:
    """
    Obtém canal por ID

    Args:
        config: Configuração carregada
        channel_id: ID do canal

    Returns:
        Dados do canal ou None
    """
    channels = get_monitored_channels(config)

    for channel in channels:
        if channel.get("id") == channel_id:
            return channel

    return None


def get_channel_by_name(config: Dict, channel_name: str) -> Optional[Dict]:
    """
    Obtém canal por nome

    Args:
        config: Configuração carregada
        channel_name: Nome do canal

    Returns:
        Dados do canal ou None
    """
    channels = get_monitored_channels(config)

    for channel in channels:
        if channel.get("name") == channel_name:
            return channel

    return None


def should_monitor_channel(config: Dict, channel_id: int, channel_name: str) -> bool:
    """
    Verifica se um canal deve ser monitorado

    Args:
        config: Configuração carregada
        channel_id: ID do canal
        channel_name: Nome do canal

    Returns:
        True se o canal deve ser monitorado
    """
    # Verifica por ID
    channel = get_channel_by_id(config, channel_id)
    if channel:
        return True

    # Verifica por nome
    channel = get_channel_by_name(config, channel_name)
    if channel:
        return True

    return False


def get_filters(config: Dict) -> Dict:
    """
    Obtém filtros de mensagens

    Args:
        config: Configuração carregada

    Returns:
        Filtros configurados
    """
    return config.get("filters", {})


def get_notification_settings(config: Dict) -> Dict:
    """
    Obtém configurações de notificação

    Args:
        config: Configuração carregada

    Returns:
        Configurações de notificação
    """
    return config.get("notification_settings", {})


def get_bot_settings(config: Dict) -> Dict:
    """
    Obtém configurações do bot

    Args:
        config: Configuração carregada

    Returns:
        Configurações do bot
    """
    return config.get("bot_settings", {})


# Função de teste
def test_config():
    """Testa o carregamento da configuração"""
    config = load_channels_config()

    print("Configuração carregada:")
    print(f"- Bot Token: {config['bot_token']}")
    print(f"- Control Chat ID: {config['control_chat_id']}")
    print(f"- Canais monitorados: {len(get_monitored_channels(config))}")
    print(f"- Filtros: {get_filters(config)}")


if __name__ == "__main__":
    test_config()

