#!/usr/bin/env python3
"""
Token Detector - Detecta endereços de tokens Solana de forma simples
Lógica: Se uma palavra tem 32-44 caracteres, provavelmente é um token!
"""

import re
import logging
from typing import List, Dict
from base58 import b58decode

logger = logging.getLogger(__name__)


class TokenDetector:
    def __init__(self, config_path: str = "channels_config.json"):
        # Caracteres válidos para endereços Solana (base58)
        self.valid_chars = r"[1-9A-HJ-NP-Za-km-z]"

        # Lista básica de endereços do sistema para filtrar
        self.system_addresses = {
            "11111111111111111111111111111111",  # System Program
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  # Token Program
            "So11111111111111111111111111111111111111112",  # Wrapped SOL
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
        }

    def extract_tokens(self, message_text: str) -> List[str]:
        """
        Extrai tokens usando lógica super simples:
        Qualquer símbolo = separador → Procura sequências de 32-44 caracteres base58

        Args:
            message_text: Texto da mensagem do Telegram

        Returns:
            Lista de endereços de tokens encontrados
        """
        try:
            # Encontra TODAS as sequências de caracteres base58 de 32-44 chars
            # Qualquer coisa que não seja base58 = separador automático
            pattern = r"[1-9A-HJ-NP-Za-km-z]{32,44}"
            potential_tokens = re.findall(pattern, message_text)

            tokens_found = []

            for token in potential_tokens:
                # Filtra endereços do sistema
                if token not in self.system_addresses:
                    # Evita duplicatas
                    if token not in tokens_found:
                        # Validação final com base58
                        if self._is_valid_format(token):
                            tokens_found.append(token)
                            logger.info(f"Token encontrado: {token}")

            return tokens_found

        except Exception as e:
            logger.error(f"Erro ao extrair tokens: {e}")
            return []

    def _is_valid_format(self, address: str) -> bool:
        """
        Verifica se uma string tem formato válido de endereço Solana

        Args:
            address: String para verificar

        Returns:
            True se tem formato válido
        """
        try:
            # Verifica se contém apenas caracteres válidos para base58
            if not re.match(f"^{self.valid_chars}+$", address):
                return False

            # Tenta decodificar como base58 (validação final)
            decoded = b58decode(address)
            if len(decoded) != 32:  # Endereços Solana são 32 bytes
                return False

            return True

        except Exception:
            return False

    def extract_and_process_tokens(
        self,
        message_text: str,
        channel_name: str = "",
        original_message: str = "",
        message_date: str = "",
    ) -> List[Dict]:
        """
        Extrai tokens e retorna dados prontos para processamento

        Args:
            message_text: Texto da mensagem do Telegram
            channel_name: Nome do canal
            original_message: Mensagem original completa
            message_date: Data da mensagem

        Returns:
            Lista de dicionários com dados dos tokens
        """
        tokens = self.extract_tokens(message_text)

        processed_tokens = []
        for token_address in tokens:
            token_data = {
                "token_address": token_address,
                "channel_name": channel_name,
                "original_message": original_message,
                "detected_at": message_date,
                "source": "telegram",
                "detection_method": "word_length",
                "confidence": 0.95,  # Alta confiança para este método simples
            }
            processed_tokens.append(token_data)

        return processed_tokens

