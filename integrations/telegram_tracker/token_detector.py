#!/usr/bin/env python3
"""
Token Detector - Detects Solana token addresses in a simple way
Logic: If a word has 32-44 characters, it is probably a token
"""

import re
import logging
from typing import List, Dict
from base58 import b58decode

logger = logging.getLogger(__name__)


class TokenDetector:
    def __init__(self, config_path: str = "channels_config.json"):
        # Valid characters for Solana addresses (base58)
        self.valid_chars = r"[1-9A-HJ-NP-Za-km-z]"

        # Basic list of system addresses to filter out
        self.system_addresses = {
            "11111111111111111111111111111111",  # System Program
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  # Token Program
            "So11111111111111111111111111111111111111112",  # Wrapped SOL
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
        }

    def extract_tokens(self, message_text: str) -> List[str]:
        """
        Extract tokens using a very simple approach:
        Any symbol = separator -> look for base58 sequences with 32-44 characters

        Args:
            message_text: Telegram message text

        Returns:
            List of found token addresses
        """
        try:
            # Find ALL base58 character sequences of 32-44 chars
            # Anything that is not base58 becomes an automatic separator
            pattern = r"[1-9A-HJ-NP-Za-km-z]{32,44}"
            potential_tokens = re.findall(pattern, message_text)

            tokens_found = []

            for token in potential_tokens:
                # Filter out system addresses
                if token not in self.system_addresses:
                    # Avoid duplicates
                    if token not in tokens_found:
                        # Final validation with base58
                        if self._is_valid_format(token):
                            tokens_found.append(token)
                            logger.info(f"Token found: {token}")

            return tokens_found

        except Exception as e:
            logger.error(f"Error extracting tokens: {e}")
            return []

    def _is_valid_format(self, address: str) -> bool:
        """
        Check whether a string has a valid Solana address format.

        Args:
            address: String to check

        Returns:
            True if the format is valid
        """
        try:
            # Check that it contains only valid base58 characters
            if not re.match(f"^{self.valid_chars}+$", address):
                return False

            # Try decoding as base58 (final validation)
            decoded = b58decode(address)
            if len(decoded) != 32:  # Solana addresses are 32 bytes
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
        Extract tokens and return data ready for processing.

        Args:
            message_text: Telegram message text
            channel_name: Channel name
            original_message: Full original message
            message_date: Message date

        Returns:
            List of dictionaries with token data
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
                "confidence": 0.95,  # High confidence for this simple method
            }
            processed_tokens.append(token_data)

        return processed_tokens
