#!/usr/bin/env python3
"""
Telegram Bot - Bot for communication and notifications
Sends notifications about detected tokens and communicates with the executor
"""

import json
import logging
from typing import Dict
from datetime import datetime
from pathlib import Path

from telegram import Bot

logger = logging.getLogger(__name__)

_BASE_DIR = Path(__file__).resolve().parent


class TelegramBot:
    def __init__(self, config_path: str = "channels_config.json"):
        self.config = self._load_config(config_path)
        self.bot = None
        self.control_chat_id = self.config.get("control_chat_id")

    def _load_config(self, config_path: str) -> Dict:
        """Load bot configuration."""
        try:
            p = Path(config_path)
            if not p.is_absolute():
                p = (_BASE_DIR / p).resolve()
            with open(p, "r", encoding="utf-8") as f:
                config = json.load(f)
            return config
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            return {}

    async def initialize(self):
        """Initialize the Telegram bot."""
        try:
            self.bot = Bot(token=self.config["bot_token"])
            logger.info("Telegram Bot initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing bot: {e}")
            raise

    async def send_token_notification(self, token_data: Dict):
        """
        Send a notification about a detected token.

        Args:
            token_data: Detected token data
        """
        try:
            if not self.bot:
                await self.initialize()

            # Build the notification message
            message = self._create_token_notification_message(token_data)

            # Send to the control chat
            if self.control_chat_id:
                await self.bot.send_message(
                    chat_id=self.control_chat_id,
                    text=message,
                    parse_mode="HTML",
                )
                logger.info(
                    f"Notification sent to control chat: {token_data['token_address']}"
                )

        except Exception as e:
            logger.error(f"Error sending notification: {e}")

    def _create_token_notification_message(self, token_data: Dict) -> str:
        """
        Build a formatted notification message.

        Args:
            token_data: Token data

        Returns:
            HTML-formatted message
        """
        token_address = token_data["token_address"]
        channel_name = token_data["channel_name"]
        detected_at = token_data["detected_at"]

        # Format the timestamp
        try:
            dt = datetime.fromisoformat(detected_at)
            formatted_time = dt.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            formatted_time = detected_at

        message = f"""
🚨 <b>TOKEN DETECTED!</b> 🚨

📍 <b>Address:</b> <code>{token_address}</code>
📺 <b>Channel:</b> {channel_name}
⏰ <b>Detected at:</b> {formatted_time}
🔗 <b>Links:</b>
• <a href="https://solscan.io/token/{token_address}">Solscan</a>
• <a href="https://explorer.solana.com/address/{token_address}">Explorer</a>
• <a href="https://dexscreener.com/solana/{token_address}">DexScreener</a>

⚡ <b>Status:</b> Sending to executor...
        """.strip()

        return message

    async def send_error_notification(self, error_message: str, context: str = ""):
        """
        Send an error notification.

        Args:
            error_message: Error message
            context: Additional context
        """
        try:
            if not self.bot:
                await self.initialize()

            message = f"""
⚠️ <b>BOT ERROR</b> ⚠️

🔍 <b>Context:</b> {context}
❌ <b>Error:</b> {error_message}
⏰ <b>Time:</b> {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
            """.strip()

            if self.control_chat_id:
                await self.bot.send_message(
                    chat_id=self.control_chat_id,
                    text=message,
                    parse_mode="HTML",
                )

        except Exception as e:
            logger.error(f"Error sending error notification: {e}")

    async def send_status_update(self, status: str):
        """
        Send a status update.

        Args:
            status: Status message
        """
        try:
            if not self.bot:
                await self.initialize()

            message = f"""
📊 <b>BOT STATUS</b> 📊

{status}

⏰ <b>Updated at:</b> {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
            """.strip()

            if self.control_chat_id:
                await self.bot.send_message(
                    chat_id=self.control_chat_id,
                    text=message,
                    parse_mode="HTML",
                )

        except Exception as e:
            logger.error(f"Error sending status update: {e}")

    async def send_execution_result(
        self,
        token_address: str,
        success: bool,
        details: str = "",
        tx_hash: str = "",
    ):
        """
        Send the buy execution result.

        Args:
            token_address: Token address
            success: Whether execution succeeded
            details: Additional details
            tx_hash: Transaction hash (if available)
        """
        try:
            if not self.bot:
                await self.initialize()

            status_emoji = "✅" if success else "❌"
            status_text = "SUCCESS" if success else "FAILED"

            message = f"""
{status_emoji} <b>BUY EXECUTION</b> {status_emoji}

📍 <b>Token:</b> <code>{token_address}</code>
📊 <b>Status:</b> {status_text}
⏰ <b>Time:</b> {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
            """.strip()

            if tx_hash:
                message += f"\n🔗 <b>TX Hash:</b> <code>{tx_hash}</code>"

            if details:
                message += f"\n📝 <b>Details:</b> {details}"

            if self.control_chat_id:
                await self.bot.send_message(
                    chat_id=self.control_chat_id,
                    text=message,
                    parse_mode="HTML",
                )

        except Exception as e:
            logger.error(f"Error sending execution result: {e}")
