#!/usr/bin/env python3
"""
Telegram Listener - Main monitor for Telegram channels
Monitors specific channels and detects Solana token addresses

Integration note (Solana bot):
- When a token/mint is detected, we enqueue a TradeIntent JSON into Redis:
  key: sb:q:trade_intents
  venue: pumpfun (quick-win, matches current bot.json allowlist)
"""

import asyncio
import atexit
import fcntl
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict
from urllib.request import Request, urlopen
from urllib.error import HTTPError

import telegram
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

from token_detector import TokenDetector
from telegram_bot import TelegramBot
from channels_config import load_channels_config
from redis_bridge import enqueue_trade_intent, enqueue_alert

# Base directory for this integration (so relative paths work from any CWD).
_BASE_DIR = Path(__file__).resolve().parent

# Ensure log directory exists (avoids FileHandler crash on first run).
(_BASE_DIR / "logs").mkdir(parents=True, exist_ok=True)

# Logging configuration
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler(str(_BASE_DIR / "logs" / "telegram_listener.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class TelegramListener:
    def __init__(self, config_path: str = "channels_config.json"):
        # Resolve config path relative to this file, so it works when launched from repo root.
        p = Path(config_path)
        if not p.is_absolute():
            p = (_BASE_DIR / p).resolve()
        self.config_path = str(p)
        self.config = load_channels_config(self.config_path)
        self.token_detector = TokenDetector()
        self.telegram_bot = TelegramBot(self.config_path)
        self.application = None
        self.monitored_channels = set()
        self._lock_fp = None

    def _acquire_single_instance_lock(self):
        """
        Prevents multiple instances from polling with the same bot token.
        Telegram will 409 Conflict if two processes call getUpdates concurrently.
        """
        lock_path = os.path.abspath("telegram_listener.lock")
        fp = open(lock_path, "w")
        try:
            fcntl.flock(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError(
                f"Another telegram_listener instance is already running (lock: {lock_path}). "
                f"Stop the other process and retry."
            )

        fp.write(str(os.getpid()))
        fp.flush()
        self._lock_fp = fp

        def _cleanup():
            try:
                fcntl.flock(fp.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
            try:
                fp.close()
            except Exception:
                pass
            # best-effort remove lock file
            try:
                os.remove(lock_path)
            except Exception:
                pass

        atexit.register(_cleanup)

    async def _error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE):
        """
        PTB error handler. In particular, make 409 Conflict actionable & stop.
        """
        err = getattr(context, "error", None)
        if isinstance(err, telegram.error.Conflict):
            logger.error(
                "Telegram polling conflict (409): another process is calling getUpdates with the same bot token. "
                "Stop the other instance and restart this listener."
            )
            try:
                if self.application and hasattr(self.application, "stop_running"):
                    self.application.stop_running()
            except Exception:
                pass
            return

        logger.exception("Unhandled Telegram listener error", exc_info=err)

    def run_polling(self):
        """
        Start the Telegram listener via Application.run_polling().

        PTB 20+ (incl. 22.x) removed Updater.idle()/wait_until_closed(). The supported
        long-running entrypoint is Application.run_polling(), which also supports
        async handlers.
        """
        try:
            self._acquire_single_instance_lock()
            self.application = Application.builder().token(self.config["bot_token"]).build()
            self.application.add_error_handler(self._error_handler)

            # PTB compatibility: some versions use ChatType.CHANNEL (singular), others differ.
            chat_type_channel = getattr(filters.ChatType, "CHANNEL", None)
            if chat_type_channel is None:
                chat_type_channel = getattr(filters.ChatType, "CHANNELS", None)
            if chat_type_channel is None:
                # Fallback: do not filter by chat type (still requires TEXT).
                chat_type_channel = filters.ALL

            self.application.add_handler(
                # Also handle caption-only posts (common for channel images/videos).
                MessageHandler((filters.TEXT | filters.CAPTION) & chat_type_channel, self.handle_message)
            )

            logger.info("Telegram Listener started successfully (run_polling)")
            self.application.run_polling()

        except Exception as e:
            logger.error(f"Error starting Telegram Listener: {e}")
            raise

    async def start_listening_async(self):
        """Start the Telegram listener (fallback for older PTB versions)."""
        try:
            # Initialize the Telegram application
            self.application = Application.builder().token(self.config["bot_token"]).build()

            # PTB compatibility: some versions use ChatType.CHANNEL (singular), others differ.
            chat_type_channel = getattr(filters.ChatType, "CHANNEL", None)
            if chat_type_channel is None:
                chat_type_channel = getattr(filters.ChatType, "CHANNELS", None)
            if chat_type_channel is None:
                # Fallback: do not filter by chat type (still requires TEXT).
                chat_type_channel = filters.ALL

            # Add handler for messages
            self.application.add_handler(
                MessageHandler((filters.TEXT | filters.CAPTION) & chat_type_channel, self.handle_message)
            )

            # Start the bot
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling()

            logger.info("Telegram Listener started successfully")

            # Keep the bot running (PTB < 20 had updater.idle()).
            if hasattr(self.application.updater, "idle"):
                await self.application.updater.idle()
            else:
                # Last resort: block forever.
                await asyncio.Event().wait()

        except Exception as e:
            logger.error(f"Error starting Telegram Listener: {e}")
            raise

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process messages received from monitored channels."""
        try:
            # PTB can deliver channel posts via channel_post; effective_message covers both.
            message = update.effective_message
            if message is None:
                return
            chat_id = message.chat_id
            chat = message.chat
            chat_title = getattr(chat, "title", None) or getattr(chat, "full_name", None) or ""
            chat_username = getattr(chat, "username", None) or ""

            # Channel posts can be text or media with caption.
            message_text = message.text or getattr(message, "caption", None) or ""
            if not message_text:
                return
            message_date = message.date
            message_id = getattr(message, "message_id", None)

            # Check whether the channel is in the monitored list
            if not self._should_monitor_channel(chat_id, chat_title, chat_username):
                return

            logger.info(f"Message received from channel: {chat_title} (ID: {chat_id})")
            logger.debug(f"Content: {message_text[:100]}...")

            # Extract and process tokens in a single operation
            processed_tokens = self.token_detector.extract_and_process_tokens(
                message_text=message_text,
                channel_name=chat_title,
                original_message=message_text,
                message_date=message_date.isoformat(),
            )

            if processed_tokens:
                logger.info(f"Tokens found in {chat_title}: {len(processed_tokens)} tokens")

                # Process each detected token
                for token_data in processed_tokens:
                    # Attach Telegram metadata for idempotent signatures + audit.
                    token_data["tg_chat_id"] = chat_id
                    token_data["tg_message_id"] = message_id
                    token_data["tg_message_date"] = message_date.isoformat()
                    await self._process_token_data(token_data)

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def _should_monitor_channel(self, chat_id: int, chat_title: str, chat_username: str = "") -> bool:
        """Check whether the channel should be monitored."""
        # If no channels are configured, monitor all of them (development mode)
        if not self.config.get("monitored_channels"):
            logger.info(f"Development mode: monitoring channel {chat_title}")
            return True

        # Check by chat ID, username, or name
        for channel in self.config["monitored_channels"]:
            if isinstance(channel, dict):
                # Check whether it is enabled
                if not channel.get("enabled", True):
                    continue

                # Check ID (if available)
                if "id" in channel and channel["id"] == chat_id:
                    logger.info(f"✅ Channel monitored by ID: {chat_title}")
                    return True

                # Check username (without @)
                if "username" in channel:
                    cfg_username = channel["username"].lstrip("@").lower()
                    actual_username = (chat_username or "").lstrip("@").lower()
                    if actual_username and cfg_username == actual_username:
                        logger.info(f"✅ Channel monitored by username: @{actual_username}")
                        return True

                # Check exact name
                if "name" in channel and channel["name"] == chat_title:
                    logger.info(f"✅ Channel monitored by name: {chat_title}")
                    return True

            elif channel == chat_id:  # Compatibility with plain IDs
                logger.info(f"✅ Channel monitored by plain ID: {chat_title}")
                return True

        logger.debug(f"❌ Channel not monitored: {chat_title} (ID: {chat_id})")
        return False

    async def _process_token_data(self, token_data: Dict):
        """Process data for a token that was already extracted and validated."""
        try:
            # Send to the executor (via Redis)
            await self._send_to_executor(token_data)

            # Send a notification to the control chat
            await self.telegram_bot.send_token_notification(token_data)

            logger.info(f"Token processed successfully: {token_data['token_address']}")

        except Exception as e:
            logger.error(
                f"Error processing token {token_data.get('token_address', 'unknown')}: {e}"
            )

    async def _send_to_executor(self, token_data: Dict):
        """Send the detected mint to the bot via Redis (sb:q:trade_intents)."""
        try:
            redis_url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
            now = datetime.now(timezone.utc).isoformat()

            mint = token_data.get("token_address", "")
            channel_name = token_data.get("channel_name", "")
            tg_chat_id = token_data.get("tg_chat_id")
            tg_msg_id = token_data.get("tg_message_id")

            # Make a deterministic-ish signature to help with dedupe/audit.
            # If message_id is missing, fall back to uuid.
            if tg_chat_id is not None and tg_msg_id is not None:
                sig = f"tg:{tg_chat_id}:{tg_msg_id}:{mint}"
            else:
                sig = f"tg:{uuid.uuid4()}:{mint}"

            intent = {
                "signature": sig,
                "slot": 0,
                "wallet": f"telegram:{channel_name}",
                "side": "buy",
                "mint": mint,
                "notional_sol": 0.0,
                "venue": "pumpfun",
                "observed_at": now,
                "classified_at": now,
                "amount_in_base_units": None,
                "created_at": now,
            }

            # Enqueue TradeIntent into the same pipeline as copytrading.
            reply = enqueue_trade_intent(redis_url, intent)
            logger.info(f"Enqueued TradeIntent to Redis reply={reply!r}")

            # Optional: also enqueue an alert for the Rust alert worker (same channel as core-app alerts).
            try:
                mcap = _birdeye_market_cap_usd(mint)
                mcap_txt = f" at ${mcap:,.0f} mcap" if mcap is not None else ""
                alert = {
                    "ts": now,
                    "kind": "tg_buy_intent_enqueued",
                    "message": f"{channel_name} called {mint}{mcap_txt}\nsig: {sig}",
                }
                _ = enqueue_alert(redis_url, alert)
            except Exception:
                pass

        except Exception as e:
            logger.error(f"Error sending to executor (Redis): {e}")


async def main():
    """Main function."""
    listener = TelegramListener()
    await listener.start_listening_async()


def _birdeye_market_cap_usd(mint: str):
    """
    Best-effort fetch market cap from Birdeye market-data endpoint.
    Returns float USD market cap or None if unavailable/rate-limited.
    """
    key = os.environ.get("BIRDEYE_API_KEY", "").strip()
    if not key or not mint:
        return None
    url = f"https://public-api.birdeye.so/defi/v3/token/market-data?address={mint}"
    req = Request(
        url,
        headers={
            "X-API-KEY": key,
            "x-chain": "solana",
            "User-Agent": "solana-bot-telegram-tracker/1.0",
        },
    )
    try:
        with urlopen(req, timeout=6) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except HTTPError:
        return None
    except Exception:
        return None
    try:
        import json

        j = json.loads(raw)
        data = j.get("data") or {}
        # Accept common key variants.
        mc = data.get("market_cap") or data.get("marketCap") or data.get("market_cap_usd") or data.get("mcap")
        if mc is None:
            return None
        if isinstance(mc, (int, float)):
            return float(mc)
        if isinstance(mc, str):
            return float(mc)
    except Exception:
        return None
    return None

if __name__ == "__main__":
    # Prefer PTB 20+ run_polling() when available.
    # IMPORTANT: run_polling() manages its own event loop and must NOT run under asyncio.run().
    if hasattr(Application, "run_polling"):
        TelegramListener().run_polling()
    else:
        asyncio.run(main())
