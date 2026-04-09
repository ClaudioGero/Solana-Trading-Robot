#!/usr/bin/env python3
"""
Telegram Bot - Bot para comunicação e notificações
Envia notificações sobre tokens encontrados e comunica com o executor
"""

import asyncio
import json
import logging
from typing import Dict, Optional
from datetime import datetime
from pathlib import Path

import telegram
from telegram import Bot
from telegram.error import TelegramError

logger = logging.getLogger(__name__)

_BASE_DIR = Path(__file__).resolve().parent


class TelegramBot:
    def __init__(self, config_path: str = "channels_config.json"):
        self.config = self._load_config(config_path)
        self.bot = None
        self.control_chat_id = self.config.get("control_chat_id")

    def _load_config(self, config_path: str) -> Dict:
        """Carrega configuração do bot"""
        try:
            p = Path(config_path)
            if not p.is_absolute():
                p = (_BASE_DIR / p).resolve()
            with open(p, "r", encoding="utf-8") as f:
                config = json.load(f)
            return config
        except Exception as e:
            logger.error(f"Erro ao carregar configuração: {e}")
            return {}

    async def initialize(self):
        """Inicializa o bot do Telegram"""
        try:
            self.bot = Bot(token=self.config["bot_token"])
            logger.info("Telegram Bot inicializado com sucesso")
        except Exception as e:
            logger.error(f"Erro ao inicializar bot: {e}")
            raise

    async def send_token_notification(self, token_data: Dict):
        """
        Envia notificação sobre token encontrado

        Args:
            token_data: Dados do token encontrado
        """
        try:
            if not self.bot:
                await self.initialize()

            # Cria mensagem de notificação
            message = self._create_token_notification_message(token_data)

            # Envia para o chat de controle
            if self.control_chat_id:
                await self.bot.send_message(
                    chat_id=self.control_chat_id,
                    text=message,
                    parse_mode="HTML",
                )
                logger.info(
                    f"Notificação enviada para chat de controle: {token_data['token_address']}"
                )

        except Exception as e:
            logger.error(f"Erro ao enviar notificação: {e}")

    def _create_token_notification_message(self, token_data: Dict) -> str:
        """
        Cria mensagem de notificação formatada

        Args:
            token_data: Dados do token

        Returns:
            Mensagem formatada em HTML
        """
        token_address = token_data["token_address"]
        channel_name = token_data["channel_name"]
        detected_at = token_data["detected_at"]

        # Formata a data
        try:
            dt = datetime.fromisoformat(detected_at)
            formatted_time = dt.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            formatted_time = detected_at

        message = f"""
🚨 <b>TOKEN DETECTADO!</b> 🚨

📍 <b>Endereço:</b> <code>{token_address}</code>
📺 <b>Canal:</b> {channel_name}
⏰ <b>Detectado em:</b> {formatted_time}
🔗 <b>Links:</b>
• <a href="https://solscan.io/token/{token_address}">Solscan</a>
• <a href="https://explorer.solana.com/address/{token_address}">Explorer</a>
• <a href="https://dexscreener.com/solana/{token_address}">DexScreener</a>

⚡ <b>Status:</b> Enviando para executor...
        """.strip()

        return message

    async def send_error_notification(self, error_message: str, context: str = ""):
        """
        Envia notificação de erro

        Args:
            error_message: Mensagem de erro
            context: Contexto adicional
        """
        try:
            if not self.bot:
                await self.initialize()

            message = f"""
⚠️ <b>ERRO NO BOT</b> ⚠️

🔍 <b>Contexto:</b> {context}
❌ <b>Erro:</b> {error_message}
⏰ <b>Hora:</b> {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
            """.strip()

            if self.control_chat_id:
                await self.bot.send_message(
                    chat_id=self.control_chat_id,
                    text=message,
                    parse_mode="HTML",
                )

        except Exception as e:
            logger.error(f"Erro ao enviar notificação de erro: {e}")

    async def send_status_update(self, status: str):
        """
        Envia atualização de status

        Args:
            status: Mensagem de status
        """
        try:
            if not self.bot:
                await self.initialize()

            message = f"""
📊 <b>STATUS DO BOT</b> 📊

{status}

⏰ <b>Atualizado em:</b> {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
            """.strip()

            if self.control_chat_id:
                await self.bot.send_message(
                    chat_id=self.control_chat_id,
                    text=message,
                    parse_mode="HTML",
                )

        except Exception as e:
            logger.error(f"Erro ao enviar status: {e}")

    async def send_execution_result(
        self,
        token_address: str,
        success: bool,
        details: str = "",
        tx_hash: str = "",
    ):
        """
        Envia resultado da execução de compra

        Args:
            token_address: Endereço do token
            success: Se a execução foi bem-sucedida
            details: Detalhes adicionais
            tx_hash: Hash da transação (se disponível)
        """
        try:
            if not self.bot:
                await self.initialize()

            status_emoji = "✅" if success else "❌"
            status_text = "SUCESSO" if success else "FALHA"

            message = f"""
{status_emoji} <b>EXECUÇÃO DE COMPRA</b> {status_emoji}

📍 <b>Token:</b> <code>{token_address}</code>
📊 <b>Status:</b> {status_text}
⏰ <b>Hora:</b> {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}
            """.strip()

            if tx_hash:
                message += f"\n🔗 <b>TX Hash:</b> <code>{tx_hash}</code>"

            if details:
                message += f"\n📝 <b>Detalhes:</b> {details}"

            if self.control_chat_id:
                await self.bot.send_message(
                    chat_id=self.control_chat_id,
                    text=message,
                    parse_mode="HTML",
                )

        except Exception as e:
            logger.error(f"Erro ao enviar resultado de execução: {e}")

    async def test_connection(self) -> bool:
        """
        Testa a conexão com o Telegram

        Returns:
            True se a conexão está funcionando
        """
        try:
            if not self.bot:
                await self.initialize()

            # Testa obtendo informações do bot
            bot_info = await self.bot.get_me()
            logger.info(f"Bot conectado: @{bot_info.username}")

            # Testa enviando mensagem de teste
            if self.control_chat_id:
                await self.bot.send_message(
                    chat_id=self.control_chat_id,
                    text="🤖 Bot de trading Solana conectado e funcionando!",
                )

            return True

        except Exception as e:
            logger.error(f"Erro no teste de conexão: {e}")
            return False


# Função de teste
async def test_telegram_bot():
    """Função para testar o bot do Telegram"""
    bot = TelegramBot()

    # Testa conexão
    if await bot.test_connection():
        print("✅ Conexão com Telegram OK")

        # Testa notificação
        test_token_data = {
            "token_address": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
            "channel_name": "Test Channel",
            "detected_at": datetime.now().isoformat(),
            "source": "telegram",
        }

        await bot.send_token_notification(test_token_data)
        print("✅ Notificação enviada")
    else:
        print("❌ Falha na conexão")


if __name__ == "__main__":
    asyncio.run(test_telegram_bot())

