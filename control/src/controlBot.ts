import dotenv from "dotenv";
import { Bot } from "grammy";
import { parseBotCommand } from "./botCommands.js";
import { loadControlBotConfig } from "./controlBotConfig.js";
import { logger } from "./logger.js";
import { RedisControl, type ControlStatus } from "./redisControl.js";

dotenv.config();

const MAX_TIMEOUT_MS = 2_147_483_647;

const config = loadControlBotConfig();
const redisControl = new RedisControl(config.redisUrl);
const bot = new Bot(config.telegramBotToken);

let botUsername: string | undefined;
let autoOffTimer: NodeJS.Timeout | null = null;

async function main(): Promise<void> {
  await redisControl.connect();

  const me = await bot.api.getMe();
  botUsername = me.username;

  const startupStatus = await redisControl.getStatus();
  await scheduleAutoOffFromStatus(startupStatus);

  logger.info(
    {
      botUsername,
      adminUserId: config.adminUserId.toString(),
      controlChatId: config.controlChatId,
      redisUrl: config.redisUrl,
    },
    "telegram control bot started",
  );

  await bot.start({
    allowed_updates: ["message"],
  });
}

bot.on("message:text", async (ctx) => {
  const parsed = parseBotCommand(ctx.message.text, botUsername);
  if (!parsed) {
    return;
  }

  const userId = BigInt(ctx.from.id);
  const chatId = String(ctx.chat.id);

  logger.info(
    {
      commandText: ctx.message.text,
      userId: userId.toString(),
      username: ctx.from.username,
      chatId,
      chatType: ctx.chat.type,
    },
    "control command received",
  );

  if (userId !== config.adminUserId) {
    logger.warn(
      {
        commandText: ctx.message.text,
        userId: userId.toString(),
        chatId,
      },
      "unauthorized control command rejected",
    );
    await ctx.reply("Unauthorized.");
    return;
  }

  if (!parsed.ok) {
    await ctx.reply(parsed.error);
    return;
  }

  if (parsed.command.action === "status") {
    const status = await redisControl.getStatus();
    await ctx.reply(formatStatusMessage(status));
    logger.info({ action: "status", userId: userId.toString(), chatId }, "status returned");
    return;
  }

  if (parsed.command.action === "off") {
    clearAutoOffTimer();
    const status = await redisControl.setBotOff();
    const reply = `Bot disabled.\n\n${formatStatusMessage(status)}`;
    await ctx.reply(reply);
    await broadcastIfConfigured(chatId, `Control update from @${ctx.from.username ?? ctx.from.id}: bot OFF.`);
    logger.info({ action: "off", userId: userId.toString(), chatId }, "bot disabled");
    return;
  }

  const autoOffAt = parsed.command.autoOffMinutes
    ? new Date(Date.now() + parsed.command.autoOffMinutes * 60_000).toISOString()
    : undefined;
  const status = await redisControl.setBotOn(autoOffAt);
  await scheduleAutoOffFromStatus(status);

  const autoOffText = parsed.command.autoOffMinutes
    ? `\nAuto-off scheduled for ${status.autoOffAt}.`
    : "\nAuto-off cleared.";
  await ctx.reply(`Bot enabled.${autoOffText}\n\n${formatStatusMessage(status)}`);
  await broadcastIfConfigured(
    chatId,
    parsed.command.autoOffMinutes
      ? `Control update from @${ctx.from.username ?? ctx.from.id}: bot ON for ${parsed.command.autoOffMinutes} minutes.`
      : `Control update from @${ctx.from.username ?? ctx.from.id}: bot ON.`,
  );
  logger.info(
    {
      action: "on",
      userId: userId.toString(),
      chatId,
      autoOffAt,
    },
    "bot enabled",
  );
});

async function scheduleAutoOffFromStatus(status: ControlStatus): Promise<void> {
  clearAutoOffTimer();

  if (!status.autoOffAt) {
    return;
  }

  const targetMs = Date.parse(status.autoOffAt);
  if (Number.isNaN(targetMs)) {
    logger.warn({ autoOffAt: status.autoOffAt }, "invalid auto-off timestamp found in redis");
    return;
  }

  if (targetMs <= Date.now()) {
    logger.info({ autoOffAt: status.autoOffAt }, "auto-off deadline already passed, disabling bot now");
    await executeAutoOff(status.autoOffAt);
    return;
  }

  scheduleLongTimeout(targetMs, async () => {
    await executeAutoOff(status.autoOffAt!);
  });

  logger.info({ autoOffAt: status.autoOffAt }, "auto-off scheduled");
}

async function executeAutoOff(autoOffAt: string): Promise<void> {
  clearAutoOffTimer();
  const currentStatus = await redisControl.getStatus();
  if (currentStatus.autoOffAt !== autoOffAt) {
    logger.info(
      {
        expectedAutoOffAt: autoOffAt,
        currentAutoOffAt: currentStatus.autoOffAt,
      },
      "auto-off skipped because the schedule changed",
    );
    return;
  }

  const status = await redisControl.setBotOff();
  logger.warn({ autoOffAt }, "auto-off executed");

  if (config.controlChatId) {
    await bot.api.sendMessage(
      config.controlChatId,
      `Auto-off executed at ${new Date().toISOString()}.\n\n${formatStatusMessage(status)}`,
    );
  }
}

function scheduleLongTimeout(targetMs: number, callback: () => Promise<void>): void {
  const remainingMs = targetMs - Date.now();
  const delay = Math.min(remainingMs, MAX_TIMEOUT_MS);

  autoOffTimer = setTimeout(() => {
    if (remainingMs > MAX_TIMEOUT_MS) {
      scheduleLongTimeout(targetMs, callback);
      return;
    }

    void callback().catch((error: unknown) => {
      logger.error({ err: error }, "auto-off execution failed");
    });
  }, delay);
}

function clearAutoOffTimer(): void {
  if (autoOffTimer) {
    clearTimeout(autoOffTimer);
    autoOffTimer = null;
  }
}

function formatStatusMessage(status: ControlStatus): string {
  const botStatus = status.emergencyStop ? "OFF" : status.pauseBuys ? "PARTIAL" : "ON";

  return [
    `bot_status: ${botStatus}`,
    `pause_buys: ${status.pauseBuys ? "ON" : "OFF"}`,
    `emergency_stop: ${status.emergencyStop ? "ON" : "OFF"}`,
    `auto_off_at: ${status.autoOffAt ?? "not set"}`,
    `timestamp: ${status.checkedAt}`,
  ].join("\n");
}

async function broadcastIfConfigured(sourceChatId: string, message: string): Promise<void> {
  if (!config.controlChatId || config.controlChatId === sourceChatId) {
    return;
  }

  await bot.api.sendMessage(config.controlChatId, message);
}

async function shutdown(signal: string): Promise<void> {
  logger.info({ signal }, "shutting down telegram control bot");
  clearAutoOffTimer();
  bot.stop();
  await redisControl.disconnect();
}

process.once("SIGINT", () => {
  void shutdown("SIGINT");
});

process.once("SIGTERM", () => {
  void shutdown("SIGTERM");
});

bot.catch((error) => {
  logger.error({ err: error.error, updateId: error.ctx.update.update_id }, "telegram bot update failed");
});

void main().catch(async (error: unknown) => {
  logger.error({ err: error }, "telegram control bot failed");
  clearAutoOffTimer();
  await redisControl.disconnect().catch(() => undefined);
  process.exitCode = 1;
});
