import { z } from "zod";

const EnvSchema = z.object({
  CONTROL_TELEGRAM_BOT_TOKEN: z.string().min(1, "CONTROL_TELEGRAM_BOT_TOKEN is required"),
  CONTROL_ADMIN_USER_ID: z.coerce.bigint(),
  REDIS_URL: z.string().min(1, "REDIS_URL is required"),
  CONTROL_CHAT_ID: z.string().optional().transform((value) => value?.trim() || undefined),
  LOG_LEVEL: z.string().optional(),
});

export type ControlBotConfig = {
  telegramBotToken: string;
  adminUserId: bigint;
  redisUrl: string;
  controlChatId?: string;
};

export function loadControlBotConfig(): ControlBotConfig {
  const env = EnvSchema.parse(process.env);

  return {
    telegramBotToken: env.CONTROL_TELEGRAM_BOT_TOKEN,
    adminUserId: env.CONTROL_ADMIN_USER_ID,
    redisUrl: env.REDIS_URL,
    controlChatId: env.CONTROL_CHAT_ID,
  };
}
