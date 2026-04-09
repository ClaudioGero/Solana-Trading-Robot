export type ParsedBotCommand =
  | { action: "on"; autoOffMinutes?: number }
  | { action: "off" }
  | { action: "status" };

export type BotCommandParseResult =
  | { ok: true; command: ParsedBotCommand }
  | { ok: false; error: string };

const BOT_COMMAND_REGEX = /^\/bot(?:@([a-z0-9_]+))?(?:\s+(.*))?$/i;

export function parseBotCommand(text: string, botUsername?: string): BotCommandParseResult | null {
  const trimmed = text.trim();
  const match = BOT_COMMAND_REGEX.exec(trimmed);
  if (!match) {
    return null;
  }

  const mentionedUsername = match[1]?.toLowerCase();
  if (mentionedUsername && botUsername && mentionedUsername !== botUsername.toLowerCase()) {
    return null;
  }

  const args = (match[2] ?? "")
    .trim()
    .split(/\s+/)
    .filter(Boolean);

  if (args.length === 0) {
    return usage("Missing action.");
  }

  const action = args[0]?.toLowerCase();
  if (action === "status") {
    if (args.length !== 1) {
      return usage("`/bot status` does not accept extra arguments.");
    }

    return { ok: true, command: { action: "status" } };
  }

  if (action === "off") {
    if (args.length !== 1) {
      return usage("`/bot off` does not accept extra arguments.");
    }

    return { ok: true, command: { action: "off" } };
  }

  if (action === "on") {
    if (args.length === 1) {
      return { ok: true, command: { action: "on" } };
    }

    if (args.length === 2) {
      const minutes = parseMinutes(args[1]);
      if (minutes === null) {
        return usage("Invalid duration. Use minutes like `45` or `30m`.");
      }

      return { ok: true, command: { action: "on", autoOffMinutes: minutes } };
    }

    return usage("Too many arguments for `/bot on`.");
  }

  return usage(`Unknown action: ${args[0]}.`);
}

function parseMinutes(raw: string): number | null {
  const normalized = raw.trim().toLowerCase();
  const bareMinutes = /^(\d+)$/.exec(normalized);
  const suffixedMinutes = /^(\d+)m(?:in(?:ute)?s?)?$/.exec(normalized);
  const match = bareMinutes ?? suffixedMinutes;

  if (!match) {
    return null;
  }

  const minutes = Number(match[1]);
  if (!Number.isInteger(minutes) || minutes <= 0) {
    return null;
  }

  return minutes;
}

function usage(reason: string): BotCommandParseResult {
  return {
    ok: false,
    error: `${reason}\nUsage: /bot on | /bot on 30m | /bot off | /bot status`,
  };
}
