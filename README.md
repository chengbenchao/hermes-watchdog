# Hermes Watchdog

Detect silent Feishu WebSocket deaths in Hermes Agent gateways and auto-recover.

## The Problem

`hermes gateway status` shows `active (running)`, but your bot stopped responding hours ago. This is a **TCP half-open connection** — the Feishu server disconnected, but the OS never told the gateway process. The gateway looks alive but can't receive messages.

## How It Works

Instead of checking if the process is alive (pointless — it usually is), the watchdog compares two data sources:

1. **Feishu REST API** — pulls the latest user message sent to the bot
2. **Gateway log** — checks if that message ID ever reached the gateway

Message exists on Feishu but NOT in the log → WebSocket is dead → restart gateway.

## Quick Start

```bash
# Auto-discover all hermes-gateway-* services
python3 hermes-watchdog.py

# Monitor a single profile
python3 hermes-watchdog.py --profile english

# Dry-run: detect only, no restart
python3 hermes-watchdog.py --no-restart

# Schedule via cron (every 10 minutes)
*/10 * * * * /usr/bin/python3 /path/to/hermes-watchdog.py
```

## Cross-Machine Portability

All paths are configurable. The script auto-detects from environment variables with sensible defaults:

| Env Var | Default | Purpose |
|---------|---------|---------|
| `HERMES_HOME` | `~/.hermes` | Hermes root directory |
| `WATCHDOG_LOG_DIR` | `$HERMES_HOME/watchdog` | Watchdog log output |
| `WATCHDOG_STATE_DIR` | same as log dir | Per-profile state files |

Run on another machine — just set `HERMES_HOME`:

```bash
HERMES_HOME=/opt/hermes python3 hermes-watchdog.py --profile default
```

## Using with OpenClaw or Other Frameworks

The watchdog is not Hermes-specific. Override the service pattern, restart command, and log path:

```bash
# OpenClaw example
python3 hermes-watchdog.py \
  --profiles-root /root/.openclaw/profiles \
  --service-pattern 'openclaw-gateway-(\w+)' \
  --restart-cmd 'openclaw gateway restart --profile {profile}' \
  --gateway-log '{profiles_root}/{profile}/logs/gateway.log'
```

⚠️ **Caveat for non-Hermes gateways**: the detection relies on Feishu message IDs (`om_*`) appearing in the gateway log. If your gateway doesn't log message IDs, you'll need a different detection strategy (e.g., timestamp comparison).

## CLI Reference

```
usage: hermes-watchdog.py [options]

  --profile NAME         Monitor a single profile (default: auto-discover all)
  --profiles-root PATH   Profiles directory (default: ~/.hermes/profiles)
  --service-pattern RE   systemd unit regex (default: hermes-gateway-(\w+))
  --restart-cmd CMD      Restart command with {profile} placeholder
  --gateway-log PATH     Gateway log with {profiles_root} and {profile}
  --no-restart           Detect only, do not restart
  --interval N           Min minutes between restarts (default: 15)
  --missed-threshold N   Minutes before declaring dead (default: 5)
  --max-age N            Max message age before idle (default: 120)
  --log-dir PATH         Watchdog log directory
  --state-dir PATH       State file directory
```

## Exit Codes

| Code | Meaning | Cron Action |
|------|---------|-------------|
| 0 | All healthy or idle | Silent |
| 1 | Dead WebSocket detected, gateway restarted | Notify |
| 2 | Watchdog itself failed (creds, API, network) | **Alert** |

Designed for `no_agent=true` cron mode — zero token consumption per check.

## How It Works (Detail)

```
1. scan systemd --user for hermes-gateway-*.service (or --profile)
2. for each running gateway:
   a. read FEISHU_APP_ID/SECRET from profile .env
   b. get tenant_access_token
   c. resolve chat_id (4-tier fallback):
      .env → state cache → IM API list chats → log grep
   d. pull latest user message via Feishu IM API
   e. grep gateway log for message_id
   f. message exists but NOT in log > 5 min → DEAD → restart
   g. message > 120 min old → idle (log may have rotated)
3. cooldown: at most one restart per 15 min per profile
4. exit with worst-case code across all profiles
```

## Requirements

- Python 3.10+
- Linux with systemd user services
- Feishu bot with `im:message:readonly` permission
- `FEISHU_APP_ID` and `FEISHU_APP_SECRET` in each profile's `.env`

## License

MIT
