#!/usr/bin/env python3
"""
Hermes Gateway Watchdog — detect silent Feishu WebSocket deaths and auto-recover.

Detection methodology:
  1. Pull the latest user message via Feishu REST API
  2. Check if that message ID appears in the gateway log
  3. Message exists but NOT in log → WebSocket is dead → restart gateway

Supports:
  - Auto-discovery of all running Hermes gateway systemd services
  - Single-profile mode via --profile
  - Cross-machine portability via environment variables and CLI args
  - Dry-run mode (--no-restart) for testing
  - Configurable restart commands (works with OpenClaw, etc.)
  - Proxy bypass for mihomo TUN environments

Environment variables:
  HERMES_HOME              Hermes root (default: ~/.hermes)
  WATCHDOG_LOG_DIR         Watchdog log dir (default: $HERMES_HOME/watchdog)
  WATCHDOG_STATE_DIR       State file dir (default: same as WATCHDOG_LOG_DIR)

Author: Cheng Benchao
License: MIT
"""

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError


# ═══════════════════════════════════════════════════════════════
#  Proxy bypass — Feishu API must not traverse mihomo TUN
# ═══════════════════════════════════════════════════════════════

for _key in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY",
              "all_proxy", "ALL_PROXY", "no_proxy", "NO_PROXY"):
    os.environ.pop(_key, None)


# ═══════════════════════════════════════════════════════════════
#  Defaults (override via env vars or CLI args)
# ═══════════════════════════════════════════════════════════════

HERMES_HOME = Path(os.environ.get("HERMES_HOME", Path.home() / ".hermes"))
PROFILES_ROOT = HERMES_HOME / "profiles"
LOG_DIR = Path(os.environ.get("WATCHDOG_LOG_DIR", HERMES_HOME / "watchdog"))
STATE_DIR = Path(os.environ.get("WATCHDOG_STATE_DIR", LOG_DIR))

SERVICE_PATTERN = re.compile(r"hermes-gateway-(\w+)\.service")
RESTART_CMD = ["hermes", "--profile", "{profile}", "gateway", "run", "--replace"]
GATEWAY_LOG_TEMPLATE = "{profiles_root}/{profile}/logs/gateway.log"

MISSED_MSG_THRESHOLD_MINUTES = 5
MAX_MSG_AGE_MINUTES = 120
MIN_RESTART_INTERVAL_MINUTES = 15


# ═══════════════════════════════════════════════════════════════
#  Logging (file only — no console noise in cron mode)
# ═══════════════════════════════════════════════════════════════

LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "watchdog.log"

_fmt = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_file = RotatingFileHandler(
    LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
)
_file.setFormatter(_fmt)
_file.setLevel(logging.DEBUG)

_console = logging.StreamHandler()
_console.setFormatter(_fmt)
_console.setLevel(logging.WARNING)  # only warnings+errors to stdout

log = logging.getLogger("hermes-watchdog")
log.setLevel(logging.DEBUG)
log.addHandler(_file)
log.addHandler(_console)
log.propagate = False


# ═══════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════

def run(*args, timeout=10):
    """Run a command, return (stdout, stderr, returncode)."""
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip(), r.stderr.strip(), r.returncode
    except subprocess.TimeoutExpired:
        return "", "timeout", -1
    except Exception as e:
        return "", str(e), -1


def discover_gateway_services():
    """Find all running gateway systemd user services matching SERVICE_PATTERN."""
    stdout, _, _ = run(
        "systemctl", "--user", "list-units",
        "--type=service", "--state=running",
        "--no-legend", "--no-pager",
    )
    profiles = []
    for line in stdout.split("\n"):
        name = line.strip().split()[0] if line.strip() else ""
        m = SERVICE_PATTERN.match(name)
        if m:
            profile = m.group(1)
            if (PROFILES_ROOT / profile).is_dir():
                profiles.append(profile)
    return profiles


def read_creds(profile: str) -> dict:
    """Read Feishu credentials from profile .env. Returns {app_id, app_secret, chat_id}."""
    env_path = PROFILES_ROOT / profile / ".env"
    creds = {}
    if env_path.exists():
        for line in env_path.read_text().split("\n"):
            if line.startswith("FEISHU_APP_ID="):
                creds["app_id"] = line.split("=", 1)[1].strip().strip('"').strip("'")
            elif line.startswith("FEISHU_APP_SECRET="):
                creds["app_secret"] = line.split("=", 1)[1].strip().strip('"').strip("'")
            elif line.startswith("FEISHU_CHAT_ID="):
                creds["chat_id"] = line.split("=", 1)[1].strip().strip('"').strip("'")
    return creds


def extract_chat_id_from_log(profile: str) -> str | None:
    """Scan gateway log for a chat_id as last-resort fallback."""
    log_path = Path(GATEWAY_LOG_TEMPLATE.format(
        profiles_root=PROFILES_ROOT, profile=profile
    ))
    if not log_path.exists():
        return None
    try:
        stdout, _, _ = run(
            "grep", "-oP", r"chat_id[=:]\s*\Koc_[a-zA-Z0-9]+",
            str(log_path), timeout=5,
        )
        if stdout:
            return stdout.strip().split("\n")[-1]
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════════
#  State tracking
# ═══════════════════════════════════════════════════════════════

def state_path(profile: str) -> Path:
    return STATE_DIR / f"state-{profile}.json"


def load_state(profile: str) -> dict:
    path = state_path(profile)
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return {"last_ok": 0, "failures": 0, "last_restart": 0, "chat_id": None}


def save_state(profile: str, s: dict):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    state_path(profile).write_text(json.dumps(s, indent=2))


# ═══════════════════════════════════════════════════════════════
#  Feishu API
# ═══════════════════════════════════════════════════════════════

def feishu_post(url: str, payload: dict, token: str = None) -> dict | None:
    """POST to Feishu API, return parsed JSON or None."""
    try:
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        data = json.dumps(payload).encode()
        req = Request(url, data=data, headers=headers)
        with urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except URLError as e:
        log.warning(f"Feishu API unreachable: {e}")
        return None
    except Exception as e:
        log.warning(f"Feishu API error: {e}")
        return None


def feishu_get(url: str, token: str) -> dict | None:
    """GET from Feishu API, return parsed JSON or None."""
    try:
        headers = {"Authorization": f"Bearer {token}"}
        req = Request(url, headers=headers)
        with urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except URLError as e:
        log.warning(f"Feishu API unreachable: {e}")
        return None
    except Exception as e:
        log.warning(f"Feishu API error: {e}")
        return None


def get_tenant_token(app_id: str, app_secret: str) -> str | None:
    """Obtain tenant_access_token."""
    data = feishu_post(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        {"app_id": app_id, "app_secret": app_secret},
    )
    return data.get("tenant_access_token") if data else None


def discover_chat_id(token: str) -> str | None:
    """Find the DM chat between this bot and its user via IM API."""
    data = feishu_get(
        "https://open.feishu.cn/open-apis/im/v1/chats"
        "?page_size=20&sort_type=ByCreateTimeDesc",
        token,
    )
    if not data or data.get("code") != 0:
        log.warning(f"List chats failed: {data.get('msg') if data else 'no response'}")
        return None

    for item in data.get("data", {}).get("items", []):
        chat_id = item.get("chat_id")
        chat_type = item.get("chat_mode", "")
        if chat_id and chat_type == "p2p":
            return chat_id

    items = data.get("data", {}).get("items", [])
    if items and items[0].get("chat_id"):
        return items[0]["chat_id"]

    return None


def get_latest_user_message(token: str, chat_id: str) -> dict | None:
    """Get the most recent message from a real user (not bot)."""
    data = feishu_get(
        f"https://open.feishu.cn/open-apis/im/v1/messages"
        f"?container_id_type=chat&container_id={chat_id}"
        f"&page_size=20&sort_type=ByCreateTimeDesc",
        token,
    )
    if not data or data.get("code") != 0:
        log.warning(f"List messages failed: {data.get('msg') if data else 'no response'}")
        return None

    for item in data.get("data", {}).get("items", []):
        sender = item.get("sender", {})
        if sender.get("id_type") != "app_id":
            return {
                "id": item["message_id"],
                "create_time": int(item.get("create_time", 0)),
            }
    return {}  # no user messages — signal idle


# ═══════════════════════════════════════════════════════════════
#  Gateway checks
# ═══════════════════════════════════════════════════════════════

def get_gateway_status(profile: str) -> tuple:
    """Return (running, pid) for hermes-gateway-{profile}.service."""
    stdout, _, _ = run(
        "systemctl", "--user", "show",
        f"hermes-gateway-{profile}.service",
        "-p", "ActiveState", "-p", "MainPID", "--value",
    )
    if not stdout:
        return False, None
    lines = stdout.split("\n")
    active = any(line == "active" for line in lines)
    pid = None
    for l in lines:
        if l.isdigit() and l != "0":
            pid = int(l)
            break
    return active, pid


def is_message_in_log(profile: str, message_id: str) -> bool:
    """Check if a Feishu message_id appears in the gateway log."""
    log_path = Path(GATEWAY_LOG_TEMPLATE.format(
        profiles_root=PROFILES_ROOT, profile=profile
    ))
    if not log_path.exists():
        return False
    try:
        _, _, rc = run("grep", "-qF", message_id, str(log_path), timeout=5)
        return rc == 0
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════
#  Restart
# ═══════════════════════════════════════════════════════════════

def restart_gateway(profile: str, state: dict, no_restart: bool = False) -> bool:
    """Restart gateway for a profile. Respects cooldown."""
    now = time.time()
    if state.get("last_restart"):
        elapsed = (now - state["last_restart"]) / 60
        if elapsed < MIN_RESTART_INTERVAL_MINUTES:
            log.info(
                f"{profile}: skipping restart — {elapsed:.0f} min since last "
                f"(cooldown={MIN_RESTART_INTERVAL_MINUTES}m)"
            )
            return False

    if no_restart:
        log.warning(f"{profile}: WOULD restart (--no-restart enabled)")
        return False

    cmd = [arg.replace("{profile}", profile) for arg in RESTART_CMD]
    log.warning(f"{profile}: restarting gateway...")
    stdout, stderr, rc = run(*cmd, timeout=60)
    if rc == 0:
        log.info(f"{profile}: gateway restarted successfully")
        state["last_restart"] = now
        return True
    else:
        log.error(
            f"{profile}: restart failed (exit {rc}): "
            f"{stderr[-300:] if stderr else 'no stderr'}"
        )
        return False


# ═══════════════════════════════════════════════════════════════
#  Per-profile check
# ═══════════════════════════════════════════════════════════════

def check_profile(profile: str, no_restart: bool = False) -> int:
    """Check one profile. Returns: 0=OK, 1=restarted, 2=error."""
    state = load_state(profile)
    now = time.time()

    # 1. Gateway must be running
    running, pid = get_gateway_status(profile)
    if not running:
        log.debug(f"{profile}: gateway not running — skip")
        return 0

    # 2. Credentials
    creds = read_creds(profile)
    if not creds.get("app_id") or not creds.get("app_secret"):
        log.error(f"{profile}: missing FEISHU_APP_ID/SECRET in .env")
        return 2

    # 3. Tenant token
    token = get_tenant_token(creds["app_id"], creds["app_secret"])
    if not token:
        log.error(f"{profile}: cannot get tenant token")
        return 2

    # 4. Resolve chat_id: .env → state cache → API discover → log scan
    chat_id = creds.get("chat_id") or state.get("chat_id")
    if not chat_id:
        chat_id = discover_chat_id(token)
        if not chat_id:
            chat_id = extract_chat_id_from_log(profile)
    if not chat_id:
        log.error(f"{profile}: cannot determine chat_id (set FEISHU_CHAT_ID in .env)")
        return 2
    if chat_id != state.get("chat_id"):
        state["chat_id"] = chat_id
        save_state(profile, state)
        log.info(f"{profile}: chat_id={chat_id[:12]}...")

    # 5. Get latest user message
    msg_resp = get_latest_user_message(token, chat_id)
    if msg_resp is None:
        log.error(f"{profile}: Feishu API error fetching messages")
        return 2

    if msg_resp == {}:
        state["last_ok"] = now
        state["failures"] = 0
        save_state(profile, state)
        log.debug(f"{profile}: idle — no user messages")
        return 0

    msg = msg_resp
    msg_age_minutes = (now * 1000 - msg["create_time"]) / 60000

    # 5b. If latest user message is too old, treat as idle
    if msg_age_minutes > MAX_MSG_AGE_MINUTES:
        state["last_ok"] = now
        state["failures"] = 0
        save_state(profile, state)
        log.debug(f"{profile}: idle — latest msg {msg_age_minutes:.0f} min old")
        return 0

    # 6. Check if message reached gateway log
    if is_message_in_log(profile, msg["id"]):
        state["last_ok"] = now
        state["failures"] = 0
        save_state(profile, state)
        log.info(f"{profile}: healthy — msg in log ({msg_age_minutes:.0f} min ago)")
        return 0

    # 7. Message NOT in log — check age
    if msg_age_minutes < MISSED_MSG_THRESHOLD_MINUTES:
        log.info(
            f"{profile}: msg not yet in log "
            f"({msg_age_minutes:.1f} min < {MISSED_MSG_THRESHOLD_MINUTES}m), waiting"
        )
        return 0

    # 7b. Safety net: old messages may be purged by log rotation
    if msg_age_minutes > MAX_MSG_AGE_MINUTES:
        state["last_ok"] = now
        state["failures"] = 0
        save_state(profile, state)
        log.info(f"{profile}: idle — msg {msg_age_minutes:.0f} min old, "
                 f"likely purged by log rotation")
        return 0

    # 8. Dead websocket
    log.warning(
        f"{profile}: DEAD — msg {msg['id'][:12]}... "
        f"({msg_age_minutes:.0f} min ago) never reached gateway!"
    )
    ok = restart_gateway(profile, state, no_restart=no_restart)
    state["failures"] = 0 if ok else state.get("failures", 0) + 1
    save_state(profile, state)
    return 1 if ok else 2


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(
        description="Hermes Gateway Watchdog — detect silent Feishu WebSocket deaths",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          auto-discover all gateway profiles
  %(prog)s --profile english        monitor a single profile
  %(prog)s --no-restart             dry-run: detect only, do not restart
  %(prog)s --profiles-root /opt/hermes/profiles
  %(prog)s --service-pattern 'openclaw-gateway-(\\w+)'
        """,
    )
    p.add_argument("--profile", help="Monitor a single profile (default: auto-discover all)")
    p.add_argument("--profiles-root", default=str(PROFILES_ROOT),
                   help=f"Profiles directory (default: {PROFILES_ROOT})")
    p.add_argument("--service-pattern", default=SERVICE_PATTERN.pattern,
                   help=f"systemd unit regex (default: {SERVICE_PATTERN.pattern})")
    p.add_argument("--restart-cmd", default=" ".join(RESTART_CMD),
                   help="Restart command with {profile} placeholder")
    p.add_argument("--gateway-log", default=GATEWAY_LOG_TEMPLATE,
                   help="Gateway log path with {profiles_root} and {profile}")
    p.add_argument("--no-restart", action="store_true",
                   help="Detect only, do not restart")
    p.add_argument("--interval", type=int, default=MIN_RESTART_INTERVAL_MINUTES,
                   help=f"Min minutes between restarts (default: {MIN_RESTART_INTERVAL_MINUTES})")
    p.add_argument("--missed-threshold", type=int, default=MISSED_MSG_THRESHOLD_MINUTES,
                   help=f"Minutes before declaring dead (default: {MISSED_MSG_THRESHOLD_MINUTES})")
    p.add_argument("--max-age", type=int, default=MAX_MSG_AGE_MINUTES,
                   help=f"Max message age before idle (default: {MAX_MSG_AGE_MINUTES})")
    p.add_argument("--log-dir", default=str(LOG_DIR),
                   help=f"Watchdog log directory (default: {LOG_DIR})")
    p.add_argument("--state-dir", default=str(STATE_DIR),
                   help=f"State file directory (default: {STATE_DIR})")
    return p.parse_args()


def main():
    global PROFILES_ROOT, LOG_DIR, STATE_DIR
    global SERVICE_PATTERN, RESTART_CMD, GATEWAY_LOG_TEMPLATE
    global MISSED_MSG_THRESHOLD_MINUTES, MAX_MSG_AGE_MINUTES, MIN_RESTART_INTERVAL_MINUTES

    args = parse_args()

    # Apply overrides
    PROFILES_ROOT = Path(args.profiles_root)
    LOG_DIR = Path(args.log_dir)
    STATE_DIR = Path(args.state_dir)
    SERVICE_PATTERN = re.compile(args.service_pattern)
    RESTART_CMD = args.restart_cmd.split()
    GATEWAY_LOG_TEMPLATE = args.gateway_log
    MIN_RESTART_INTERVAL_MINUTES = args.interval
    MISSED_MSG_THRESHOLD_MINUTES = args.missed_threshold
    MAX_MSG_AGE_MINUTES = args.max_age

    # Re-init logging with new LOG_DIR
    global log, _file, _console
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log.removeHandler(_file)
    log.removeHandler(_console)
    _file = RotatingFileHandler(
        LOG_DIR / "watchdog.log", maxBytes=5 * 1024 * 1024, backupCount=5,
        encoding="utf-8",
    )
    _file.setFormatter(_fmt)
    _file.setLevel(logging.DEBUG)
    log.addHandler(_file)
    log.addHandler(_console)

    log.info("── Watchdog starting ──")

    if args.profile:
        profiles = [args.profile]
    else:
        profiles = discover_gateway_services()

    if not profiles:
        log.info("No gateway services found — nothing to monitor")
        return 0

    log.info(f"Monitoring {len(profiles)} gateway(s): {', '.join(profiles)}")

    results = {}
    for profile in profiles:
        try:
            results[profile] = check_profile(profile, no_restart=args.no_restart)
        except Exception as e:
            log.error(f"{profile}: unhandled exception: {e}")
            results[profile] = 2

    ok = sum(1 for v in results.values() if v == 0)
    restarted = sum(1 for v in results.values() if v == 1)
    errors = sum(1 for v in results.values() if v == 2)
    log.info(f"Scan complete: {ok} OK, {restarted} restarted, {errors} errors")

    if errors:
        return 2
    if restarted:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
