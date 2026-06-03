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
  - API retry with exponential backoff
  - False-positive prevention: restarted gateways won't re-trigger on old messages

Environment variables:
  HERMES_HOME              Hermes root (default: ~/.hermes)
  WATCHDOG_LOG_DIR         Watchdog log dir (default: $HERMES_HOME/watchdog)
  WATCHDOG_STATE_DIR       State file dir (default: same as WATCHDOG_LOG_DIR)

Author: Cheng Benchao
License: MIT
"""

# ═══════════════════════════════════════════════════════════════
#  Proxy bypass — Feishu API must not traverse mihomo TUN
# ═══════════════════════════════════════════════════════════════

import os
for _key in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY",
              "all_proxy", "ALL_PROXY", "no_proxy", "NO_PROXY"):
    os.environ.pop(_key, None)


import argparse
import json
import logging
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import TypedDict, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError


# ═══════════════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════════════

# Detection thresholds
MISSED_MSG_THRESHOLD_MINUTES = 5    # msg older than this without reaching log → suspect dead
MAX_MSG_AGE_MINUTES = 120           # msg older than this → treat as idle (likely log-rotated)
MIN_RESTART_INTERVAL_MINUTES = 15   # cooldown between restarts of the same gateway

# Network / API
API_TIMEOUT_SECONDS = 10
API_MAX_RETRIES = 2
API_RETRY_BACKOFF = 2.0             # seconds between retries (doubles each retry)
MESSAGE_PAGE_SIZE = 20

# Log rotation
LOG_MAX_BYTES = 5 * 1024 * 1024     # 5 MB
LOG_BACKUP_COUNT = 5


# ═══════════════════════════════════════════════════════════════
#  Types
# ═══════════════════════════════════════════════════════════════

class GatewayState(TypedDict, total=False):
    """Persistent per-profile state stored as JSON."""
    last_ok: float          # unix timestamp of last confirmed-healthy scan
    failures: int           # consecutive failure count
    last_restart: float     # unix timestamp of last restart
    chat_id: str | None     # cached Feishu chat_id


@dataclass
class WatchdogConfig:
    """Immutable configuration for a watchdog run."""
    profiles_root: Path
    log_dir: Path
    state_dir: Path
    service_pattern: re.Pattern
    restart_cmd: list[str]             # token list with {profile} placeholder
    gateway_log_template: str          # with {profiles_root} and {profile}
    missed_msg_threshold: int = MISSED_MSG_THRESHOLD_MINUTES
    max_msg_age: int = MAX_MSG_AGE_MINUTES
    min_restart_interval: int = MIN_RESTART_INTERVAL_MINUTES

    @classmethod
    def defaults(cls) -> "WatchdogConfig":
        home = Path(os.environ.get("HERMES_HOME", Path.home() / ".hermes"))
        logd = Path(os.environ.get("WATCHDOG_LOG_DIR", home / "watchdog"))
        return cls(
            profiles_root=home / "profiles",
            log_dir=logd,
            state_dir=Path(os.environ.get("WATCHDOG_STATE_DIR", logd)),
            service_pattern=re.compile(r"hermes-gateway-(\w+)\.service"),
            restart_cmd=["systemctl", "--user", "restart", "hermes-gateway-{profile}.service"],
            gateway_log_template="{profiles_root}/{profile}/logs/gateway.log",
        )


# ═══════════════════════════════════════════════════════════════
#  Logging
# ═══════════════════════════════════════════════════════════════

_LOG_FMT = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def _setup_logging(log_dir: Path) -> logging.Logger:
    """Create a logger that writes to a rotating file + warnings to stderr."""
    log_dir.mkdir(parents=True, exist_ok=True)

    file_handler = RotatingFileHandler(
        log_dir / "watchdog.log",
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(_LOG_FMT)
    file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(_LOG_FMT)
    console_handler.setLevel(logging.WARNING)

    logger = logging.getLogger("hermes-watchdog")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False

    return logger


# ═══════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════

def run(*args: str, timeout: int = 10) -> tuple[str, str, int]:
    """Run a command, return (stdout, stderr, returncode)."""
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip(), r.stderr.strip(), r.returncode
    except subprocess.TimeoutExpired:
        return "", f"timeout ({timeout}s)", -1
    except Exception as e:
        return "", str(e), -1


def retry_api(func, *args, max_retries: int = API_MAX_RETRIES, **kwargs):
    """Call func(*args, **kwargs) with exponential backoff on failure."""
    last_err = None
    for attempt in range(max_retries + 1):
        try:
            result = func(*args, **kwargs)
            if result is not None:
                return result
        except Exception as e:
            last_err = e
        if attempt < max_retries:
            delay = API_RETRY_BACKOFF * (2 ** attempt)
            time.sleep(delay)
    return None


# ═══════════════════════════════════════════════════════════════
#  Service discovery
# ═══════════════════════════════════════════════════════════════

def discover_gateway_services(cfg: WatchdogConfig) -> list[str]:
    """Find all running gateway systemd user services matching SERVICE_PATTERN."""
    stdout, stderr, rc = run(
        "systemctl", "--user", "list-units",
        "--type=service", "--state=running",
        "--no-legend", "--no-pager",
    )
    if rc != 0:
        return []

    profiles: list[str] = []
    for line in stdout.split("\n"):
        name = line.strip().split()[0] if line.strip() else ""
        m = cfg.service_pattern.match(name)
        if m:
            profile = m.group(1)
            if (cfg.profiles_root / profile).is_dir():
                profiles.append(profile)
    return profiles


# ═══════════════════════════════════════════════════════════════
#  State persistence
# ═══════════════════════════════════════════════════════════════

def state_path(cfg: WatchdogConfig, profile: str) -> Path:
    return cfg.state_dir / f"state-{profile}.json"


def load_state(cfg: WatchdogConfig, profile: str) -> GatewayState:
    path = state_path(cfg, profile)
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return {"last_ok": 0, "failures": 0, "last_restart": 0, "chat_id": None}


def save_state(cfg: WatchdogConfig, profile: str, s: GatewayState) -> None:
    cfg.state_dir.mkdir(parents=True, exist_ok=True)
    state_path(cfg, profile).write_text(json.dumps(s, indent=2))


# ═══════════════════════════════════════════════════════════════
#  Credentials
# ═══════════════════════════════════════════════════════════════

def read_creds(cfg: WatchdogConfig, profile: str) -> dict[str, str]:
    """Read Feishu credentials from profile .env."""
    env_path = cfg.profiles_root / profile / ".env"
    creds: dict[str, str] = {}
    if not env_path.exists():
        return creds
    for line in env_path.read_text().split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        for prefix in ("FEISHU_APP_ID", "FEISHU_APP_SECRET", "FEISHU_CHAT_ID"):
            key = prefix.lower() if prefix != "FEISHU_APP_ID" else "app_id"
            if prefix == "FEISHU_APP_SECRET":
                key = "app_secret"
            elif prefix == "FEISHU_CHAT_ID":
                key = "chat_id"
            if line.startswith(f"{prefix}="):
                val = line.split("=", 1)[1].strip()
                # Strip matching quotes: "val" or 'val'
                if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
                    val = val[1:-1]
                creds[key] = val
    return creds


# ═══════════════════════════════════════════════════════════════
#  Feishu API
# ═══════════════════════════════════════════════════════════════

def _feishu_request(url: str, *, payload: dict | None = None,
                    token: str | None = None, method: str = "GET",
                    log: logging.Logger) -> dict | None:
    """Low-level Feishu API call with retry."""
    try:
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        data = json.dumps(payload).encode() if payload else None
        req = Request(url, data=data, headers=headers, method=method)
        with urlopen(req, timeout=API_TIMEOUT_SECONDS) as resp:
            return json.loads(resp.read())
    except URLError as e:
        log.warning(f"Feishu API unreachable: {e}")
        return None
    except Exception as e:
        log.warning(f"Feishu API error ({url[:60]}): {e}")
        return None


def _feishu_call(log: logging.Logger, url: str, *,
                 payload: dict | None = None,
                 token: str | None = None,
                 method: str = "GET") -> dict | None:
    """Feishu API call with automatic retry."""
    return retry_api(
        _feishu_request, url, payload=payload, token=token, method=method, log=log,
        max_retries=API_MAX_RETRIES,
    )


def get_tenant_token(app_id: str, app_secret: str,
                     log: logging.Logger) -> str | None:
    """Obtain tenant_access_token."""
    data = _feishu_call(
        log,
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        payload={"app_id": app_id, "app_secret": app_secret},
        method="POST",
    )
    return data.get("tenant_access_token") if data else None


def discover_chat_id(token: str, log: logging.Logger) -> str | None:
    """Find the p2p DM chat between this bot and its user."""
    data = _feishu_call(
        log,
        f"https://open.feishu.cn/open-apis/im/v1/chats"
        f"?page_size={MESSAGE_PAGE_SIZE}&sort_type=ByCreateTimeDesc",
        token=token,
    )
    if not data or data.get("code") != 0:
        log.warning(f"List chats failed: {data.get('msg') if data else 'no response'}")
        return None

    items = data.get("data", {}).get("items", [])
    for item in items:
        if item.get("chat_mode") == "p2p" and item.get("chat_id"):
            return item["chat_id"]

    # Fallback: first available chat
    if items and items[0].get("chat_id"):
        return items[0]["chat_id"]

    return None


def extract_chat_id_from_log(cfg: WatchdogConfig, profile: str) -> str | None:
    """Scan gateway log for a chat_id as last-resort fallback."""
    log_path = Path(cfg.gateway_log_template.format(
        profiles_root=cfg.profiles_root, profile=profile
    ))
    if not log_path.exists():
        return None
    try:
        stdout, _, rc = run(
            "grep", "-oP", r"chat_id[=:]\s*\Koc_[a-zA-Z0-9]+",
            str(log_path), timeout=5,
        )
        if rc == 0 and stdout:
            return stdout.strip().split("\n")[-1]
    except Exception:
        pass
    return None


def get_latest_user_message(token: str, chat_id: str,
                            log: logging.Logger) -> dict | None:
    """Get the most recent message from a real user (not bot).
    Returns: dict with 'id'/'create_time', empty dict if no user messages, None on error.
    """
    data = _feishu_call(
        log,
        f"https://open.feishu.cn/open-apis/im/v1/messages"
        f"?container_id_type=chat&container_id={chat_id}"
        f"&page_size={MESSAGE_PAGE_SIZE}&sort_type=ByCreateTimeDesc",
        token=token,
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
    return {}  # no user messages → signal idle


# ═══════════════════════════════════════════════════════════════
#  Gateway checks
# ═══════════════════════════════════════════════════════════════

def get_gateway_status(profile: str) -> tuple[bool, int | None]:
    """Return (running, pid) for hermes-gateway-{profile}.service."""
    stdout, _, _ = run(
        "systemctl", "--user", "show",
        f"hermes-gateway-{profile}.service",
        "-p", "ActiveState", "-p", "MainPID", "--value",
    )
    if not stdout:
        return False, None
    lines = stdout.split("\n")
    active = "active" in lines
    pid: int | None = None
    for line in lines:
        if line.isdigit() and line != "0":
            pid = int(line)
            break
    return active, pid


def is_message_in_log(cfg: WatchdogConfig, profile: str, message_id: str) -> bool:
    """Check if a Feishu message_id appears in the gateway log."""
    log_path = Path(cfg.gateway_log_template.format(
        profiles_root=cfg.profiles_root, profile=profile
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

def restart_gateway(cfg: WatchdogConfig, profile: str, state: GatewayState,
                    no_restart: bool = False, log: logging.Logger = None) -> bool:
    """Restart gateway for a profile. Respects cooldown. Returns True if restarted."""
    now = time.time()

    # Cooldown check
    if state.get("last_restart"):
        elapsed = (now - state["last_restart"]) / 60
        if elapsed < cfg.min_restart_interval:
            log.info(
                f"{profile}: skipping restart — {elapsed:.0f} min since last "
                f"(cooldown={cfg.min_restart_interval}m)"
            )
            return False

    if no_restart:
        log.warning(f"{profile}: WOULD restart (--no-restart enabled)")
        return False

    cmd = [arg.replace("{profile}", profile) for arg in cfg.restart_cmd]
    log.warning(f"{profile}: restarting gateway via: {' '.join(cmd)}")
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
#  Per-profile check (core logic)
# ═══════════════════════════════════════════════════════════════

def check_profile(cfg: WatchdogConfig, profile: str,
                  no_restart: bool = False, log: logging.Logger = None) -> int:
    """Check one profile. Returns: 0=OK, 1=restarted, 2=error."""
    state = load_state(cfg, profile)
    now = time.time()

    # ── 1. Gateway must be running ──
    running, _ = get_gateway_status(profile)
    if not running:
        log.debug(f"{profile}: gateway not running — skip (systemd will restart)")
        return 0

    # ── 2. Credentials ──
    creds = read_creds(cfg, profile)
    app_id = creds.get("app_id")
    app_secret = creds.get("app_secret")
    if not app_id or not app_secret:
        log.error(f"{profile}: missing FEISHU_APP_ID/SECRET in .env")
        return 2

    # ── 3. Tenant token ──
    token = get_tenant_token(app_id, app_secret, log=log)
    if not token:
        log.error(f"{profile}: cannot get tenant token")
        return 2

    # ── 4. Resolve chat_id: .env → state cache → API discover → log scan ──
    chat_id: str | None = creds.get("chat_id") or state.get("chat_id")
    if not chat_id:
        chat_id = discover_chat_id(token, log=log)
    if not chat_id:
        chat_id = extract_chat_id_from_log(cfg, profile)
    if not chat_id:
        log.error(f"{profile}: cannot determine chat_id (set FEISHU_CHAT_ID in .env)")
        return 2

    if chat_id != state.get("chat_id"):
        state["chat_id"] = chat_id
        save_state(cfg, profile, state)
        log.info(f"{profile}: chat_id={chat_id[:12]}...")

    # ── 5. Get latest user message ──
    msg = get_latest_user_message(token, chat_id, log=log)
    if msg is None:
        log.error(f"{profile}: Feishu API error fetching messages")
        return 2

    if msg == {}:
        _mark_healthy(cfg, profile, state, now)
        log.debug(f"{profile}: idle — no user messages")
        return 0

    msg_age_min = (now * 1000 - msg["create_time"]) / 60000

    # ── 6. Message predates last restart → false positive guard ──
    # After a gateway restart, the log is fresh and old messages won't be
    # found.  Avoid flagging the gateway as DEAD for messages sent before
    # the restart.
    last_restart = state.get("last_restart", 0)
    msg_timestamp_sec = msg["create_time"] / 1000
    if last_restart and msg_timestamp_sec < last_restart:
        _mark_healthy(cfg, profile, state, now)
        mins_since_restart = (now - last_restart) / 60
        log.debug(
            f"{profile}: msg ({msg_age_min:.0f}min old) predates last restart "
            f"({mins_since_restart:.0f}min ago) — waiting for newer message"
        )
        return 0

    # ── 7. Message is too old to be in log (log rotation) ──
    if msg_age_min > cfg.max_msg_age:
        _mark_healthy(cfg, profile, state, now)
        log.debug(f"{profile}: idle — latest msg {msg_age_min:.0f} min old")
        return 0

    # ── 8. Message found in log → healthy ──
    if is_message_in_log(cfg, profile, msg["id"]):
        _mark_healthy(cfg, profile, state, now)
        log.info(f"{profile}: healthy — msg in log ({msg_age_min:.0f} min ago)")
        return 0

    # ── 9. Message not in log, but too recent to panic ──
    if msg_age_min < cfg.missed_msg_threshold:
        log.info(
            f"{profile}: msg not yet in log "
            f"({msg_age_min:.1f} min < {cfg.missed_msg_threshold}m threshold)"
        )
        return 0

    # ── 10. Dead WebSocket detected ──
    log.warning(
        f"{profile}: DEAD — msg {msg['id'][:12]}... "
        f"({msg_age_min:.0f} min ago) never reached gateway!"
    )
    ok = restart_gateway(cfg, profile, state, no_restart=no_restart, log=log)
    if ok:
        state["failures"] = 0
    else:
        state["failures"] = state.get("failures", 0) + 1
    save_state(cfg, profile, state)
    return 1 if ok else 2


def _mark_healthy(cfg: WatchdogConfig, profile: str, state: GatewayState,
                  now: float) -> None:
    """Persist healthy status."""
    state["last_ok"] = now
    state["failures"] = 0
    save_state(cfg, profile, state)


# ═══════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════

def parse_args(defaults: WatchdogConfig) -> argparse.Namespace:
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
    p.add_argument("--profile",
                   help="Monitor a single profile (default: auto-discover all)")
    p.add_argument("--profiles-root", default=str(defaults.profiles_root),
                   help=f"Profiles directory (default: {defaults.profiles_root})")
    p.add_argument("--service-pattern", default=defaults.service_pattern.pattern,
                   help=f"systemd unit regex (default: {defaults.service_pattern.pattern})")
    p.add_argument("--restart-cmd", default=" ".join(defaults.restart_cmd),
                   help="Restart command with {profile} placeholder")
    p.add_argument("--gateway-log", default=defaults.gateway_log_template,
                   help="Gateway log path with {profiles_root} and {profile}")
    p.add_argument("--no-restart", action="store_true",
                   help="Detect only, do not restart")
    p.add_argument("--interval", type=int, default=defaults.min_restart_interval,
                   help=f"Min minutes between restarts (default: {defaults.min_restart_interval})")
    p.add_argument("--missed-threshold", type=int, default=defaults.missed_msg_threshold,
                   help=f"Minutes before declaring dead (default: {defaults.missed_msg_threshold})")
    p.add_argument("--max-age", type=int, default=defaults.max_msg_age,
                   help=f"Max message age before idle (default: {defaults.max_msg_age})")
    p.add_argument("--log-dir", default=str(defaults.log_dir),
                   help=f"Watchdog log directory (default: {defaults.log_dir})")
    p.add_argument("--state-dir", default=str(defaults.state_dir),
                   help=f"State file directory (default: {defaults.state_dir})")
    return p.parse_args()


def _apply_cli_overrides(defaults: WatchdogConfig,
                         args: argparse.Namespace) -> WatchdogConfig:
    """Merge CLI arguments into a new WatchdogConfig."""
    return WatchdogConfig(
        profiles_root=Path(args.profiles_root),
        log_dir=Path(args.log_dir),
        state_dir=Path(args.state_dir),
        service_pattern=re.compile(args.service_pattern),
        restart_cmd=args.restart_cmd.split(),
        gateway_log_template=args.gateway_log,
        missed_msg_threshold=args.missed_threshold,
        max_msg_age=args.max_age,
        min_restart_interval=args.interval,
    )


# ═══════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════

def main() -> int:
    defaults = WatchdogConfig.defaults()
    args = parse_args(defaults)
    cfg = _apply_cli_overrides(defaults, args)
    log = _setup_logging(cfg.log_dir)

    log.info("── Watchdog starting ──")

    if args.profile:
        profiles = [args.profile]
    else:
        profiles = discover_gateway_services(cfg)

    if not profiles:
        log.info("No gateway services found — nothing to monitor")
        return 0

    log.info(f"Monitoring {len(profiles)} gateway(s): {', '.join(profiles)}")

    results: dict[str, int] = {}
    for profile in profiles:
        try:
            results[profile] = check_profile(
                cfg, profile, no_restart=args.no_restart, log=log,
            )
        except Exception as e:
            log.error(f"{profile}: unhandled exception: {e}", exc_info=True)
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
