"""DayHost configuration.

Deliberately dependency-light: `python-dotenv` + a frozen dataclass.
`pydantic-settings` was dropped to keep idle RSS low and the dep tree small.

Usage:
    from app.config import get_settings
    settings = get_settings()          # cached singleton
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent


def _env_file() -> Path | None:
    """Prefer an explicit DAYHOST_ENV_FILE, else repo-root .env."""
    explicit = os.environ.get("DAYHOST_ENV_FILE")
    if explicit:
        return Path(explicit)
    candidate = REPO_ROOT / ".env"
    return candidate if candidate.exists() else None


def _str(name: str, default: str = "") -> str:
    value = os.environ.get(name)
    return default if value is None else value.strip()


def _int(name: str, default: int) -> int:
    raw = _str(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _float(name: str, default: float) -> float:
    raw = _str(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _bool(name: str, default: bool = False) -> bool:
    raw = _str(name).lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _csv(name: str, default: str = "") -> tuple[str, ...]:
    raw = _str(name, default)
    return tuple(part.strip() for part in raw.split(",") if part.strip())


@dataclass(frozen=True)
class Settings:
    # --- Core ---------------------------------------------------------------
    env: str = "dev"
    app_host: str = "127.0.0.1"
    app_port: int = 8000
    allowed_hosts: tuple[str, ...] = ("127.0.0.1", "localhost")

    # --- Daytona ------------------------------------------------------------
    daytona_api_key: str = ""
    daytona_api_base: str = "https://app.daytona.io/api"
    sandbox_id: str = ""
    daytona_timeout: int = 15
    daytona_max_retries: int = 2
    # Path templates are configurable so we can adapt without a redeploy.
    # See app/services/daytona_client.py for the assumed endpoints.
    daytona_path_sandbox: str = "/sandbox/{sandbox_id}"
    daytona_path_start: str = "/sandbox/{sandbox_id}/start"
    daytona_path_stop: str = "/sandbox/{sandbox_id}/stop"
    daytona_path_recover: str = "/sandbox/{sandbox_id}/recover"
    # --- Daytona toolbox (file transport, Fase 3) ---------------------------
    # DIFFERENT base URL from the lifecycle API above. Empty -> discovered at
    # runtime from /sandbox/{id}/toolbox-proxy-url (cached, one call).
    daytona_toolbox_url: str = ""
    # The toolbox proxy publishes no securitySchemes of its own, so the auth
    # scheme is configurable: "bearer" (default, same key as lifecycle) or
    # "none". Anything else is a configuration error, reported cleanly.
    daytona_toolbox_auth: str = "bearer"

    # --- RCON (console) -----------------------------------------------------
    # Empty password keeps the Fase 1 stub behaviour (200 + executed:false).
    rcon_host: str = "127.0.0.1"
    rcon_port: int = 25575
    rcon_password: str = ""
    rcon_timeout: float = 5.0
    # Destructive commands (stop/op/ban/...) stay blocked unless explicitly
    # allowed by the operator.
    rcon_allow_destructive: bool = False
    # Minimum seconds between console commands (per session).
    rcon_min_interval: float = 1.0
    # Persistent RCON audit trail (SQLite). 0 disables purging at startup.
    rcon_audit_retention_days: int = 30

    # --- Auth / session -----------------------------------------------------
    admin_user: str = "admin"
    admin_password_hash: str = ""
    session_secret: str = ""
    session_ttl: int = 604800
    session_cookie: str = "dayhost_session"

    # --- Minecraft ----------------------------------------------------------
    mc_host: str = "127.0.0.1"
    mc_port: int = 25565
    mc_ping_timeout: int = 4

    # --- Tunnel -------------------------------------------------------------
    tunnel_mode: str = "frp"
    public_host: str = ""
    public_port: int = 25565
    frpc_log_path: str = ""

    # --- Storage ------------------------------------------------------------
    db_path: str = "./data/dayhost.db"
    backup_dir: str = "./data/backups"
    rclone_remote: str = ""
    # --- Sync execution (Fase 2) -------------------------------------------
    # Kill-switch: false means plans are built and logged but nothing is run.
    sync_enabled: bool = False
    sync_timeout: int = 180
    # Container-side paths used by the tar/rsync plans (Fase 1 hardcodes).
    sync_world_src: str = "/data/world"
    sync_world_dst: str = "/data"
    # Transport for the world transfer itself: "local" (Fase 2 behaviour) or
    # "daytona-files" (pull/push through the toolbox file API).
    sync_transport: str = "local"
    # Sandbox-side world directory pulled before the local archive is built.
    sync_remote_world: str = "/data/world"
    # Optional sandbox-side single-file archive. When set, the transport pulls
    # this one file instead of walking the world tree.
    sync_remote_archive: str = ""

    # --- Logging (Fase 3) ---------------------------------------------------
    log_file: str = "./data/dayhost.log"
    log_max_bytes: int = 5_242_880
    log_backup_count: int = 5
    log_level: str = "INFO"
    log_format: str = "plain"  # plain | json

    # --- Derived ------------------------------------------------------------
    extra_allowed_hosts: tuple[str, ...] = field(default_factory=tuple)

    # ------------------------------------------------------------------ utils
    @property
    def is_prod(self) -> bool:
        return self.env.lower() in {"prod", "production"}

    @property
    def cookie_secure(self) -> bool:
        return self.is_prod

    @property
    def db_file(self) -> Path:
        p = Path(self.db_path)
        return p if p.is_absolute() else (REPO_ROOT / p)

    @property
    def backup_path(self) -> Path:
        p = Path(self.backup_dir)
        return p if p.is_absolute() else (REPO_ROOT / p)

    @property
    def daytona_configured(self) -> bool:
        return bool(self.daytona_api_key and self.sandbox_id)

    @property
    def rcon_configured(self) -> bool:
        return bool(self.rcon_password)

    @property
    def log_path(self) -> Path:
        p = Path(self.log_file)
        return p if p.is_absolute() else (REPO_ROOT / p)

    @property
    def auth_configured(self) -> bool:
        return bool(self.admin_password_hash and self.session_secret)

    def host_allowlist(self) -> tuple[str, ...]:
        merged = list(self.allowed_hosts) + list(self.extra_allowed_hosts)
        seen: dict[str, None] = {}
        for host in merged:
            seen.setdefault(host, None)
        return tuple(seen)


def _build() -> Settings:
    env_file = _env_file()
    if env_file is not None:
        load_dotenv(env_file, override=False)

    return Settings(
        env=_str("DAYHOST_ENV", "dev"),
        app_host=_str("APP_HOST", "127.0.0.1"),
        app_port=_int("APP_PORT", 8000),
        allowed_hosts=_csv("ALLOWED_HOSTS", "127.0.0.1,localhost"),
        daytona_api_key=_str("DAYTONA_API_KEY"),
        daytona_api_base=_str("DAYTONA_API_BASE", "https://app.daytona.io/api"),
        sandbox_id=_str("SANDBOX_ID"),
        daytona_timeout=_int("DAYTONA_TIMEOUT", 15),
        daytona_max_retries=_int("DAYTONA_MAX_RETRIES", 2),
        daytona_path_sandbox=_str("DAYTONA_PATH_SANDBOX", "/sandbox/{sandbox_id}"),
        daytona_path_start=_str("DAYTONA_PATH_START", "/sandbox/{sandbox_id}/start"),
        daytona_path_stop=_str("DAYTONA_PATH_STOP", "/sandbox/{sandbox_id}/stop"),
        daytona_path_recover=_str("DAYTONA_PATH_RECOVER", "/sandbox/{sandbox_id}/recover"),
        daytona_toolbox_url=_str("DAYTONA_TOOLBOX_URL"),
        daytona_toolbox_auth=_str("DAYTONA_TOOLBOX_AUTH", "bearer").lower(),
        rcon_host=_str("RCON_HOST", "127.0.0.1"),
        rcon_port=_int("RCON_PORT", 25575),
        rcon_password=_str("RCON_PASSWORD"),
        rcon_timeout=_float("RCON_TIMEOUT", 5.0),
        rcon_allow_destructive=_bool("RCON_ALLOW_DESTRUCTIVE", False),
        rcon_min_interval=_float("RCON_MIN_INTERVAL", 1.0),
        rcon_audit_retention_days=_int("RCON_AUDIT_RETENTION_DAYS", 30),
        admin_user=_str("ADMIN_USER", "admin"),
        admin_password_hash=_str("ADMIN_PASSWORD_HASH"),
        session_secret=_str("SESSION_SECRET"),
        session_ttl=_int("SESSION_TTL", 604800),
        session_cookie=_str("SESSION_COOKIE", "dayhost_session"),
        mc_host=_str("MC_HOST", "127.0.0.1"),
        mc_port=_int("MC_PORT", 25565),
        mc_ping_timeout=_int("MC_PING_TIMEOUT", 4),
        tunnel_mode=_str("TUNNEL_MODE", "frp"),
        public_host=_str("PUBLIC_HOST"),
        public_port=_int("PUBLIC_PORT", 25565),
        frpc_log_path=_str("FRPC_LOG_PATH"),
        db_path=_str("DB_PATH", "./data/dayhost.db"),
        backup_dir=_str("BACKUP_DIR", "./data/backups"),
        rclone_remote=_str("RCLONE_REMOTE"),
        sync_enabled=_bool("SYNC_ENABLED", False),
        sync_timeout=_int("SYNC_TIMEOUT", 180),
        sync_world_src=_str("SYNC_WORLD_SRC", "/data/world"),
        sync_world_dst=_str("SYNC_WORLD_DST", "/data"),
        sync_transport=_str("SYNC_TRANSPORT", "local").lower(),
        sync_remote_world=_str("SYNC_REMOTE_WORLD", "/data/world"),
        sync_remote_archive=_str("SYNC_REMOTE_ARCHIVE"),
        log_file=_str("LOG_FILE", "./data/dayhost.log"),
        log_max_bytes=_int("LOG_MAX_BYTES", 5_242_880),
        log_backup_count=_int("LOG_BACKUP_COUNT", 5),
        log_level=_str("LOG_LEVEL", "INFO"),
        log_format=_str("LOG_FORMAT", "plain").lower(),
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached settings singleton. Call get_settings.cache_clear() in tests."""
    return _build()
