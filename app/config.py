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
    # --- Daytona process / exec (Fase 4-A, daytona_driver.py) ---------------
    # Every path is a template so a wrong assumption is fixed by editing .env,
    # not by redeploying. Ground truth: docs/daytona-file-api.md (v0.225).
    daytona_toolbox_path_exec: str = "/process/session/{session_id}/exec"
    daytona_toolbox_path_session: str = "/process/session"
    daytona_toolbox_path_session_get: str = "/process/session/{session_id}"
    daytona_toolbox_path_command: str = "/process/session/{session_id}/command/{command_id}"
    daytona_toolbox_path_process_status: str = "/process/{name}/status"
    daytona_toolbox_path_process_logs: str = "/process/{name}/logs"
    # Stateless fallback used when DAYTONA_TOOLBOX_EXEC_MODE=stateless.
    daytona_process_exec_path: str = "/process/exec"
    # "session" (POST /process/session/{id}/exec, preferred) or "stateless"
    # (POST /process/exec). An empty DAYTONA_TOOLBOX_PATH_EXEC also forces
    # stateless mode, which is the one-setting escape hatch.
    daytona_toolbox_exec_mode: str = "session"
    # Reused across calls so exec does not pay a session create each time.
    daytona_toolbox_session_id: str = "dayhost"
    # Bound the start poll loop: total seconds, and the gap between GETs.
    daytona_start_timeout: int = 90
    daytona_poll_interval: float = 2.0
    # --- Agent launch (Fase 4-A) --------------------------------------------
    # Directory inside the sandbox holding agent.py, and the launch template.
    # {dir} is substituted; `setsid nohup ... &` keeps the agent alive after
    # the exec call returns.
    agent_remote_dir: str = "mc-orchestrator/agent"
    agent_launch_cmd: str = (
        "cd {dir} && setsid nohup python3 agent.py >> agent.log 2>&1 &"
    )

    # --- Agent WebSocket server (Fase 4-B, agent_ws.py) ---------------------
    # The venv has no websockets/wsproto, so the agent link is a dedicated
    # stdlib RFC-6455 server on its own port (fusion-analysis.md §3 Option A).
    # NOTHING starts it yet: app/main.py does not wire it. Phase C does.
    agent_ws_enabled: bool = False
    agent_ws_host: str = "127.0.0.1"
    agent_ws_port: int = 8765
    agent_ws_path: str = "/agent-ws"
    # Shared secret the agent sends in the X-Agent-Token header
    # (a "token" query-string parameter is accepted too). Falls back
    # to AGENT_TOKEN. If both are empty the server still starts but refuses
    # every connection with 503 — never an unauthenticated agent link.
    agent_token: str = ""
    agent_ws_token: str = ""
    # Heartbeat: ping every N seconds, close a connection whose last frame is
    # older than M seconds. M must stay comfortably above N.
    agent_ws_ping_interval: float = 20.0
    agent_ws_stale_timeout: float = 75.0
    # Frame/message cap in bytes (4 MiB, same as the agent's client).
    agent_ws_max_frame: int = 4_194_304
    # Public host the agent dials back on. Empty -> AGENT_WS_HOST (fine for a
    # loopback/tunnel-only setup; set it to the real VPS hostname for a sandbox).
    agent_ws_public_host: str = ""

    # --- Orchestrator / fusion core (Fase 4-C) ------------------------------
    # Master switch for the Phase-C wiring. Phase D flips this from main.py;
    # until then nothing imports the orchestrator at startup.
    orch_enabled: bool = False
    # Root for the ported storage. Subdirectories are fixed on purpose so a
    # mirror, a snapshot and the SQLite db never share a directory.
    orch_data_dir: str = "./data"
    orch_mirror_dir: str = ""        # empty -> {orch_data_dir}/mirror
    orch_snapshots_dir: str = ""     # empty -> {orch_data_dir}/snapshots
    orch_deltas_dir: str = ""        # empty -> {orch_data_dir}/deltas
    orch_plugins_dir: str = ""       # empty -> {orch_data_dir}/plugins
    orch_tmp_dir: str = ""           # empty -> {orch_data_dir}/orch-tmp
    # Snapshot loop: interval in seconds (0 disables the loop), how many to
    # keep, and how many raw delta archives to keep.
    orch_snapshot_enabled: bool = True
    orch_snapshot_interval: float = 21_600.0
    orch_snapshot_keep: int = 8
    orch_delta_keep: int = 20
    orch_max_plugin_bytes: int = 100 * 1024 * 1024
    # Provisioning bounds: whole sandbox start, and how long to wait for the
    # agent's WS hello after it was launched.
    orch_provision_timeout: float = 180.0
    orch_connect_timeout: float = 60.0
    orch_connect_poll: float = 0.2
    orch_stop_mc_timeout: float = 90.0
    orch_backup_flush_timeout: float = 120.0
    # Start the host when the orchestrator starts (off by default: a restart of
    # the panel must not boot a sandbox unattended).
    orch_autostart: bool = False
    # Values forwarded verbatim into the generated agent.env (see
    # Orchestrator._build_agent_env). Explicit flags beat agent defaults: the
    # orchestrator, not the sandbox, decides who provisions and who boots MC.
    orch_provision: bool = True
    orch_kill_orphans: bool = True
    orch_ws_insecure: bool = False
    orch_controller_http: str = ""
    # Watchdog: on agent disconnect/stale, re-run the idempotent bootstrap.
    orch_recovery_enabled: bool = True
    # Wait this long before reacting (a normal agent reconnect beats it), and
    # never retry more often than the cooldown.
    orch_recovery_grace: float = 5.0
    orch_recovery_cooldown: float = 30.0
    # Artifacts pushed to the sandbox. Relative paths resolve against REPO_ROOT.
    orch_agent_source: str = "agent/agent.py"
    orch_agent_stop_cmd: str = "pkill -f agent.py"
    # Values written into the generated agent.env (never hand-edited in the box).
    # Java is pinned to 21 for this phase (Ner's decision; the old agent used 25).
    orch_java_version: int = 21
    orch_server_dir: str = "mc-orchestrator/server"
    orch_runtime_dir: str = "mc-orchestrator/runtime"
    orch_sandbox_home: str = "/home/daytona"   # root sandbox; relative orch paths resolve here
    # CONTROLLER_WS written into agent.env. Empty -> built from AGENT_WS_*.
    orch_agent_ws_url: str = ""
    orch_agent_ws_tls: bool = False

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
    tunnel_mode: str = "frp"        # "frp" | "ssh" | "none"
    public_host: str = ""
    public_port: int = 25565

    # --- SSH tunnel to the Daytona sandbox (see docs/daytona-ssh-tunnel.md) --
    # The sandbox cannot dial us (org-level egress block), so the VPS dials
    # the sandbox. These are only used when tunnel_mode == "ssh".
    ssh_host: str = "ssh.app.daytona.io"
    ssh_token_minutes: int = 1440
    ssh_connect_timeout: float = 25.0
    ssh_renew_margin: float = 600.0
    ssh_bind_mc: str = "0.0.0.0"

    frpc_log_path: str = ""
    # --- FRP client (Fase E) ------------------------------------------------
    # Values written into the sandbox's agent.env; the agent edits none of them
    # locally. frp >= 0.52 speaks TOML only, hence the .toml name.
    frp_server_addr: str = ""
    frp_server_port: int = 7000
    frp_token: str = ""
    frp_local_port: int = 25565        # agent-side PaperMC port
    frp_remote_port: int = 25565       # port players connect to on the VPS
    frp_version: str = "0.71.1"
    # SHA-256 of frp_<version>_linux_<arch>.tar.gz. EMPTY by default on
    # purpose: the agent then refuses to download frpc (state
    # "unconfigured") instead of trusting an unverified binary.
    # Official list: https://github.com/fatedier/frp/releases/download/v0.71.1/frp_sha256_checksums.txt
    # linux_amd64 -> 1d9875210972a5d1a62b2dc4c48df45ab0af05d3ca1b5e494a014428259df803
    # linux_arm64 -> 8edf590b0c396fa42f9c147121a76af83cfc2002b08f5aef0ea1008c43482019
    # The value is per-arch; set only the one you deploy.
    frp_sha256: str = ""

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
    def daytona_toolbox_configured(self) -> bool:
        """Toolbox needs a sandbox id; the base may still be discovered."""
        return bool(self.sandbox_id) and self.daytona_toolbox_auth in {"bearer", "none"}

    @property
    def rcon_configured(self) -> bool:
        return bool(self.rcon_password)

    @property
    def agent_ws_configured(self) -> bool:
        """The WS server is usable only with a token to compare against."""
        return bool(self.agent_ws_effective_token)

    @property
    def agent_ws_effective_token(self) -> str:
        """AGENT_WS_TOKEN wins; AGENT_TOKEN is the shared fallback."""
        return self.agent_ws_token or self.agent_token

    @property
    def log_path(self) -> Path:
        p = Path(self.log_file)
        return p if p.is_absolute() else (REPO_ROOT / p)

    def _orch_path(self, value: str, default_name: str) -> Path:
        """One resolved orchestrator directory. Explicit value wins."""
        raw = (value or "").strip()
        if raw:
            p = Path(raw)
            return p if p.is_absolute() else (REPO_ROOT / p)
        root = Path(self.orch_data_dir or "./data")
        if not root.is_absolute():
            root = REPO_ROOT / root
        return root / default_name

    @property
    def orch_mirror_path(self) -> Path:
        return self._orch_path(self.orch_mirror_dir, "mirror")

    @property
    def orch_snapshots_path(self) -> Path:
        return self._orch_path(self.orch_snapshots_dir, "snapshots")

    @property
    def orch_deltas_path(self) -> Path:
        return self._orch_path(self.orch_deltas_dir, "deltas")

    @property
    def orch_plugins_path(self) -> Path:
        return self._orch_path(self.orch_plugins_dir, "plugins")

    @property
    def orch_tmp_path(self) -> Path:
        return self._orch_path(self.orch_tmp_dir, "orch-tmp")

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
        daytona_toolbox_path_exec=_str(
            "DAYTONA_TOOLBOX_PATH_EXEC", "/process/session/{session_id}/exec"
        ),
        daytona_toolbox_path_session=_str("DAYTONA_TOOLBOX_PATH_SESSION", "/process/session"),
        daytona_toolbox_path_session_get=_str(
            "DAYTONA_TOOLBOX_PATH_SESSION_GET", "/process/session/{session_id}"
        ),
        daytona_toolbox_path_command=_str(
            "DAYTONA_TOOLBOX_PATH_COMMAND",
            "/process/session/{session_id}/command/{command_id}",
        ),
        daytona_toolbox_path_process_status=_str(
            "DAYTONA_TOOLBOX_PATH_PROCESS_STATUS", "/process/{name}/status"
        ),
        daytona_toolbox_path_process_logs=_str(
            "DAYTONA_TOOLBOX_PATH_PROCESS_LOGS", "/process/{name}/logs"
        ),
        daytona_process_exec_path=_str("DAYTONA_PROCESS_EXEC_PATH", "/process/exec"),
        daytona_toolbox_exec_mode=_str("DAYTONA_TOOLBOX_EXEC_MODE", "session").lower(),
        daytona_toolbox_session_id=_str("DAYTONA_TOOLBOX_SESSION_ID", "dayhost"),
        daytona_start_timeout=_int("DAYTONA_START_TIMEOUT", 90),
        daytona_poll_interval=_float("DAYTONA_POLL_INTERVAL", 2.0),
        agent_remote_dir=_str("AGENT_REMOTE_DIR", "mc-orchestrator/agent"),
        agent_launch_cmd=_str(
            "AGENT_LAUNCH_CMD",
            "cd {dir} && setsid nohup python3 agent.py >> agent.log 2>&1 &",
        ),
        agent_ws_enabled=_bool("AGENT_WS_ENABLED", False),
        agent_ws_host=_str("AGENT_WS_HOST", "127.0.0.1"),
        agent_ws_port=_int("AGENT_WS_PORT", 8765),
        agent_ws_path=_str("AGENT_WS_PATH", "/agent-ws"),
        agent_token=_str("AGENT_TOKEN"),
        agent_ws_token=_str("AGENT_WS_TOKEN"),
        agent_ws_ping_interval=_float("AGENT_WS_PING_INTERVAL", 20.0),
        agent_ws_stale_timeout=_float("AGENT_WS_STALE_TIMEOUT", 75.0),
        agent_ws_max_frame=_int("AGENT_WS_MAX_FRAME", 4_194_304),
        agent_ws_public_host=_str("AGENT_WS_PUBLIC_HOST"),
        orch_enabled=_bool("ORCH_ENABLED", False),
        orch_data_dir=_str("ORCH_DATA_DIR", "./data"),
        orch_mirror_dir=_str("ORCH_MIRROR_DIR"),
        orch_snapshots_dir=_str("ORCH_SNAPSHOTS_DIR"),
        orch_deltas_dir=_str("ORCH_DELTAS_DIR"),
        orch_plugins_dir=_str("ORCH_PLUGINS_DIR"),
        orch_tmp_dir=_str("ORCH_TMP_DIR"),
        orch_snapshot_enabled=_bool("ORCH_SNAPSHOT_ENABLED", True),
        orch_snapshot_interval=_float("ORCH_SNAPSHOT_INTERVAL", 21_600.0),
        orch_snapshot_keep=_int("ORCH_SNAPSHOT_KEEP", 8),
        orch_delta_keep=_int("ORCH_DELTA_KEEP", 20),
        orch_max_plugin_bytes=_int("ORCH_MAX_PLUGIN_BYTES", 100 * 1024 * 1024),
        orch_provision_timeout=_float("ORCH_PROVISION_TIMEOUT", 180.0),
        orch_connect_timeout=_float("ORCH_CONNECT_TIMEOUT", 60.0),
        orch_connect_poll=_float("ORCH_CONNECT_POLL", 0.2),
        orch_stop_mc_timeout=_float("ORCH_STOP_MC_TIMEOUT", 90.0),
        orch_backup_flush_timeout=_float("ORCH_BACKUP_FLUSH_TIMEOUT", 120.0),
        orch_autostart=_bool("ORCH_AUTOSTART", False),
        orch_provision=_bool("ORCH_PROVISION", True),
        orch_kill_orphans=_bool("ORCH_KILL_ORPHANS", True),
        orch_ws_insecure=_bool("ORCH_WS_INSECURE", False),
        orch_controller_http=_str("ORCH_CONTROLLER_HTTP"),
        orch_recovery_enabled=_bool("ORCH_RECOVERY_ENABLED", True),
        orch_recovery_grace=_float("ORCH_RECOVERY_GRACE", 5.0),
        orch_recovery_cooldown=_float("ORCH_RECOVERY_COOLDOWN", 30.0),
        orch_agent_source=_str("ORCH_AGENT_SOURCE", "agent/agent.py"),
        orch_agent_stop_cmd=_str("ORCH_AGENT_STOP_CMD", "pkill -f agent.py"),
        orch_java_version=_int("ORCH_JAVA_VERSION", 21),
        orch_server_dir=_str("ORCH_SERVER_DIR", "mc-orchestrator/server"),
        orch_runtime_dir=_str("ORCH_RUNTIME_DIR", "mc-orchestrator/runtime"),
        orch_sandbox_home=_str("ORCH_SANDBOX_HOME", "/home/daytona"),
        orch_agent_ws_url=_str("ORCH_AGENT_WS_URL"),
        orch_agent_ws_tls=_bool("ORCH_AGENT_WS_TLS", False),
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
        ssh_host=_str("SSH_HOST", "ssh.app.daytona.io"),
        ssh_token_minutes=_int("SSH_TOKEN_MINUTES", 1440),
        ssh_connect_timeout=_float("SSH_CONNECT_TIMEOUT", 25.0),
        ssh_renew_margin=_float("SSH_RENEW_MARGIN", 600.0),
        ssh_bind_mc=_str("SSH_BIND_MC", "0.0.0.0"),
        frpc_log_path=_str("FRPC_LOG_PATH"),
        frp_server_addr=_str("FRP_SERVER_ADDR"),
        frp_server_port=_int("FRP_SERVER_PORT", 7000),
        frp_token=_str("FRP_TOKEN"),
        frp_local_port=_int("FRP_LOCAL_PORT", 25565),
        frp_remote_port=_int("FRP_REMOTE_PORT", 25565),
        frp_version=_str("FRP_VERSION", "0.71.1"),
        frp_sha256=_str("FRP_SHA256").lower(),
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
