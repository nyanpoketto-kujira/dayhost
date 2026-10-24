"""Fusion core (Fusion Phase C) — orchestrator package.

Ported from ``mc-orchestrator/controller/`` with the SoloA/SSH/aiohttp parts
removed:

  * ``util.py``    stdlib helpers (ported as-is)
  * ``storage.py`` mirror / delta / snapshot / plugin store (ported as-is,
                   ``Config`` replaced by :class:`~.storage.StoreConfig`)
  * ``state.py``   in-memory state + log ring buffer (ported; sandbox state is
                   now a field of its own — see below)
  * ``core.py``    provisioning state machine, delta ingest, snapshot loop

**Two state sources, two fields, never merged** (Ner's decision #1):

  * ``state.mc_state``      — PaperMC process. Authoritative source: the agent
                              (``hello`` / ``mc_state`` / ``heartbeat``).
  * ``state.sandbox_state`` — sandbox lifecycle. Authoritative source: Daytona
                              (driver ``status`` / ``start`` / ``stop``).
  * ``state.status``        — agent WS link (online | disconnected | recovering).
                              This is a *link* flag, not a process or lifecycle
                              claim; it existed before this phase.

Nothing here is wired into ``app/main.py`` yet (that is Phase D). Importing this
package has no side effects: no socket, no task, no config read.
"""

from .core import Orchestrator
from .state import AppState
from .storage import Storage, StoreConfig

__all__ = ["AppState", "Orchestrator", "Storage", "StoreConfig"]
