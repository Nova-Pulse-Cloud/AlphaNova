# core/federated_memory_client.py

import asyncio
import logging
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime
from prometheus_client import Counter, Histogram
from pydantic import BaseModel, Field

logger = logging.getLogger("nova.federated_memory_client")
logger.setLevel(logging.INFO)

# ─────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ─────────────────────────────────────────────────────────────────────────────
FEDMEM_UPLOADS   = Counter("federated_uploads_total", "Total federated memory uploads")
FEDMEM_ERRORS    = Counter("federated_errors_total", "Federated memory errors")
FEDMEM_LATENCY   = Histogram("federated_upload_latency_seconds", "Upload latency")
FEDMEM_DLQ       = Counter("federated_dlq_total", "Federated memory DLQ events")

# ─────────────────────────────────────────────────────────────────────────────
# Event hooks, DLQ, and live registry
# ─────────────────────────────────────────────────────────────────────────────
_fedmem_event_hooks: List[Callable[[str, Dict[str, Any]], Any]] = []
FAILED_FEDMEM_DLQ: List[Dict[str, Any]] = []

def register_federated_event_hook(fn: Callable[[str, Dict[str, Any]], Any]) -> None:
    _fedmem_event_hooks.append(fn)

async def _fire_fedmem_event(event: str, payload: Dict[str, Any]) -> None:
    for hook in _fedmem_event_hooks:
        try:
            result = hook(event, payload)
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            logger.error(f"[FedMem] Event hook {hook.__name__} failed: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────
class MemoryRecord(BaseModel):
    symbol: str
    data: Dict[str, Any]
    agent: Optional[str] = None
    ts: float = Field(default_factory=lambda: datetime.utcnow().timestamp())

# ─────────────────────────────────────────────────────────────────────────────
# Elite FederatedMemoryClient
# ─────────────────────────────────────────────────────────────────────────────
class FederatedMemoryClient:
    """
    Elite async federated-memory client.
    No network backend by default, but full event, DLQ, and Prometheus support.
    Easily connect to any mesh, DB, or cloud store by extending `upload`.
    """
    def __init__(self):
        self._local_store: List[MemoryRecord] = []
        self._lock = asyncio.Lock()
        self._dlq = FAILED_FEDMEM_DLQ

    async def upload(self, records: List[Any]) -> None:
        """Upload a list of training/fill/memory records to federated mesh/cloud."""
        FEDMEM_UPLOADS.inc()
        async with self._lock, FEDMEM_LATENCY.time():
            try:
                # --- Simulated upload (in-memory append) ---
                for rec in records:
                    if isinstance(rec, dict):
                        rec_obj = MemoryRecord(**rec)
                    elif isinstance(rec, MemoryRecord):
                        rec_obj = rec
                    else:
                        raise ValueError("Invalid record type for upload")
                    self._local_store.append(rec_obj)
                await _fire_fedmem_event("fedmem_upload", {
                    "count": len(records),
                    "symbols": [getattr(r, 'symbol', None) or r.get('symbol') for r in records]
                })
            except Exception as e:
                FEDMEM_ERRORS.inc()
                FEDMEM_DLQ.inc()
                self._dlq.append({"records": records, "error": str(e)})
                logger.error(f"[FederatedMemoryClient] Upload failed: {e}")
                raise

    def get_local_store(self) -> List[Dict[str, Any]]:
        return [r.dict() for r in self._local_store]

    def get_dlq(self) -> List[Dict[str, Any]]:
        return list(self._dlq)

    async def replay_dlq(self) -> int:
        """Retry DLQ uploads for any failed record batches."""
        successes = 0
        for entry in list(self._dlq):
            try:
                await self.upload(entry.get("records", []))
                self._dlq.remove(entry)
                successes += 1
            except Exception as e:
                logger.error(f"[FedMem][DLQ] Still failed: {e}")
        return successes

# Explicit exports
__all__ = [
    "FederatedMemoryClient",
    "register_federated_event_hook",
    "FAILED_FEDMEM_DLQ",
    "MemoryRecord",
]
