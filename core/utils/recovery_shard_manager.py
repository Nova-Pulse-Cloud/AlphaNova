# core/utils/recovery_shard_manager.py

import asyncio
import logging
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import redis.asyncio as redis_client
from prometheus_client import Counter, Histogram

from core.utils.config import config

logger = logging.getLogger("nova.recovery_shard_manager")
logger.setLevel(logging.INFO)

# Prometheus metrics
SHARD_SET_CALLS = Counter("shard_set_calls_total", "Total shard set calls")
SHARD_ERRORS = Counter("shard_errors_total", "Total shard errors")
SHARD_LATENCY = Histogram("shard_latency_seconds", "Shard set latency")
SHARD_DLQ = Counter("shard_dlq_total", "Shard DLQ events")

# Dead-letter queue for failed shard operations
FAILED_SHARD_DLQ: List[Dict[str, Any]] = []

# Event hooks
shard_event_hooks: List = []

def register_shard_event_hook(fn: Any) -> None:
    """Register a callback for shard events."""
    shard_event_hooks.append(fn)

async def fire_shard_event(event: str, data: Dict[str, Any]) -> None:
    """Invoke all registered shard event hooks."""
    for hook in shard_event_hooks:
        try:
            await hook(event, data)
        except Exception as e:
            logger.error(f"[ShardManager] Hook error: {e}")


def shard_hash(shard: Dict[str, Any]) -> str:
    """Compute a SHA256 hash for a shard record."""
    return hashlib.sha256(json.dumps(shard, sort_keys=True).encode()).hexdigest()

class RecoveryShardManager:
    """
    Async manager for versioned data shards with DLQ, pruning, and backup.
    """
    def __init__(self) -> None:
        self.redis_url = config.REDIS_URL
        self.redis: Optional[redis_client.Redis] = None
        self.max_versions = int(getattr(config, "SHARD_MAX_VERSIONS", 6))
        artifact = getattr(config, "SHARD_ARTIFACT_PATH", "artifacts/shards")
        self.artifact_path = Path(artifact)
        self.backup_callback = self._backup_to_disk

    async def connect(self) -> None:
        """Initialize Redis connection."""
        try:
            self.redis = redis_client.from_url(self.redis_url, decode_responses=True)
            await self.redis.ping()
            logger.info("[ShardManager] Connected to Redis")
        except Exception as e:
            logger.error(f"[ShardManager] Redis connection failed: {e}")
            self.redis = None

    async def set_shard(
        self,
        group: str,
        shard: Dict[str, Any],
        user: Optional[str] = None,
        version: Optional[int] = None,
    ) -> None:
        """
        Store a shard with versioning, pruning, and backup.
        """
        SHARD_SET_CALLS.inc()
        start_ts = int(datetime.utcnow().timestamp())
        key_user = user or "global"
        ver = version or start_ts
        trace_id = shard.get("trace_id") or shard_hash(shard)
        shard["trace_id"] = trace_id

        try:
            async with SHARD_LATENCY.time():
                if not self.redis:
                    raise RuntimeError("Redis unavailable")
                key = f"shard:{group}:{key_user}:{ver}"
                await self.redis.set(key, json.dumps(shard))
                await self._prune_old_shards(group, key_user)
                await fire_shard_event("shard_set", {"group": group, "user": user, "version": ver, "trace_id": trace_id})
                self.backup_callback(key, shard)
        except Exception as e:
            SHARD_ERRORS.inc()
            SHARD_DLQ.inc()
            failed_entry = {"group": group, "user": user, "version": ver, "shard": shard, "error": str(e), "trace_id": trace_id}
            FAILED_SHARD_DLQ.append(failed_entry)
            await fire_shard_event("shard_set_failed", failed_entry)
            logger.error(f"[ShardManager] Failed to set shard for {group}/{key_user}: {e}")

    async def _prune_old_shards(self, group: str, user: str) -> None:
        """Remove oldest shards to enforce max_versions."""
        if not self.redis:
            return
        pattern = f"shard:{group}:{user}:*"
        keys = await self.redis.keys(pattern)
        entries: List[Tuple[int, str]] = []
        for k in keys:
            try:
                ver = int(k.rsplit(":", 1)[-1])
                entries.append((ver, k))
            except ValueError:
                continue
        entries.sort(reverse=True)
        for _, k in entries[self.max_versions:]:
            await self.redis.delete(k)
            logger.info(f"[ShardManager] Pruned shard: {k}")

    def _backup_to_disk(self, key: str, shard: Dict[str, Any]) -> None:
        """Backup shard data to filesystem as JSON."""
        self.artifact_path.mkdir(parents=True, exist_ok=True)
        safe_key = key.replace(':', '_')
        path = self.artifact_path / f"{safe_key}.json"
        try:
            with open(path, "w") as f:
                json.dump(shard, f)
        except Exception as e:
            logger.error(f"[ShardManager] Backup to disk failed: {e}")

    async def replay_dlq(self) -> None:
        """Retry failed shard operations from DLQ."""
        for entry in FAILED_SHARD_DLQ[:]:
            try:
                await self.set_shard(entry["group"], entry["shard"], entry.get("user"), entry.get("version"))
                FAILED_SHARD_DLQ.remove(entry)
            except Exception as e:
                logger.error(f"[ShardManager] Replay DLQ failed: {e}")

    def get_dlq(self) -> List[Dict[str, Any]]:
        """Return current DLQ entries."""
        return list(FAILED_SHARD_DLQ)
