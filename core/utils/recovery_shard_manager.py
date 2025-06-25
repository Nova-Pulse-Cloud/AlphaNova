# core/utils/recovery_shard_manager.py
"""
Async manager for versioned data shards with DLQ, pruning, and backup.
Handles Redis storage, filesystem backup, and Prometheus metrics.
"""
import asyncio
import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional

import redis.asyncio as redis_client
from prometheus_client import Counter, Histogram

from core.utils.config import config

logger = logging.getLogger("nova.recovery_shard_manager")
logger.setLevel(logging.INFO)

# Prometheus metrics
SHARD_SET_CALLS = Counter(
    "shard_set_calls_total", "Total shard set calls", ["group"]
)
SHARD_ERRORS = Counter(
    "shard_errors_total", "Total shard errors", ["group"]
)
SHARD_LATENCY = Histogram(
    "shard_latency_seconds", "Shard set latency", ["group"]
)
SHARD_DLQ = Counter(
    "shard_dlq_total", "Shard DLQ events", ["group"]
)

# Dead-letter queue and event hooks
FAILED_SHARD_DLQ: List[Dict[str, Any]] = []
_EVENT_HOOKS: List[Callable[[str, Dict[str, Any]], Awaitable[None]]] = []


def register_shard_event_hook(
    fn: Callable[[str, Dict[str, Any]], Awaitable[None]]
) -> None:
    """Register a coroutine to receive shard events."""
    _EVENT_HOOKS.append(fn)


async def fire_shard_event(event: str, data: Dict[str, Any]) -> None:
    """Dispatch a shard event to all registered hooks."""
    for hook in tuple(_EVENT_HOOKS):
        try:
            await hook(event, data)
        except Exception as exc:
            logger.error(f"[ShardManager] Event hook error: {exc}")


def _compute_hash(shard: Dict[str, Any]) -> str:
    """Compute a SHA256 hash for the shard payload."""
    return hashlib.sha256(
        json.dumps(shard, sort_keys=True).encode()
    ).hexdigest()


class RecoveryShardManager:
    def __init__(self) -> None:
        self.redis_url = config.REDIS_URL
        self.redis: Optional[redis_client.Redis] = None
        self.max_versions = int(getattr(config, "SHARD_MAX_VERSIONS", 6))
        self.artifact_path = Path(
            getattr(config, "SHARD_ARTIFACT_PATH", "data/shards")
        )
        self.artifact_path.mkdir(parents=True, exist_ok=True)

    async def connect(self) -> None:
        """Initialize Redis connection."""
        try:
            self.redis = redis_client.from_url(
                self.redis_url, decode_responses=True
            )
            await self.redis.ping()
            logger.info("[ShardManager] Connected to Redis")
        except Exception as exc:
            logger.error(f"[ShardManager] Redis connect failed: {exc}")
            self.redis = None

    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self.redis:
            await self.redis.close()
            logger.info("[ShardManager] Disconnected from Redis")

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
        user_key = user or "global"
        ver = version or int(datetime.utcnow().timestamp())
        shard_id = shard.get("trace_id") or _compute_hash(shard)
        shard.update({
            "trace_id": shard_id,
            "timestamp": int(datetime.utcnow().timestamp()),
        })

        SHARD_SET_CALLS.labels(group=group).inc()
        try:
            async with SHARD_LATENCY.labels(group=group).time():
                if not self.redis:
                    raise RuntimeError("Redis unavailable")
                key = f"shard:{group}:{user_key}:{ver}"
                await self.redis.set(key, json.dumps(shard))
                await self._prune_old(group, user_key)
                await fire_shard_event(
                    "shard_set", {"group": group, "user": user_key, "version": ver, "shard": shard}
                )
                self._backup_to_disk(key, shard)
        except Exception as exc:
            SHARD_ERRORS.labels(group=group).inc()
            SHARD_DLQ.labels(group=group).inc()
            failed = {
                "group": group,
                "user": user_key,
                "version": ver,
                "shard": shard,
                "error": str(exc),
                "trace_id": shard_id,
            }
            FAILED_SHARD_DLQ.append(failed)
            await fire_shard_event("shard_set_failed", failed)
            logger.error(f"[ShardManager] Failed to set shard {group}/{user_key}: {exc}")

    async def _prune_old(self, group: str, user_key: str) -> None:
        """Prune shards exceeding max_versions."""
        if not self.redis:
            return
        pattern = f"shard:{group}:{user_key}:*"
        keys = await self.redis.keys(pattern)
        entries = []
        for key in keys:
            try:
                ver = int(key.rsplit(':', 1)[-1])
                entries.append((ver, key))
            except ValueError:
                continue
        # Keep newest max_versions
        for _, old_key in sorted(entries, reverse=True)[self.max_versions:]:
            await self.redis.delete(old_key)
            logger.info(f"[ShardManager] Pruned shard: {old_key}")

    def _backup_to_disk(self, key: str, shard: Dict[str, Any]) -> None:
        """Save shard JSON to filesystem for backup."""
        safe = key.replace(':', '_')
        path = self.artifact_path / f"{safe}.json"
        try:
            with open(path, 'w') as f:
                json.dump(shard, f, indent=2)
        except Exception as exc:
            logger.error(f"[ShardManager] Backup failed: {exc}")

    async def replay_dlq(self) -> None:
        """Retry failed shard operations."""
        for entry in FAILED_SHARD_DLQ[:]:
            try:
                await self.set_shard(
                    entry["group"], entry["shard"], entry.get("user"), entry.get("version")
                )
                FAILED_SHARD_DLQ.remove(entry)
            except Exception as exc:
                logger.error(f"[ShardManager] DLQ replay failed {entry}: {exc}")

    def get_dlq(self) -> List[Dict[str, Any]]:
        """Return current DLQ entries."""
        return list(FAILED_SHARD_DLQ)
