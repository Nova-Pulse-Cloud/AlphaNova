import asyncio
import logging
import hashlib
import json
from datetime import datetime
from typing import Dict, Any, Optional, Callable
from prometheus_client import Counter, Histogram
import aioredis

# Prometheus metrics
SHARD_SET_CALLS = Counter("shard_set_calls_total", "Total shard set calls")
SHARD_ERRORS = Counter("shard_errors_total", "Total shard errors")
SHARD_LATENCY = Histogram("shard_latency_seconds", "Shard set latency")
SHARD_DLQ = Counter("shard_dlq_total", "Shard DLQ events")

# Dead letter queue for failed shard sets
FAILED_SHARD_DLQ = []

# Logging setup
logger = logging.getLogger("recovery_shard_manager")

# Audit hash function for traceability
def shard_hash(shard: dict) -> str:
    m = hashlib.sha256()
    m.update(json.dumps(shard, sort_keys=True).encode())
    return m.hexdigest()

# Event handler for shard events (e.g., audit, failure)
async def fire_shard_event(event_name, data):
    # This is just a placeholder for actual event hook processing (e.g., Prometheus, DLQ)
    logger.info(f"Event triggered: {event_name} with data: {data}")

class RecoveryShardManager:
    def __init__(self, redis_url: str, max_versions: int = 6, redis=None, backup_callback=None):
        self.redis_url = redis_url
        self.max_versions = max_versions
        self.redis = redis
        self.shard_versions = {}
        self.backup_callback = backup_callback or self.backup_shard_external

    async def connect(self):
        """Connect to Redis."""
        if not self.redis:
            self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
        logger.info("RecoveryShardManager connected to Redis.")

    async def set_shard(self, group: str, shard: dict, user: Optional[str] = None, version: Optional[int] = None, trace_id: Optional[str] = None):
        """Sets a shard and manages version history with DLQ and backup."""
        SHARD_SET_CALLS.inc()
        async with SHARD_LATENCY.time():
            try:
                ts = int(datetime.utcnow().timestamp())
                ver = version or ts
                shard_key = f"shard:{group}:{user or 'global'}:{ver}"

                # Add audit hash for traceability
                shard["audit_hash"] = shard_hash(shard)

                # Store the shard in Redis
                await self.redis.set(shard_key, json.dumps(shard))

                # Prune old versions of shards based on max_versions
                await self._prune_old_shards(group, user)

                # Event with trace_id/audit
                await fire_shard_event("shard_set", {"group": group, "user": user, "ver": ver, "shard": shard, "trace_id": shard["audit_hash"]})

                # Backup shard to external service
                await self.backup_callback(shard_key, shard)

            except Exception as e:
                # Handle errors and push failed events to DLQ
                SHARD_ERRORS.inc()
                SHARD_DLQ.inc()
                failed_data = {"group": group, "shard": shard, "error": str(e), "user": user, "trace_id": shard.get("audit_hash")}
                FAILED_SHARD_DLQ.append(failed_data)
                await fire_shard_event("shard_set_failed", failed_data)
                logger.error(f"Failed to set shard for group {group}: {e}")

    async def _prune_old_shards(self, group: str, user: Optional[str] = None):
        """Prunes old shards to maintain the max_versions limit."""
        keys = await self.redis.keys(f"shard:{group}:{user or 'global'}:*")
        if len(keys) > self.max_versions:
            keys.sort(reverse=True)  # Sort by version, most recent first
            to_delete = keys[self.max_versions:]
            for key in to_delete:
                await self.redis.delete(key)
                logger.info(f"Pruned shard: {key}")

    async def backup_shard_external(self, shard_key: str, shard: dict):
        """Simulate an external backup process for the shard."""
        # External backup logic (e.g., S3, database, etc.)
        logger.info(f"Backing up shard {shard_key} to external storage.")
        # Placeholder backup operation (should be implemented based on requirements)
        await asyncio.sleep(0.5)  # Simulate a delay for the backup process

    async def replay_dlq(self):
        """Attempts to reprocess failed DLQ items."""
        for failed in FAILED_SHARD_DLQ[:]:
            try:
                # Attempt to retry the failed shard
                await self.set_shard(failed["group"], failed["shard"], failed["user"], trace_id=failed.get("trace_id"))
                FAILED_SHARD_DLQ.remove(failed)
            except Exception as e:
                logger.error(f"Failed to replay DLQ item: {e}")

    def get_dlq(self):
        """Returns the DLQ for failed shard operations."""
        return FAILED_SHARD_DLQ.copy()
