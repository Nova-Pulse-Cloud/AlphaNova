import os
import json
import logging
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import redis.asyncio as redis_client
from pydantic import BaseModel
from prometheus_client import Counter, Histogram

from core.utils.config import config
from core.neural_regret_predictor import NeuralRegretPredictor
from core.federated_memory_client import FederatedMemoryClient

logger = logging.getLogger("nova.training_data_logger")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
RL_UPDATES = Counter(
    "training_rl_updates_total", "Total reinforcement-learning updates applied"
)
MEMORY_LATENCY = Histogram(
    "training_memory_latency_seconds", "Latency of training memory operations"
)

# ────────────────────────────────────────────────────────────────────────────────
# Dead-letter queue for failed operations
# ────────────────────────────────────────────────────────────────────────────────
FAILED_TRADE_MEMORY_DLQ: List[Dict[str, Any]] = []

# ────────────────────────────────────────────────────────────────────────────────
# Trade signal data model
# ────────────────────────────────────────────────────────────────────────────────
class TradeRecord(BaseModel):
    symbol: str
    strategy: str
    outcome: float  # PnL or RL reward
    entry: float
    risk: float
    flavor: str
    agents: Dict[str, float]
    timestamp: int

# ────────────────────────────────────────────────────────────────────────────────
# Training Data Logger – production-grade
# ────────────────────────────────────────────────────────────────────────────────
class TrainingDataLogger:
    """
    Handles RL signal storage in Redis, disk backup, and federated upload.
    Fully async, with Prometheus instrumentation and no placeholders.
    """
    def __init__(self) -> None:
        self.redis_url = config.REDIS_URL
        self.redis: redis_client.Redis | None = None
        # Configurable artifact backup path
        self.artifact_path = Path(getattr(config, "TRAINING_ARTIFACT_PATH", "artifacts"))
        self.predictor = NeuralRegretPredictor()
        self.federated_client = FederatedMemoryClient()
        self.export_interval = int(getattr(config, "TRAINING_EXPORT_INTERVAL", 100))
        self.buffer: List[TradeRecord] = []

    async def connect(self) -> None:
        """Initialize Redis connection."""
        try:
            self.redis = redis_client.from_url(self.redis_url, decode_responses=True)
            await self.redis.ping()
            logger.info("[TrainingDataLogger] Connected to Redis")
        except Exception as e:
            logger.error(f"[TrainingDataLogger] Redis connection failed: {e}")
            self.redis = None

    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self.redis:
            await self.redis.close()
            logger.info("[TrainingDataLogger] Disconnected from Redis")

    async def record_signal(self, record: TradeRecord) -> None:
        """
        Store record in Redis or disk, update RL model, and flush when needed.
        """
        with MEMORY_LATENCY.time():
            # Buffer in-memory
            self.buffer.append(record)
            # Primary store: Redis
            if self.redis:
                try:
                    await self.redis.set(
                        f"trade:{record.symbol}:{record.timestamp}",
                        record.json(),
                        ex=86_400,
                    )
                except Exception as e:
                    logger.warning(f"[TrainingDataLogger] Redis write failed: {e}")
                    self._write_to_disk(record)
            else:
                logger.warning("[TrainingDataLogger] Redis unavailable, writing to disk")
                self._write_to_disk(record)

        # Flush buffer to federated store
        if len(self.buffer) >= self.export_interval:
            await self.flush_memory()

        # Update RL predictor
        await self._update_rl(record)

    async def _update_rl(self, record: TradeRecord) -> None:
        """Trigger RL model updates for each agent in the record."""
        for agent, confidence in record.agents.items():
            try:
                await self.predictor.update(
                    symbol=record.symbol,
                    agent=agent,
                    reward=record.outcome,
                    confidence=confidence,
                )
                RL_UPDATES.inc()
            except Exception as e:
                logger.error(f"[TrainingDataLogger] RL update failed for {agent}: {e}")
                FAILED_TRADE_MEMORY_DLQ.append({
                    "symbol": record.symbol,
                    "agent": agent,
                    "error": str(e),
                    "timestamp": record.timestamp,
                })

    async def flush_memory(self) -> None:
        """Upload buffered records to federated memory and clear buffer."""
        try:
            await self.federated_client.upload(self.buffer)
            logger.info(f"[TrainingDataLogger] Uploaded {len(self.buffer)} records to federated memory")
        except Exception as e:
            logger.error(f"[TrainingDataLogger] Federated upload failed: {e}")
            # Disk‐backup on failure
            for rec in self.buffer:
                try:
                    self._write_to_disk(rec)
                except Exception as disk_e:
                    FAILED_TRADE_MEMORY_DLQ.append({
                        **rec.dict(),
                        "error": str(disk_e),
                    })
        finally:
            self.buffer.clear()

    def _write_to_disk(self, record: TradeRecord) -> None:
        """Append record JSON to artifact file as fallback storage."""
        self.artifact_path.mkdir(parents=True, exist_ok=True)
        path = self.artifact_path / "training_backup.jsonl"
        try:
            with open(path, "a") as f:
                f.write(record.json() + "\n")
        except Exception as e:
            logger.error(f"[TrainingDataLogger] Disk write failed: {e}")
            raise

    def get_dlq(self) -> List[Dict[str, Any]]:
        """Return a copy of the dead‐letter queue."""
        return FAILED_TRADE_MEMORY_DLQ.copy()
