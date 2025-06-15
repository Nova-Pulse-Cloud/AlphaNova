# core/utils/training_data_logger.py

import os
import json
import logging
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

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
RL_UPDATES       = Counter("training_rl_updates_total", "Total RL updates applied")
MEMORY_LATENCY    = Histogram("training_memory_latency_seconds", "Latency of training memory operations")
SIGNAL_STORED     = Counter("training_signals_stored_total", "Total training signals stored (Redis/disk)")
FLUSH_COUNT       = Counter("training_flush_total", "Total federated memory flush operations")
FLUSH_ERRORS      = Counter("training_flush_errors_total", "Errors during federated memory flush")

# ────────────────────────────────────────────────────────────────────────────────
# Dead-letter queue
# ────────────────────────────────────────────────────────────────────────────────
FAILED_TRADE_MEMORY_DLQ: List[Dict[str, Any]] = []

# ────────────────────────────────────────────────────────────────────────────────
# Data Model
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
# Training Data Logger
# ────────────────────────────────────────────────────────────────────────────────
class TrainingDataLogger:
    """
    Handles RL signal storage in Redis, disk backup, and federated upload.
    Fully async, with Prometheus instrumentation and no placeholders.
    """
    def __init__(self) -> None:
        self.redis_url     = config.REDIS_URL
        self.redis: Optional[redis_client.Redis] = None
        self.artifact_path = Path(getattr(config, "TRAINING_ARTIFACT_PATH", "artifacts"))
        self.export_interval = int(getattr(config, "TRAINING_EXPORT_INTERVAL", 100))
        self.buffer: List[TradeRecord] = []
        self.predictor = NeuralRegretPredictor()
        self.federated_client = FederatedMemoryClient()
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        try:
            self.redis = redis_client.from_url(self.redis_url, decode_responses=True)
            await self.redis.ping()
            logger.info("[TrainingDataLogger] Connected to Redis")
        except Exception as e:
            logger.error(f"[TrainingDataLogger] Redis connection failed: {e}")
            self.redis = None

    async def disconnect(self) -> None:
        if self.redis:
            await self.redis.close()
            logger.info("[TrainingDataLogger] Disconnected from Redis")

    async def record_signal(self, record: TradeRecord) -> None:
        """Store record, update RL model, and flush when needed."""
        async with self._lock:
            with MEMORY_LATENCY.time():
                rec_json = record.json()
                try:
                    if self.redis:
                        await self.redis.set(
                            f"training:{record.symbol}:{record.timestamp}", rec_json,
                            ex=86400
                        )
                    else:
                        raise RuntimeError("Redis unavailable")
                    SIGNAL_STORED.inc()
                except Exception as e:
                    logger.warning(f"[TrainingDataLogger] Redis write failed: {e}")
                    self._write_to_disk(record)

                # Buffer for federated upload
                self.buffer.append(record)

            # RL model update
            await self._update_rl(record)

            # Flush if threshold reached
            if len(self.buffer) >= self.export_interval:
                await self.flush_memory()

    async def _update_rl(self, record: TradeRecord) -> None:
        """Trigger RL updates for each agent."""
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
        """Upload buffer to federated memory and clear buffer."""
        FLUSH_COUNT.inc()
        try:
            await self.federated_client.upload(self.buffer)
            logger.info(f"[TrainingDataLogger] Uploaded {len(self.buffer)} records")
        except Exception as e:
            FLUSH_ERRORS.inc()
            logger.error(f"[TrainingDataLogger] Federated upload failed: {e}")
            for rec in self.buffer:
                try:
                    self._write_to_disk(rec)
                except Exception as de:
                    FAILED_TRADE_MEMORY_DLQ.append({**rec.dict(), "error": str(de)})
        finally:
            self.buffer.clear()

    def _write_to_disk(self, record: TradeRecord) -> None:
        """Append record JSON to local artifact file as fallback."""
        path = self.artifact_path / "training_backup.jsonl"
        try:
            self.artifact_path.mkdir(parents=True, exist_ok=True)
            with open(path, "a") as f:
                f.write(record.json() + "\n")
        except Exception as e:
            logger.error(f"[TrainingDataLogger] Disk write failed: {e}")

    def get_dlq(self) -> List[Dict[str, Any]]:
        """Return current dead-letter queue items."""
        return list(FAILED_TRADE_MEMORY_DLQ)
