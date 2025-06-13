import os
import json
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, List

import aioredis
from pydantic import BaseModel
from prometheus_client import Counter, Histogram

from core.neural_regret_predictor import NeuralRegretPredictor
from core.federated_memory_client import FederatedMemoryClient
from utils.training_data_logger import log_training_sample

logger = logging.getLogger("trade_memory")

# === Metrics ===
RL_UPDATES = Counter("trade_memory_rl_updates_total", "Total RL updates applied")
MEMORY_LATENCY = Histogram("trade_memory_latency_seconds", "Latency of memory operations")

# === DLQ for failed writes ===
FAILED_TRADE_MEMORY_DLQ = []

# === Trade Data Model ===
class TradeRecord(BaseModel):
    symbol: str
    strategy: str
    outcome: float  # True PnL or RL reward
    entry: float
    risk: float
    flavor: str
    agents: Dict[str, float]
    timestamp: int

class TradeMemory:
    def __init__(self, redis_url: str, artifact_path: str = ".github/artifacts"):
        self.redis_url = redis_url
        self.redis = None
        self.artifact_path = artifact_path
        self.predictor = NeuralRegretPredictor()
        self.federated_client = FederatedMemoryClient()
        self.export_interval = 100
        self.buffer: List[TradeRecord] = []

    async def connect(self):
        try:
            self.redis = await aioredis.from_url(self.redis_url)
            logger.info("TradeMemory connected to Redis.")
        except Exception as e:
            logger.error(f"TradeMemory failed to connect to Redis: {e}")
            self.redis = None

    async def disconnect(self):
        if self.redis:
            await self.redis.close()
            logger.info("TradeMemory disconnected.")

    async def record_signal(self, record: TradeRecord):
        async with MEMORY_LATENCY.time():
            self.buffer.append(record)
            try:
                if self.redis:
                    await self.redis.set(
                        f"trade:{record.symbol}:{record.timestamp}",
                        record.json(),
                        ex=86400
                    )
                else:
                    raise Exception("Redis unavailable")
            except Exception as e:
                logger.warning(f"[TradeMemory] Redis unavailable. Writing to disk. {e}")
                try:
                    self._write_to_disk(record)
                except Exception as disk_e:
                    FAILED_TRADE_MEMORY_DLQ.append({"record": record.dict(), "error": str(disk_e)})

            if len(self.buffer) >= self.export_interval:
                await self.flush_memory()

            reward = record.outcome
            await self._update_rl(record.symbol, record.agents, reward, record)

    async def _update_rl(self, symbol: str, agents: Dict[str, float], reward: float, record: TradeRecord):
        for agent, score in agents.items():
            try:
                await self.predictor.update(symbol, agent, reward, confidence=score)
                RL_UPDATES.inc()
                # Log each RL sample to training dataset
                log_training_sample(
                    symbol=symbol,
                    agent=agent,
                    reward=reward,
                    confidence=score,
                    entry=record.entry,
                    risk=record.risk,
                    strategy=record.strategy,
                    flavor=record.flavor,
                    timestamp=record.timestamp
                )
            except Exception as e:
                FAILED_TRADE_MEMORY_DLQ.append({"agent": agent, "symbol": symbol, "error": str(e)})

    async def flush_memory(self):
        try:
            if self.buffer:
                await self.federated_client.upload(self.buffer)
                logger.info(f"[TradeMemory] Uploaded {len(self.buffer)} trades to federated memory.")
        except Exception as e:
            logger.warning(f"[TradeMemory] Federated memory upload failed: {e}")
            for rec in self.buffer:
                try:
                    self._write_to_disk(rec)
                except Exception as disk_e:
                    FAILED_TRADE_MEMORY_DLQ.append({"record": rec.dict(), "error": str(disk_e)})
        finally:
            self.buffer.clear()

    def _write_to_disk(self, record: TradeRecord):
        os.makedirs(self.artifact_path, exist_ok=True)
        path = os.path.join(self.artifact_path, "backup.jsonl")
        try:
            with open(path, "a") as f:
                f.write(record.json() + "\n")
        except Exception as e:
            logger.error(f"Failed to write trade record to disk: {e}")
            raise

    def get_dlq(self):
        return FAILED_TRADE_MEMORY_DLQ.copy()
