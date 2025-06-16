# core/alpha_memory_mesh.py
"""
Nova Pulse ALPHA – Alpha Memory Mesh (Elite Trade Meta-Learning Layer)
────────────────────────────────────────────────────────────────────────
• Tracks advanced memory states for signals, trades, and token patterns
• Used for RL and trade explainer AI modules
• Supports elite risk overlays, regret/prune, and pattern scoring
• 100% compatible with the rest of the Nova Pulse system
"""
import logging
import threading
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger("nova.alpha_memory_mesh")
logger.setLevel(logging.INFO)

class MemoryMesh:
    """
    Centralized memory mesh for signals, trades, and reinforcement meta-patterns.
    Used by trade explainer, RL agent, and advanced TP/SL overlays.
    """
    def __init__(self):
        self._lock = threading.RLock()
        self._memory: Dict[str, List[Dict[str, Any]]] = {}

    def log_trade(self, trade: Dict[str, Any], signal: Dict[str, Any]) -> None:
        symbol = trade.get("symbol")
        with self._lock:
            record = {
                "timestamp": int(time.time()),
                "trade": trade,
                "signal": signal,
                "regret": trade.get("regret", 0.0),
                "rr_ratio": trade.get("rr_ratio", None),
                "reason": trade.get("reason", "")
            }
            self._memory.setdefault(symbol, []).append(record)
            logger.info(f"[AlphaMesh] Trade log for {symbol}: {record}")

    def get_token_stats(self, symbol: str) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._memory.get(symbol, []))

    def prune_regret(self, symbol: str, cap: float = 0.12) -> None:
        """
        Remove all trades/signals for a symbol with regret above threshold.
        """
        with self._lock:
            records = self._memory.get(symbol, [])
            kept = [r for r in records if abs(r.get("regret", 0.0)) < cap]
            removed = len(records) - len(kept)
            self._memory[symbol] = kept
            if removed:
                logger.info(f"[AlphaMesh] Pruned {removed} high-regret signals for {symbol}")

    def get_regret_curve(self, symbol: str) -> List[float]:
        with self._lock:
            return [r.get("regret", 0.0) for r in self._memory.get(symbol, [])]

    def to_json(self) -> Dict[str, Any]:
        with self._lock:
            return {k: v for k, v in self._memory.items()}
