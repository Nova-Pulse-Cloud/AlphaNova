# core/utils/dlq_manager.py

import asyncio
import logging
from typing import Any, Dict, List

from core.execution.execution_router import FAILED_EXEC_DLQ
from core.utils.plugin_manager import FAILED_PLUGIN_DLQ

# ────────────────────────────────────────────────────────────────────────────────
# Logger & Metrics
# ────────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger("nova.dlq_manager")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Dead-letter queue accessors
# ────────────────────────────────────────────────────────────────────────────────
def get_all_dlq() -> Dict[str, List[Dict[str, Any]]]:
    """Get current state of execution & plugin DLQs."""
    return {
        "execution": list(FAILED_EXEC_DLQ),
        "plugins": list(FAILED_PLUGIN_DLQ),
    }


def get_dlq_summary() -> Dict[str, int]:
    """Count of failed items in each DLQ."""
    return {
        "execution_count": len(FAILED_EXEC_DLQ),
        "plugin_count": len(FAILED_PLUGIN_DLQ),
    }

# ────────────────────────────────────────────────────────────────────────────────
# DLQ Replay
# ────────────────────────────────────────────────────────────────────────────────
async def replay_execution_dlq(execution_router) -> List[Dict[str, Any]]:
    """
    Replay all failed execution tasks via the provided ExecutionRouter.
    Returns a list of successful replay results.
    """
    results: List[Dict[str, Any]] = []
    tasks = [
        _process_failed_execution(entry, execution_router)
        for entry in list(FAILED_EXEC_DLQ)
    ]

    outcomes = await asyncio.gather(*tasks, return_exceptions=True)

    # Evaluate outcomes and clean up DLQ
    for entry, outcome in zip(list(FAILED_EXEC_DLQ), outcomes):
        if isinstance(outcome, Exception):
            logger.error(f"[DLQ Replay] Error replaying entry {entry}: {outcome}")
        elif outcome.get("success"):
            FAILED_EXEC_DLQ.remove(entry)
            results.append(outcome)
        else:
            logger.warning(f"[DLQ Replay] Execution still failing for entry {entry}")

    logger.info(f"[DLQ Replay] Completed. Successes: {len(results)}")
    return results


async def _process_failed_execution(
    failed: Dict[str, Any], execution_router
) -> Dict[str, Any]:
    """
    Attempt to re-execute a single failed execution entry.
    Raises on missing signal data.
    """
    signal = failed.get("signal")
    user_id = failed.get("user_id")
    if not signal:
        raise ValueError(f"Missing 'signal' in DLQ entry: {failed}")

    logger.info(f"[DLQ Replay] Replaying signal for {signal.get('symbol')}")
    return await execution_router.execute_trade(signal, user_id=user_id)

async def replay_plugin_dlq(plugin_manager) -> List[Any]:
    """
    Placeholder for future plugin DLQ replay, using the provided PluginManager.
    """
    results = []
    for entry in list(FAILED_PLUGIN_DLQ):
        try:
            res = await plugin_manager.process_failed_plugin(entry)
            if res.get("success"):
                FAILED_PLUGIN_DLQ.remove(entry)
                results.append(res)
        except Exception as e:
            logger.error(f"[DLQ Replay] Plugin replay error for {entry}: {e}")
    logger.info(f"[DLQ Replay] Plugin DLQ replay completed. Successes: {len(results)}")
    return results
