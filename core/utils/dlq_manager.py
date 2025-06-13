import logging
from typing import List, Dict
import asyncio

from core.execution_router import FAILED_EXEC_DLQ
from core.plugin_manager import FAILED_PLUGIN_DLQ

logger = logging.getLogger("dlq_manager")

# === DLQ Inspector ===
def get_all_dlq() -> Dict[str, List[Dict]]:
    """Returns the current state of all failed execution and plugin DLQs."""
    return {
        "execution": FAILED_EXEC_DLQ.copy(),
        "plugins": FAILED_PLUGIN_DLQ.copy()
    }

def get_dlq_summary() -> Dict[str, int]:
    """Returns a summary of the count of failed executions and plugins."""
    return {
        "execution_count": len(FAILED_EXEC_DLQ),
        "plugin_count": len(FAILED_PLUGIN_DLQ),
    }

# === DLQ Replay ===
async def replay_execution_dlq(execution_router) -> List[Dict]:
    """Attempts to replay all failed execution tasks from the DLQ asynchronously."""
    results = []
    errors = []
    to_remove = []  # To avoid modifying the list while iterating

    # Create tasks for each failed item in the DLQ for concurrent execution
    tasks = []
    for failed in FAILED_EXEC_DLQ:
        tasks.append(process_failed_task(failed, execution_router, results, to_remove, errors))
    
    # Wait for all tasks to complete
    await asyncio.gather(*tasks)
    
    # After all tasks, remove successfully processed items from the DLQ
    for item in to_remove:
        FAILED_EXEC_DLQ.remove(item)

    if errors:
        logger.error(f"[DLQ Replay] Errors occurred during replay: {errors}")
    else:
        logger.info(f"[DLQ Replay] Successfully processed all failed executions.")
    
    return results

async def process_failed_task(failed, execution_router, results, to_remove, errors):
    """Processes a single failed task."""
    try:
        signal = failed.get("signal")
        user_id = failed.get("user_id")
        if signal:
            logger.info(f"[DLQ Replay] Attempting to recover execution for signal: {signal.get('symbol')}")
            result = await execution_router.execute_trade(signal, user_id)
            if result.get("success"):
                to_remove.append(failed)
                logger.info(f"[DLQ Replay] Recovered execution for signal: {signal.get('symbol')}")
                results.append(result)
            else:
                logger.warning(f"[DLQ Replay] Failed to recover execution for signal: {signal.get('symbol')}")
        else:
            logger.warning(f"[DLQ Replay] Missing signal for failed execution: {failed}")
    except Exception as e:
        error_message = f"[DLQ Replay] Failed to replay trade for signal: {failed.get('signal', 'N/A')}. Error: {e}"
        logger.error(error_message)
        errors.append(error_message)

# === Future: Replay plugins if plugin DLQ ever becomes actionable ===
