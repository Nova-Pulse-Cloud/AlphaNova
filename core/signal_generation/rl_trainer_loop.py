import asyncio
import logging
from datetime import datetime
import uuid
from prometheus_client import Counter, Histogram
import time
import random

# Prometheus metrics
RL_TRAINER_RUNS = Counter("rl_trainer_runs_total", "Total RL trainer loop runs")
RL_TRAINER_ERRORS = Counter("rl_trainer_errors_total", "RL trainer loop errors")
RL_TRAINER_LATENCY = Histogram("rl_trainer_latency_seconds", "RL trainer loop latency")
RL_TRAINER_EPOCHS = Counter("rl_trainer_epochs_total", "Total RL epochs completed")
RL_TRAINER_RETRIES = Counter("rl_trainer_retries_total", "Total RL trainer loop retries")

log = logging.getLogger("rl_trainer_loop")

class RLTrainerLoop:
    def __init__(self, rl_tuner, interval_sec: int = 900, max_retries: int = 3):
        """
        RLTrainerLoop: Schedules and manages the RL training process.
        - rl_tuner: The RL tuner that handles the actual training logic.
        - interval_sec: Time between each training cycle.
        - max_retries: Maximum retries for failed training cycles.
        """
        self.rl_tuner = rl_tuner
        self.interval = interval_sec
        self.max_retries = max_retries
        self._running = True

    async def start(self):
        """Start the RL training loop."""
        log.info("🔁 RL Trainer Loop started")
        while self._running:
            try:
                await self._train_cycle()
            except Exception as e:
                RL_TRAINER_ERRORS.inc()
                log.error(f"RL Trainer loop error: {e}", exc_info=True)
            await asyncio.sleep(self.interval)

    async def _train_cycle(self):
        """
        Executes a single training cycle with retry logic.
        """
        RL_TRAINER_RUNS.inc()
        trace_id = f"rl-train-{uuid.uuid4().hex[:8]}"
        timestamp = datetime.utcnow().isoformat()
        log.info(f"🚀 RL Trainer cycle started [{trace_id}] at {timestamp}")

        try:
            async with RL_TRAINER_LATENCY.time():
                # Retry logic with exponential backoff for robustness
                retries = 0
                success = False
                while retries < self.max_retries and not success:
                    try:
                        result = await self.rl_tuner.run_epoch(trace_id=trace_id, timestamp=timestamp)
                        success = True
                        RL_TRAINER_EPOCHS.inc()
                        log.info(f"✅ RL update complete [{trace_id}]: {result}")
                    except Exception as e:
                        retries += 1
                        RL_TRAINER_RETRIES.inc()
                        wait_time = random.uniform(2**retries, 2**(retries+1))  # Exponential backoff
                        log.warning(f"RL train cycle failed [{trace_id}], retrying... Attempt {retries}/{self.max_retries}. Error: {e}")
                        await asyncio.sleep(wait_time)
                if not success:
                    raise RuntimeError(f"RL Trainer cycle failed after {self.max_retries} retries")
        except Exception as e:
            RL_TRAINER_ERRORS.inc()
            log.error(f"RL train cycle failed [{trace_id}]: {e}", exc_info=True)

    def stop(self):
        """Stop the RL training loop."""
        self._running = False
        log.info("RL Trainer Loop stopped")
