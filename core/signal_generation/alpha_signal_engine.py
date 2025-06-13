import os
import logging
import asyncio
from datetime import datetime
import numpy as np
from solana.rpc.asyncapi import AsyncClient
from solana.keypair import Keypair
from solana.transaction import Transaction
from solana.system_program import TransferParams, transfer

from prometheus_client import Counter, Histogram, Gauge

# Your other imports...
from core.market_conditions import MarketConditionsService
from core.social_sentiment import score_token_sentiment, get_live_narrative
from core.whale_detector import get_recent_whale_activity
from core.onchain_flows import get_token_flow_stats
from core.orderbook_overlay import get_orderbook_imbalance  # OrderBookOverlay import
from core.token_auditor import get_token_rug_score
from core.alpha_memory_mesh import update_memory, get_token_memory_score
from core.reinforcement_learning import DQNAgent
from fills_db import get_latest_fill_result  # RL feedback
from openai import AsyncOpenAI

logger = logging.getLogger("nova.alpha_signal_engine")
logger.setLevel(logging.INFO)

REDIS_URL = os.getenv("REDIS_URL")
GECKO_API_URL = os.getenv("GECKO_API_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SOL_PRIVATE_KEY_PATH = os.getenv("SOL_PRIVATE_KEY_PATH", "/etc/secrets/solana_private_key.json")
SOL_ADDRESS = os.getenv("SOL_ADDRESS")

# Solana Client
solana_client = AsyncClient("https://api.mainnet-beta.solana.com")

# Metrics
SIGNALS_GENERATED = Counter("signal_engine_signals_total", "Alpha signals generated")
SIGNAL_ERRORS = Counter("signal_engine_errors_total", "Alpha signal generation errors")
OVERLAY_LATENCY = Histogram("signal_engine_overlay_latency_seconds", "Signal overlay fetch latency", ["overlay"])
ACTION_GAUGE = Gauge("signal_engine_action_gauge", "RL action output", ["action"])

# Overlay plugins registry
overlay_plugins = {}

def register_overlay(name, coro):
    overlay_plugins[name] = coro

event_hooks = []
def register_event_hook(hook_fn):
    event_hooks.append(hook_fn)

async def fire_event(event_name, data):
    for hook in event_hooks:
        try:
            await hook(event_name, data)
        except Exception as e:
            logger.error(f"[Nova] Event hook failure [{hook.__name__}]: {e}")

# Read private key from Render secrets
def load_private_key():
    try:
        with open(SOL_PRIVATE_KEY_PATH, 'r') as keyfile:
            key_data = keyfile.read()  # Read private key in hex format
        private_key = bytes.fromhex(key_data.strip())  # Convert hex string to bytes
        keypair = Keypair.from_secret_key(private_key)  # Create a Keypair object
        return keypair
    except Exception as e:
        logger.error(f"Failed to load Solana private key from {SOL_PRIVATE_KEY_PATH}: {e}")
        raise

# ----------------------------
#   Auto-Trade Execution Logic
# ----------------------------

async def execute_trade(symbol, action, user=None):
    """
    Executes a trade on the Solana blockchain using Phantom wallet.
    This function places a transfer order using Solana’s AsyncClient and signs it with the user's private key.
    """
    try:
        # Load the wallet from the private key
        keypair = load_private_key()

        # Define the trade amount and transaction details
        trade_amount = 0.1  # Example trade amount (adjust as needed)
        recipient_address = SOL_ADDRESS  # The address to send Solana to (this can be dynamic)
        
        # Prepare the transaction
        transaction = Transaction()
        transaction.add(
            transfer(
                TransferParams(
                    from_pubkey=keypair.public_key,
                    to_pubkey=recipient_address,
                    lamports=int(trade_amount * 10**9)  # Convert SOL to lamports
                )
            )
        )

        # Send the transaction
        response = await solana_client.send_transaction(transaction, keypair)
        logger.info(f"Trade executed for {symbol}: {response}")

        # Emit PNL Card after trade
        pnl = calculate_pnl(symbol, action)
        emit_pnl_card(symbol, pnl, response, user=user)

        return response
    except Exception as e:
        logger.error(f"Trade execution failed for {symbol}: {e}")
        return {"status": "failed", "symbol": symbol, "action": action, "error": str(e)}

# ----------------------------
#   PNL Card Generator
# ----------------------------

def emit_pnl_card(symbol, pnl, trade_details, user=None):
    """
    Emits a PNL card for each trade. You must connect this to your trade logic.
    """
    card = {
        "symbol": symbol,
        "pnl": pnl,
        "trade_details": trade_details,
        "user": user,
        "timestamp": datetime.utcnow().isoformat()
    }
    # You can fire a real event, send to Telegram, or save to DB as needed.
    asyncio.create_task(fire_event("pnl_card", card))
    logger.info(f"[Nova][PNL CARD] {card}")

def calculate_pnl(symbol, action):
    """
    A simple placeholder PNL calculation based on action.
    Replace with real logic depending on your platform.
    """
    if action == "buy":
        return 0.05  # Placeholder PNL value for a buy action
    elif action == "sell":
        return 0.10  # Placeholder PNL value for a sell action
    return 0.0

# ----------------------------
#      Main Signal Generator
# ----------------------------

async def generate_signal(symbol, user=None):
    if not is_signals_enabled(user):
        logger.info(f"[Nova] Signals are currently disabled. No signal generated for {symbol}.")
        return {"symbol": symbol, "error": "Signals are OFF", "timestamp": datetime.utcnow().isoformat()}

    try:
        overlay_results = {}
        for name, fn in overlay_plugins.items():
            overlay_results[name] = await fetch_overlay_with_retry(name, fn, symbol, user)

        # Adjust signal scores based on order book imbalance
        orderbook_imbalance = overlay_results.get("orderbook", 0)
        for key in overlay_results:
            overlay_results[key] *= (1 + orderbook_imbalance)  # Enhance signals based on imbalance

        state_vec = build_state_vector(overlay_results)
        latest_fill = get_latest_fill_result(symbol, user)
        action, q_values = agent.act(state_vec, feedback=latest_fill)
        ACTION_GAUGE.labels(action=str(action)).set(q_values[action])
        SIGNALS_GENERATED.inc()

        update_memory(symbol, overlays=overlay_results, action=action, user=user)

        explainer = await generate_explainer(overlay_results, action, symbol)
        signal_data = {
            "symbol": symbol,
            "action": action,
            "overlays": overlay_results,
            "explainer": explainer,
            "q_values": q_values,
            "timestamp": datetime.utcnow().isoformat()
        }
        await fire_event("signal_generated", signal_data)

        # If auto-trade is enabled, execute the trade
        if is_auto_trade_enabled(user):
            logger.info(f"[Nova] Auto-trade enabled for {symbol}. Executing trade...")
            trade_result = await execute_trade(symbol, action, user=user)
            return trade_result

        return signal_data

    except Exception as e:
        SIGNAL_ERRORS.inc()
        logger.error(f"Signal generation failed for {symbol}: {e}")
        return {"symbol": symbol, "error": str(e), "timestamp": datetime.utcnow().isoformat()}
