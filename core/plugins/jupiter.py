# core/plugins/jupiter.py
import logging
from core.wallet_management.wallet_manager import WalletManager
from core.execution.tx_builder import build_compressed_swap
from core.utils.config import config
from core.utils.trade_logger import log_trade

logger = logging.getLogger("plugin.jupiter")

async def router(signal, rpc_url=None):
    """
    Execute a swap directly via Jupiter route.
    This version guarantees dynamic TP/SL fields are always passed through,
    recorded, and returned (for full elite audit & analytics compliance).
    """
    wallet_manager = WalletManager(config)
    await wallet_manager.connect()
    wallet = await wallet_manager.get_wallet(signal.get("user_id", "default"))
    input_mint = signal["input_mint"]
    output_mint = signal["output_mint"]
    amount = int(signal["size"])
    slippage_bps = int(signal.get("slippage", 100))
    symbol = signal["symbol"]
    tp = signal.get("tp")
    sl = signal.get("sl")
    try:
        tx_meta = await build_compressed_swap(
            wallet=wallet,
            input_mint=input_mint,
            output_mint=output_mint,
            amount=amount,
            slippage_bps=slippage_bps,
            symbol=symbol,
            rpc_url=rpc_url or config.HELIUS_RPC
        )
        logger.info(f"[Jupiter] TX success for {symbol}: {tx_meta['hash']}")
        await log_trade(
            symbol,
            signal.get("side"),
            amount,
            tx_meta.get("route", {}).get("inAmount"),
            tx_meta.get("route", {}).get("outAmount"),
            0,  # PnL—set when position closed
            "jupiter plugin",
            signal.get("user_id"),
            tp=tp,
            sl=sl,
            reason="executed",
        )
        return {
            "success": True,
            "tx_meta": tx_meta,
            "hash": tx_meta['hash'],
            "symbol": symbol,
            "tp": tp,
            "sl": sl,
        }
    except Exception as e:
        logger.error(f"[Jupiter] TX error for {symbol}: {e}")
        return {"success": False, "error": str(e), "symbol": symbol, "tp": tp, "sl": sl}

async def pre_trade(signal):
    logger.info(f"[Jupiter][pre_trade] Pre-trade for {signal.get('symbol')}")

async def post_trade(signal, result):
    logger.info(f"[Jupiter][post_trade] Post-trade for {signal.get('symbol')} status={result.get('success')}")

def register(manager):
    logger.info("[Plugin] Jupiter trade router registered.")
