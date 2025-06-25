import logging
from typing import Any, Dict, Optional

from solana.publickey import PublicKey
from solana.transaction import Transaction, TransactionInstruction
from solana.rpc.async_api import AsyncClient

from core.wallet_management.wallet_manager import WalletManager
from core.utils.config import config
from core.utils.trade_logger import log_trade

logger = logging.getLogger("plugin.raydium")
logger.setLevel(logging.INFO)


async def pre_trade(signal: Dict[str, Any]) -> None:
    """Hook called before executing a trade."""
    symbol = signal.get("symbol", "<unknown>")
    logger.info(f"[Raydium][pre_trade] Preparing trade for {symbol}")


async def post_trade(signal: Dict[str, Any], result: Dict[str, Any]) -> None:
    """Hook called after executing a trade."""
    symbol = signal.get("symbol", "<unknown>")
    status = "succeeded" if result.get("success") else "failed"
    logger.info(f"[Raydium][post_trade] Trade {status} for {symbol}")


async def router(signal: Dict[str, Any], rpc_url: Optional[str] = None) -> Dict[str, Any]:
    """
    Execute a swap via Raydium AMM on Solana.

    Returns a result dict that always includes 'tp' and 'sl'.
    """
    tp = float(signal.get("tp", 0))
    sl = float(signal.get("sl", 0))
    symbol = signal.get("symbol", "<unknown>")

    try:
        # Parse inputs
        input_mint = PublicKey(signal["input_mint"])
        output_mint = PublicKey(signal["output_mint"])
        amount_in = int(signal.get("size", 0))
        slippage_bps = int(signal.get("slippage", 100))
        user_id = signal.get("user_id", "default")

        # Connect wallet
        wallet_manager = WalletManager(config)
        await wallet_manager.connect()
        wallet = await wallet_manager.get_wallet(user_id)
        payer = wallet.public_key

        # Discover pool
        pool_info = await fetch_raydium_pool(input_mint, output_mint)
        if pool_info is None:
            raise RuntimeError("No Raydium pool found for given pair")

        # Build transaction
        swap_ix = build_raydium_swap_ix(
            user_pubkey=payer,
            input_mint=input_mint,
            output_mint=output_mint,
            amount_in=amount_in,
            pool_info=pool_info,
            slippage_bps=slippage_bps,
        )
        tx = Transaction().add(swap_ix)

        # Send
        rpc = rpc_url or config.HELIUS_RPC
        async with AsyncClient(rpc) as client:
            response = await wallet_manager.sign_and_send_transaction(tx, client)

        # Evaluate
        result = {
            "symbol": symbol,
            "tp": tp,
            "sl": sl,
        }
        tx_hash = response.get("result", {}).get("transactionHash")
        if tx_hash:
            result.update({"success": True, "tx_hash": tx_hash})
            logger.info(f"[Raydium] Trade success for {symbol}: {tx_hash}")
        else:
            raise RuntimeError(f"Transaction failed: {response}")

        # Log trade (TP/SL always recorded)
        await log_trade(
            symbol=symbol,
            side=signal.get("side"),
            size=amount_in,
            in_amount=0,
            out_amount=0,
            pnl=0,
            source="raydium",
            user_id=user_id,
            tp=tp,
            sl=sl,
        )
        return result

    except Exception as err:
        logger.error(f"[Raydium] Error executing trade for {symbol}: {err}")
        return {"success": False, "error": str(err), "symbol": symbol, "tp": tp, "sl": sl}


async def fetch_raydium_pool(
    input_mint: PublicKey,
    output_mint: PublicKey
) -> Optional[Dict[str, Any]]:
    """
    Retrieve Raydium pool metadata for the given mint pair.
    TODO: implement on-chain registry lookup or API cache.
    """
    # Placeholder: no pool available
    return None


def build_raydium_swap_ix(
    user_pubkey: PublicKey,
    input_mint: PublicKey,
    output_mint: PublicKey,
    amount_in: int,
    pool_info: Dict[str, Any],
    slippage_bps: int,
) -> TransactionInstruction:
    """
    Construct the Raydium swap instruction.
    This must conform to Raydium's on-chain program spec.
    """
    program_id = pool_info.get(
        "amm_program_id",
        PublicKey("RVKd61ztZW9aB4Kz4mVD49Qfj3ovcSg9GdQWcZ7GzWj")
    )
    # TODO: assemble account metas and data payload per spec
    return TransactionInstruction(program_id=program_id, keys=[], data=b"")


def register(manager: Any) -> None:
    """Register pre- and post-trade hooks with the trading manager."""
    manager.register_pre_hook(pre_trade)
    manager.register_post_hook(post_trade)
    manager.register_router(router)
    logger.info("[Plugin] Raydium router registered")
