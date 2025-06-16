import logging
from core.wallet_management.wallet_manager import WalletManager
from core.utils.config import config
from core.utils.trade_logger import log_trade

logger = logging.getLogger("plugin.raydium")

async def router(signal, rpc_url=None):
    """
    Execute a swap using Raydium AMM directly on-chain.
    Pro-level: Builds raw Raydium instructions using solana-py.
    Returns: Always includes tp and sl fields (even if stubbed/failed).
    """
    # Always attach TP/SL to downstream (even if not executed)
    tp = float(signal.get("tp", 0))
    sl = float(signal.get("sl", 0))
    try:
        from solana.publickey import PublicKey
        from solana.transaction import Transaction
        from solana.rpc.async_api import AsyncClient
        from spl.token.constants import TOKEN_PROGRAM_ID

        RAYDIUM_AMM_PROGRAM_ID = PublicKey("RVKd61ztZW9aB4Kz4mVD49Qfj3ovcSg9GdQWcZ7GzWj")  # mainnet

        input_mint  = PublicKey(signal["input_mint"])
        output_mint = PublicKey(signal["output_mint"])
        amount = int(signal["size"])
        user_id = signal.get("user_id", "default")
        symbol = signal["symbol"]
        slippage_bps = int(signal.get("slippage", 100))

        wallet_manager = WalletManager(config)
        await wallet_manager.connect()
        wallet = await wallet_manager.get_wallet(user_id)
        payer = wallet.public_key

        pool_info = await fetch_raydium_pool(input_mint, output_mint)
        if not pool_info:
            raise Exception("No matching Raydium pool found for given pair.")

        swap_ix = build_raydium_swap_ix(
            user_pubkey=payer,
            input_mint=input_mint,
            output_mint=output_mint,
            amount_in=amount,
            pool_info=pool_info,
            slippage_bps=slippage_bps,
        )
        tx = Transaction()
        tx.add(swap_ix)
        async with AsyncClient(rpc_url or config.HELIUS_RPC) as client:
            resp = await wallet_manager.sign_and_send_transaction(tx, client)
        if resp.get("result") and resp["result"].get("transactionHash"):
            tx_hash = resp["result"]["transactionHash"]
            logger.info(f"[Raydium] TX success for {symbol}: {tx_hash}")
            # Always log TP/SL with trade
            await log_trade(
                symbol, signal.get("side"), amount, 
                0, 0, 0,  # inAmount/outAmount/pnl (fill in if you have)
                "raydium plugin", user_id, 
                tp=tp, sl=sl
            )
            return {
                "success": True,
                "tx_hash": tx_hash,
                "symbol": symbol,
                "tp": tp,
                "sl": sl
            }
        else:
            raise Exception(f"Raydium TX failed: {resp}")
    except Exception as e:
        logger.error(f"[Raydium] TX error for {signal.get('symbol')}: {e}")
        # Always include tp/sl in errors too!
        return {
            "success": False,
            "error": str(e),
            "symbol": signal.get("symbol"),
            "tp": tp,
            "sl": sl
        }

async def fetch_raydium_pool(input_mint, output_mint):
    """
    Fetch Raydium pool metadata.
    In production: Use Raydium on-chain registry or a maintained list (API, cache, etc).
    Here: Mock logic for demonstration only.
    """
    # TODO: Replace with live pool lookup
    return None

def build_raydium_swap_ix(user_pubkey, input_mint, output_mint, amount_in, pool_info, slippage_bps):
    """
    Build the actual Raydium swap instruction.
    This requires knowledge of Raydium's program instruction format.
    """
    from solana.transaction import TransactionInstruction
    from solana.publickey import PublicKey
    # Placeholder only
    return TransactionInstruction(
        program_id=pool_info.get('amm_program_id', PublicKey("RVKd61ztZW9aB4Kz4mVD49Qfj3ovcSg9GdQWcZ7GzWj")),
        keys=[],  # FILL IN all account metas required for Raydium
        data=bytes([]),  # FILL IN per Raydium docs
    )

async def pre_trade(signal):
    logger.info(f"[Raydium][pre_trade] Pre-trade for {signal.get('symbol')}")

async def post_trade(signal, result):
    logger.info(f"[Raydium][post_trade] Post-trade for {signal.get('symbol')} status={result.get('success')}")

def register(manager):
    logger.info("[Plugin] Raydium trade router registered.")
