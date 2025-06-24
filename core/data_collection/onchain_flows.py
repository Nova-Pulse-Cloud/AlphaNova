async def token_flows(self, symbol: str, wallet: str, *, limit: int = 100) -> Dict[str, Any]:
    """Aggregate inflow/outflow & whale hits for `symbol` at `wallet`."""
    txs = await self.fetch_transactions(wallet, limit=limit)
    inflow = outflow = 0.0
    whales: List[Dict[str, Any]] = []

    for tx in txs:
        meta = tx.get("meta") or {}
        pre = meta.get("preBalances", [0])[0] / 1e9
        post = meta.get("postBalances", [0])[0] / 1e9
        delta = post - pre
        if abs(delta) < 1e-9:
            continue
        if delta > 0:
            inflow += delta
        else:
            outflow += abs(delta)
        if abs(delta) >= self.whale_thresh:
            OCF_WHALE_EVT.inc()
            whales.append({
                "sig": tx.get("transaction", {}).get("signatures", ["?"])[0],
                "amount": delta,
            })

    analysis = {
        "symbol":       symbol,
        "wallet":       wallet,
        "inflow":       inflow,
        "outflow":      outflow,
        "whale_events": whales,
        "timestamp":    int(datetime.utcnow().timestamp()),
        "audit_hash":   _hash((symbol, wallet, inflow, outflow, len(whales))),
    }

    await fire_plugin_event("onchain_flow", analysis)
    return analysis
