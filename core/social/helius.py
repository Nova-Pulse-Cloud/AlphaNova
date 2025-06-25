# File: core/social/helius.py

"""
Nova Pulse ALPHA – Helius Webhook Integration
────────────────────────────────────────────
• Secure HMAC/Bearer token verification
• Parses swap events from Helius payloads
• Exposes FastAPI router for webhooks
• Provides helper to register router on an app
"""
import os
import logging
import hmac
import hashlib
from fastapi import APIRouter, FastAPI, Request, Header, HTTPException, status
from typing import List, Dict, Optional

router = APIRouter()
log = logging.getLogger("nova.helius")

HELIUS_SECRET = os.getenv("HELIUS_WEBHOOK_SECRET")


def verify_signature(header_sig: str, body: bytes, secret: str) -> bool:
    """HMAC-SHA256 verification of webhook payload."""
    if not header_sig or not secret:
        return False
    try:
        expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        return hmac.compare_digest(header_sig, expected)
    except Exception as e:
        log.error(f"Helius signature verification failed: {e}")
        return False


def parse_swap_events(payload: Dict) -> List[Dict]:
    """Extract swap events from Helius webhook payload."""
    # TODO: implement actual event filtering logic
    return [e for e in payload.get("events", []) if e.get("type") == "swap"]


@router.post("/webhook")
async def helius_webhook(
    request: Request,
    authorization: Optional[str] = Header(None),
    x_helius_signature: Optional[str] = Header(None),
):
    if not HELIUS_SECRET:
        log.error("Missing HELIUS_WEBHOOK_SECRET environment variable.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server misconfigured")

    raw_body = await request.body()
    # Bearer token takes precedence
    if authorization:
        if authorization != f"Bearer {HELIUS_SECRET}":
            log.warning("Helius webhook: invalid bearer token.")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
    elif x_helius_signature:
        if not verify_signature(x_helius_signature, raw_body, HELIUS_SECRET):
            log.warning("Helius webhook: signature mismatch.")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized - invalid signature")
    else:
        log.warning("Helius webhook: missing auth headers.")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized - missing credentials")

    try:
        payload = await request.json()
        swap_events = parse_swap_events(payload)
        for event in swap_events:
            log.info(f"[Helius Swap] {event}")
            # TODO: forward event to core processing pipeline
        return {"status": "ok", "events_processed": len(swap_events)}
    except Exception as e:
        log.error(f"Helius webhook error: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Bad request")


def register_helius_webhook(app: FastAPI, prefix: str = "/helius"):
    """
    Register the Helius webhook router on a FastAPI application.
    :param app: FastAPI instance
    :param prefix: URL prefix for webhook routes (default '/helius')
    """
    app.include_router(router, prefix=prefix)
    log.info(f"Helius webhook router registered at prefix: {prefix}")
