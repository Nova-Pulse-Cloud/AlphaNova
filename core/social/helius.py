import os
import logging
import hmac
import hashlib
from fastapi import APIRouter, Request, Header, HTTPException, status
from typing import List, Dict, Optional

router = APIRouter()
log = logging.getLogger("helius")

HELIUS_SECRET = os.getenv("HELIUS_WEBHOOK_SECRET")

def verify_signature(header_sig: str, body: bytes, secret: str) -> bool:
    """HMAC verification (optional: add to webhook security)."""
    if not header_sig or not secret:
        return False
    try:
        expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        return hmac.compare_digest(header_sig, expected)
    except Exception as e:
        log.error(f"Signature verification failed: {e}")
        return False

def parse_swap_events(payload: dict) -> List[Dict]:
    # Stub for swap event parsing — must implement to extract useful trade/swaps
    # Example: return [e for e in payload.get("events", []) if e["type"] == "swap"]
    return payload.get("events", [])  # TODO: replace with your event extraction logic

def register_helius_webhook(settings):
    """
    Registers the Helius Webhook URL.
    """
    webhook_url = settings.WEBHOOK_URL
    log.info(f"Webhook registered at {webhook_url}")
    # Perform any additional setup you need for the webhook registration here

@router.post("/webhook")
async def helius_webhook(
    request: Request,
    authorization: Optional[str] = Header(None),
    x_helius_signature: Optional[str] = Header(None)
):
    if not HELIUS_SECRET:
        log.error("Missing HELIUS_WEBHOOK_SECRET in environment.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server misconfigured")

    # Optional: Strict HMAC or Bearer token auth (choose one or both)
    raw_body = await request.body()
    if authorization:
        # Bearer token check
        if authorization != f"Bearer {HELIUS_SECRET}":
            log.warning("Unauthorized webhook attempt: invalid bearer.")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
    elif x_helius_signature:
        # HMAC signature check (if Helius provides a signature header)
        if not verify_signature(x_helius_signature, raw_body, HELIUS_SECRET):
            log.warning("Unauthorized webhook attempt: signature mismatch.")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized (signature)")

    else:
        log.warning("Unauthorized webhook attempt: missing auth.")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized (missing header)")

    try:
        payload = await request.json()
        swap_events = parse_swap_events(payload)
        for event in swap_events:
            log.info(f"[HELIUS SWAP] {event}")
            # TODO: Forward to processing pipeline (async queue, DB, or pubsub)
        return {"status": "ok", "events_processed": len(swap_events)}
    except Exception as e:
        log.error(f"[Webhook Error] {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Bad request")
