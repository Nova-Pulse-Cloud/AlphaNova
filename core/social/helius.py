import os
import logging
import hmac
import hashlib
from fastapi import APIRouter, FastAPI, Request, Header, HTTPException, status
from typing import List, Dict, Optional

router = APIRouter()
log = logging.getLogger("helius")

HELIUS_SECRET = os.getenv("HELIUS_WEBHOOK_SECRET")

def verify_signature(header_sig: str, body: bytes, secret: str) -> bool:
    if not header_sig or not secret:
        return False
    try:
        expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        return hmac.compare_digest(header_sig, expected)
    except Exception as e:
        log.error(f"Signature verification failed: {e}")
        return False

def parse_swap_events(payload: dict) -> List[Dict]:
    return payload.get("events", [])

@router.post("/webhook")
async def helius_webhook(
    request: Request,
    authorization: Optional[str] = Header(None),
    x_helius_signature: Optional[str] = Header(None)
):
    if not HELIUS_SECRET:
        log.error("Missing HELIUS_WEBHOOK_SECRET in environment.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server misconfigured")

    raw_body = await request.body()
    if authorization:
        if authorization != f"Bearer {HELIUS_SECRET}":
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
    elif x_helius_signature:
        if not verify_signature(x_helius_signature, raw_body, HELIUS_SECRET):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized (signature)")
    else:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized (missing header)")

    payload = await request.json()
    events = parse_swap_events(payload)
    for e in events:
        log.info(f"[HELIUS SWAP] {e}")
        # ← forward `e` into your pipeline
    return {"status": "ok", "processed": len(events)}

def register_helius_webhook(app: FastAPI, prefix: str = "/hel"):  # default prefix
    log.info(f"Mounting Helius router at {prefix}")
    app.include_router(router, prefix=prefix)
