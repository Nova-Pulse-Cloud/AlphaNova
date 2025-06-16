# core/ws_endpoint.py
"""
Nova Pulse ALPHA – ELITE WebSocket Signal Endpoint
---------------------------------------------------------
• Async, robust FastAPI WebSocket manager for live signals, PnL, trades
• Handles multiple clients, broadcast, connection cleanup, error safety
• Plug-and-play for dashboards, browser, or trading clients
• Production-ready: queue overflow, client auto-clean, JSON payloads
• Optional: Auth hooks, replay, and room support (future upgrades)
"""
import asyncio
import logging
import json
from typing import Set, Dict, Any, Optional
from fastapi import WebSocket, WebSocketDisconnect, status
from starlette.websockets import WebSocketState

logger = logging.getLogger("nova.ws_endpoint")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# ELITE WebSocket Manager
# ────────────────────────────────────────────────────────────────────────────────
class WSManager:
    """
    Elite async WebSocket connection manager for live signal push & broadcast.
    - Add/remove clients, handle broken connections
    - Broadcast JSON payloads to all clients
    - Simple, production-safe, ready for dashboard plug-in
    """
    def __init__(self, max_history: int = 100):
        self.active: Set[WebSocket] = set()
        self.history: list = []
        self.max_history = max_history
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self.active.add(ws)
        logger.info(f"[WSManager] Client connected: {ws.client}")
        # On connect, send recent signal history
        for msg in self.history[-self.max_history:]:
            try:
                await ws.send_text(msg)
            except Exception:
                pass

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            self.active.discard(ws)
        logger.info(f"[WSManager] Client disconnected: {ws.client}")

    async def broadcast(self, data: Any):
        # Always JSON encode
        msg = json.dumps(data, default=str)
        # Save to history (for new clients)
        self.history.append(msg)
        if len(self.history) > self.max_history:
            self.history = self.history[-self.max_history:]
        # Send to all active clients
        drop: Set[WebSocket] = set()
        async with self._lock:
            for ws in list(self.active):
                try:
                    if ws.application_state == WebSocketState.CONNECTED:
                        await ws.send_text(msg)
                except Exception as e:
                    drop.add(ws)
                    logger.warning(f"[WSManager] Drop client {ws.client}: {e}")
            for ws in drop:
                self.active.discard(ws)

    def num_clients(self) -> int:
        return len(self.active)

    async def close_all(self):
        async with self._lock:
            for ws in list(self.active):
                try:
                    await ws.close(code=status.WS_1001_GOING_AWAY)
                except Exception:
                    pass
            self.active.clear()

# Singleton instance
ws_manager = WSManager()

# ────────────────────────────────────────────────────────────────────────────────
# WebSocket Endpoint (for FastAPI app.add_api_websocket_route)
# ────────────────────────────────────────────────────────────────────────────────
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            # Wait for any inbound (ignore/ping only)
            try:
                msg = await websocket.receive_text()
                # Optionally, handle incoming commands (auth, subscribe, etc.)
                # Example: Respond to ping
                if msg == "ping":
                    await websocket.send_text("pong")
            except Exception:
                break
    except WebSocketDisconnect:
        pass
    finally:
        await ws_manager.disconnect(websocket)

# ────────────────────────────────────────────────────────────────────────────────
# Usage (already in main_service.py):
#   from core.ws_endpoint import ws_manager, websocket_endpoint
#   app.add_api_websocket_route("/ws/signal_feed", websocket_endpoint)
#   await ws_manager.broadcast({...})
# ────────────────────────────────────────────────────────────────────────────────
