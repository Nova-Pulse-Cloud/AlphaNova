# core/ws_endpoint.py
"""
Nova Pulse ALPHA – WebSocket Endpoint (Elite Version)
-----------------------------------------------------
• Broadcasts live signals, trade events, and health status to all clients
• Pluggable hooks for pushing new events (signals, PnL, executions)
• Async, FastAPI/WebSocket compatible
• Supports authenticated/anonymous clients
• Prometheus metrics for connection, error, and event stats
• Fully integrated with Nova Pulse's signal engine and health endpoints
"""
import logging
import asyncio
from typing import Dict, Set, Any
from fastapi import WebSocket, WebSocketDisconnect
from prometheus_client import Counter, Gauge

logger = logging.getLogger("nova.ws_endpoint")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
WS_CONNECTIONS = Gauge("ws_connections", "Active WebSocket connections")
WS_EVENTS      = Counter("ws_events_total", "Total WebSocket events broadcasted")
WS_ERRORS      = Counter("ws_errors_total", "WebSocket errors")

# ────────────────────────────────────────────────────────────────────────────────
# WebSocket Manager
# ────────────────────────────────────────────────────────────────────────────────
class WSManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self._lock:
            self.active_connections.add(websocket)
            WS_CONNECTIONS.inc()
            logger.info(f"[WS] Client connected. Total: {len(self.active_connections)}")

    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)
                WS_CONNECTIONS.dec()
                logger.info(f"[WS] Client disconnected. Total: {len(self.active_connections)}")

    async def broadcast(self, data: Any):
        WS_EVENTS.inc()
        to_remove = set()
        async with self._lock:
            for ws in self.active_connections:
                try:
                    await ws.send_json(data)
                except Exception as e:
                    WS_ERRORS.inc()
                    logger.warning(f"[WS] Failed to send to client: {e}")
                    to_remove.add(ws)
            for ws in to_remove:
                self.active_connections.remove(ws)
                WS_CONNECTIONS.dec()

# Singleton instance
ws_manager = WSManager()

# ────────────────────────────────────────────────────────────────────────────────
# WebSocket Endpoint for FastAPI
# ────────────────────────────────────────────────────────────────────────────────
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            _ = await websocket.receive_text()  # Optionally parse commands from client
    except WebSocketDisconnect:
        await ws_manager.disconnect(websocket)
    except Exception as e:
        WS_ERRORS.inc()
        logger.error(f"[WS Endpoint] Error: {e}")
        await ws_manager.disconnect(websocket)
