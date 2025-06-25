# File: src/main_service.py
# Elite pre-launch main service wiring for Nova Pulse Alpha

import os
import glob
import logging
import asyncio
from datetime import datetime
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic_settings import BaseSettings
from prometheus_client import start_http_server, Counter, Gauge
from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Aiogram imports
from aiogram import Bot, Dispatcher
from aiogram.types import Update

# Core imports
from core.utils.config import Config
from core.wallet_management.wallet_manager import WalletManager
from core.utils.trade_memory import TradeMemory
from core.signal_generation.reinforcement_tuner import ReinforcementTuner
from core.execution.execution_router import ExecutionRouter
from core.signal_generation.signal_explainer_ai import SignalExplainerAI
from core.signal_generation.alpha_signal_agent import AlphaSignalAgent
from core.signal_generation.combined_signal_engine import CombinedSignalEngine
from core.agents.quantum_fractal_agent import QuantumFractalAgent
from core.data_collection.social_sentiment import SocialSentiment, register_sentiment_event_hook
from core.data_collection.tech_indicator_agent import TechIndicatorAgent
from core.signal_generation.sl_watchdog import start_sl_watchdog
from core.utils.plugin_manager import PluginManager as CorePluginManager, fire_plugin_event
from core.ws_endpoint import ws_manager, websocket_endpoint
from core.signal_generation.rl_trainer_loop import RLTrainerLoop
from core.execution.execute_trade import AutoTrader
from core.social.helius import register_helius_webhook
from core.utils.token_auditor import TokenAuditor
from core.utils.token_guardian import check_guardian as TokenGuardian
from core.utils.trade_logger import log_trade
from core.utils.dlq_manager import get_dlq_summary, replay_execution_dlq
from core.utils.signal_card import generate_signal_card
from core.utils.pnl_card import generate_pnl_card
from core.telegram_pusher import (
    push_signal_card,
    push_trade_card,
    push_pnl_card,
    push_trade_update,
)
from telegram.integration.telegram_router import router as telegram_router
from core.utils.token_scanner import TokenScanner


class Settings(BaseSettings):
    """
    Application settings loaded from environment or .env
    """
    WEBHOOK_URL: str
    WEBHOOK_PATH: str
    TELEGRAM_CHAT_ID: int
    TELEGRAM_BOT_TOKEN: str
    SOL_PRIVATE_KEY: str
    SOL_ADDRESS: str
    REDIS_URL: str
    HELIUS_RPC: str
    HELIUS_API_KEY: str
    CMC_API_KEY: str
    GECKO_API_URL: str
    OPENAI_API_KEY: str
    TWITTER_BEARER_TOKEN: str
    MACRO_API_URL: str
    MACRO_API_KEY: str
    ETHERSCAN_API_KEY: str
    METRICS_PORT: int = 8001
    HTTP_PORT: int = 8000
    JAEGER_HOST: str = "localhost"
    JAEGER_PORT: int = 6831
    SIGNAL_INTERVAL: int = 60  # seconds between scans

    class Config:
        env_file = ".env"


# Load settings and core config
settings = Settings()
config = Config()

# Initialize Bot and Dispatcher
bot = Bot(token=settings.TELEGRAM_BOT_TOKEN)
dp = Dispatcher(bot=bot)
dp.include_router(telegram_router)

# Configure FastAPI
app = FastAPI(title="Nova Pulse Alpha", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
FastAPIInstrumentor.instrument_app(app)
app.add_api_websocket_route("/ws/signal_feed", websocket_endpoint)

# Start Prometheus metrics server
start_http_server(settings.METRICS_PORT)
signals_counter = Counter("nova_signals_generated", "Total signals generated")
agents_gauge = Gauge("nova_agents_active", "Number of active agents")

# Core subsystem instances
token_scanner = TokenScanner(chain="solana")
wallet_manager = WalletManager(config)
trade_memory = TradeMemory(config)
rl_tuner = ReinforcementTuner()
execution_router = ExecutionRouter(
    wallet_manager=wallet_manager,
    rpc_nodes=[settings.HELIUS_RPC],
)
auditor = TokenAuditor(config.REDIS_URL)
guardian = TokenGuardian
explainer = SignalExplainerAI(config.OPENAI_API_KEY)
plugin_manager = CorePluginManager()
auto_trader = AutoTrader(
    redis_url=settings.REDIS_URL,
    router=execution_router,
    wallet_manager=wallet_manager,
    telegram_token=settings.TELEGRAM_BOT_TOKEN,
    telegram_chat_id=settings.TELEGRAM_CHAT_ID,
)

# Agent registry
AGENT_REGISTRY: dict[str, any] = {}

def register_agent(name: str, agent_obj: any) -> None:
    AGENT_REGISTRY[name] = agent_obj

# Register built-in agents
register_agent(
    "alpha_signal",
    AlphaSignalAgent(
        redis_url=settings.REDIS_URL,
        openai_key=settings.OPENAI_API_KEY,
        symbols=[],
    ),
)
register_agent("quantum_fractal", QuantumFractalAgent(config))
register_agent("social_sentiment", SocialSentiment(config))
register_agent("tech_indicators", TechIndicatorAgent(config))


async def adaptive_signal_fusion(symbol: str, agent_results: list[dict]) -> dict:
    """
    Fuse multiple agent scores with trust-based weighting and entropy penalty.
    """
    weights = {r["source"]: rl_tuner.get_trust_score(r["source"], symbol) for r in agent_results}
    total = sum(weights.values()) or 1.0
    raw_score = sum(r["score"] * weights[r["source"]] for r in agent_results) / total
    entropy_penalty = 1.0 - min(max(raw_score, 0.0), 1.0)
    fused_score = round(raw_score * (1 - entropy_penalty * 0.15), 4)
    breakdown = [{**r, "weight": weights[r["source"]]} for r in agent_results]
    return {
        "symbol": symbol,
        "score": fused_score,
        "breakdown": breakdown,
        "meta": {"fusion_strategy": "adaptive_trust_entropy", "agents": list(weights.keys())},
    }


async def load_plugins_from_dir(directory: str = "core/plugins") -> None:
    """
    Dynamically load and initialize plugins from specified directory.
    """
    mapping = {
        os.path.splitext(os.path.basename(f))[0]: f.replace(os.sep, ".")[:-3]
        for f in glob.glob(f"{directory}/*.py")
    }
    await plugin_manager.initialize(mapping)
    logging.info("🔌 Plugins loaded: %s", list(mapping.keys()))


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def setup_tracing() -> None:
    provider = TracerProvider(resource=Resource.create({"service.name": "nova-pulse"}))
    exporter = JaegerExporter(
        agent_host_name=settings.JAEGER_HOST,
        agent_port=settings.JAEGER_PORT,
    )
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    logging.info("✅ Jaeger tracing enabled on %s:%d", settings.JAEGER_HOST, settings.JAEGER_PORT)


# Startup event
@app.on_event("startup")
async def on_startup() -> None:
    setup_logging()
    setup_tracing()
    logging.info("Nova Pulse starting—metrics on port %d", settings.METRICS_PORT)

    await bot.set_webhook(settings.WEBHOOK_URL)

    # Connect subsystems
    for agent in AGENT_REGISTRY.values():
        if hasattr(agent, "connect"):
            await agent.connect()
    await token_scanner.connect()
    await auditor.connect()
    await explainer.connect()
    await auto_trader.connect()
    await execution_router.connect()

    register_sentiment_event_hook(fire_plugin_event)
    await load_plugins_from_dir()

    # Background loops
    asyncio.create_task(RLTrainerLoop(rl_tuner).start())
    asyncio.create_task(start_sl_watchdog(wallet_manager, trade_memory, execution_router))

    async def trade_loop():
        while True:
            try:
                await auto_trader.execute_next_trade()
            except Exception as exc:
                logging.error("Auto-trade error: %s", exc)
            await asyncio.sleep(settings.SIGNAL_INTERVAL)

    asyncio.create_task(trade_loop())

    async def scan_loop():
        while True:
            try:
                tokens = await token_scanner.get_all_tokens(filter_rugs=True)
                logging.info("Scanned %d tokens at %s", len(tokens), datetime.utcnow())
            except Exception as exc:
                logging.error("Scan loop error: %s", exc)
            await asyncio.sleep(settings.SIGNAL_INTERVAL)

    asyncio.create_task(scan_loop())

-    await register_helius_webhook(settings)
+    # Mount Helius webhook routes onto FastAPI
+    register_helius_webhook(app)

    logging.info("Nova Pulse LIVE with agents: %s", list(AGENT_REGISTRY.keys()))


# Shutdown event
@app.on_event("shutdown")
async def on_shutdown() -> None:
    for agent in AGENT_REGISTRY.values():
        if hasattr(agent, "disconnect"):
            await agent.disconnect()
    await token_scanner.disconnect()
    await auditor.disconnect()
    await explainer.disconnect()
    await auto_trader.disconnect()
    await execution_router.disconnect()


# Webhook endpoint
@app.post(settings.WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    payload = await request.json()
    update = Update(**payload)
    await dp.process_update(update)
    return JSONResponse({"ok": True})


# Error handler
def universal_exception_handler(request: Request, exc: Exception):
    logging.error("Unhandled error: %s", exc, exc_info=True)
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})

app.add_exception_handler(Exception, universal_exception_handler)


# Health check
@app.get("/healthz")
def health_check():
    return {"status": "ok", "agents": list(AGENT_REGISTRY.keys())}


# DLQ endpoints
@app.get("/dlq/status")
async def dlq_status():
    summary = await get_dlq_summary()
    return {"status": "ok", "summary": summary}

@app.post("/dlq/replay")
async def replay_dlq():
    results = await replay_execution_dlq(execution_router)
    return {"attempted": len(results), "results": results}


# Signal generation endpoint
@app.post("/generate_signal")
async def generate_signal(request: Request):
    payload = await request.json()
    symbol = payload.get("symbol")
    if not symbol:
        raise HTTPException(400, "Missing 'symbol' in payload")

    # Fetch from each agent
    tasks = [
        agent.analyze(symbol, **payload)
        if hasattr(agent, "analyze") else agent.fetch_sentiment({"symbol": symbol}, user=None)
        for agent in AGENT_REGISTRY.values()
    ]
    results = await asyncio.gather(*tasks)

    # Prepare agent results
    agent_results = []
    for name, res in zip(AGENT_REGISTRY.keys(), results):
        score = getattr(res, "sentiment_score", None) or getattr(res, "score", 0.0)
        entry = getattr(res, "entry", 0.0)
        agent_results.append({"source": name, "score": score, "entry": entry})
    
    signals_counter.inc()
    agents_gauge.set(len(AGENT_REGISTRY))

    # Fuse signals and broadcast
    fused = await adaptive_signal_fusion(symbol, agent_results)
    await ws_manager.broadcast(fused)
    await push_signal_card(fused)

    # Execute trade
    exec_res = await execution_router.execute_trade(fused, user_id="telegram")
    fused["execution"] = exec_res
    await push_trade_card(fused, exec_res)
    await log_trade(
        fused["symbol"], exec_res.get("side"), exec_res.get("size"),
        exec_res.get("entry"), exec_res.get("exit"), exec_res.get("pnl"),
        rationale="auto-trade fused signal", user="telegram",
    )
    await push_trade_update(fused)
    if exec_res.get("success") and exec_res.get("exit"):
        await push_pnl_card(fused["symbol"])```
