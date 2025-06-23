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

# === Settings ===
class Settings(BaseSettings):
    WEBHOOK_URL: str        # https://your.domain.com/telegram/webhook
    WEBHOOK_PATH: str       # /telegram/webhook
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
    SIGNAL_INTERVAL: int = 60    # seconds between scans

    class Config:
        env_file = ".env"

# Initialize settings and config
settings = Settings()
config = Config()

# Bot & Dispatcher
bot = Bot(token=settings.TELEGRAM_BOT_TOKEN)
dp  = Dispatcher(bot=bot)
dp.include_router(telegram_router)

# FastAPI app setup
app = FastAPI(title="Nova Pulse Alpha", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
FastAPIInstrumentor.instrument_app(app)
app.add_api_websocket_route("/ws/signal_feed", websocket_endpoint)

# Prometheus metrics
start_http_server(settings.METRICS_PORT)
signals_counter = Counter("nova_signals_generated", "Signals generated")
agents_gauge    = Gauge("nova_agents_active", "Active agents in mesh")

# === Core subsystems ===
wallet_manager   = WalletManager(config)
trade_memory     = TradeMemory(config)
rl_tuner         = ReinforcementTuner()
execution_router = ExecutionRouter(wallet_manager=wallet_manager, rpc_nodes=[settings.HELIUS_RPC])
auditor          = TokenAuditor(config.REDIS_URL)
guardian         = TokenGuardian
explainer        = SignalExplainerAI(config.OPENAI_API_KEY)
plugin_manager   = CorePluginManager()
auto_trader      = AutoTrader(
    redis_url=settings.REDIS_URL,
    router=execution_router,
    wallet_manager=wallet_manager,
    telegram_token=settings.TELEGRAM_BOT_TOKEN,
    telegram_chat_id=settings.TELEGRAM_CHAT_ID,
)
scanner          = TokenScanner(chain="solana")

# Register agents
AGENT_REGISTRY = {}
def register_agent(name: str, agent_obj):
    AGENT_REGISTRY[name] = agent_obj

register_agent("alpha_signal", AlphaSignalAgent(redis_url=settings.REDIS_URL, openai_key=settings.OPENAI_API_KEY, symbols=[]))
register_agent("quantum_fractal", QuantumFractalAgent(config))
register_agent("social_sentiment", SocialSentiment(config))
register_agent("tech_indicators", TechIndicatorAgent(config))

async def adaptive_signal_fusion(symbol: str, agent_results: list[dict]) -> dict:
    weights = {res["source"]: rl_tuner.get_trust_score(res["source"], symbol) for res in agent_results}
    total = sum(weights.values()) or 1.0
    score = sum(res["score"] * weights[res["source"]] for res in agent_results) / total
    entropy_penalty = 1.0 - min(max(score, 0), 1)
    adjusted = round(score * (1 - entropy_penalty * 0.15), 4)
    return {"symbol": symbol, "score": adjusted, "breakdown": [{**res, "weight": weights[res["source"]]} for res in agent_results], "meta": {"fusion":"adaptive_trust_entropy","agents":list(weights.keys())}}

async def load_plugins_from_dir(directory: str = "core/plugins"):
    mapping = {os.path.splitext(os.path.basename(f))[0]: f.replace("/", ".")[:-3] for f in glob.glob(f"{directory}/*.py")}
    await plugin_manager.initialize(mapping)
    logging.info("🔌 Loaded plugins: %s", list(mapping.keys()))

def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

def setup_tracing():
    exporter = JaegerExporter(agent_host_name=settings.JAEGER_HOST, agent_port=settings.JAEGER_PORT)
    provider = TracerProvider(resource=Resource.create({"service.name":"nova-pulse"}))
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    logging.info("✅ Jaeger tracing on %s:%s", settings.JAEGER_HOST, settings.JAEGER_PORT)

# ─ Startup & Shutdown ─
@app.on_event("startup")
async def on_startup():
    setup_logging()
    setup_tracing()
    logging.info("Prometheus on port %s", settings.METRICS_PORT)

    await bot.set_webhook(settings.WEBHOOK_URL)

    for agent in AGENT_REGISTRY.values():
        if hasattr(agent, "connect"):
            await agent.connect()
    await auditor.connect()
    await explainer.connect()
    await auto_trader.connect()
    await execution_router.connect()
    await scanner.connect()

    register_sentiment_event_hook(fire_plugin_event)
    await load_plugins_from_dir()
    asyncio.create_task(RLTrainerLoop(rl_tuner).start())
    asyncio.create_task(start_sl_watchdog(wallet_manager, trade_memory, execution_router))

    async def trade_loop():
        while True:
            try:
                await auto_trader.execute_next_trade()
            except Exception as e:
                logging.error("Auto-trade error: %s", e)
            await asyncio.sleep(60)
    asyncio.create_task(trade_loop())

    async def scan_loop():
        while True:
            try:
                tokens = await scanner.get_all_tokens(filter_rugs=True)
                for ts in tokens:
                    symbol = ts.symbol
                    # reuse internal generate_signal logic
                    r = await app.router.default_mount.get_route("/generate_signal").endpoint(Request({"symbol":symbol}))
                    # above is conceptual; you can call generate_signal logic directly here
                logging.info(f"Scanned and generated signals for {len(tokens)} tokens at {datetime.utcnow()}")
            except Exception as e:
                logging.error("Scan loop error: %s", e)
            await asyncio.sleep(settings.SIGNAL_INTERVAL)
    asyncio.create_task(scan_loop())

    await register_helius_webhook(settings)
    logging.info("Nova Pulse LIVE: %s", list(AGENT_REGISTRY.keys()))

@app.on_event("shutdown")
async def on_shutdown():
    for agent in AGENT_REGISTRY.values():
        if hasattr(agent, "disconnect"):
            await agent.disconnect()
    await auditor.disconnect()
    await explainer.disconnect()
    await auto_trader.disconnect()
    await execution_router.disconnect()
    await scanner.disconnect()

@app.post(settings.WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    data = await request.json()
    update = Update(**data)
    await dp.process_update(update)
    return JSONResponse(content={"ok": True})

@app.exception_handler(Exception)
async def universal_exception_handler(request: Request, exc: Exception):
    logging.error("Unhandled error: %s", exc, exc_info=True)
    return JSONResponse(status_code=500, content={"detail":"Internal error occurred."})

@app.get("/healthz")
def health_check():
    return {"status":"ok","agents":list(AGENT_REGISTRY.keys())}

@app.get("/dlq/status")
async def dlq_status():
    return {"status":"ok","summary": await get_dlq_summary()}

@app.post("/dlq/replay")
async def replay_dlq():
    results = await replay_execution_dlq(execution_router)
    return {"replay_attempted": len(results), "details": results}

@app.post("/generate_signal")
async def generate_signal(request: Request):
    payload = await request.json()
    symbol = payload.get("symbol")
    if not symbol:
        raise HTTPException(status_code=400, detail="Missing 'symbol'.")
    tasks = [agent.analyze(symbol, **payload) if hasattr(agent, "analyze") else agent.fetch_sentiment({"symbol": symbol}, user=None) for agent in AGENT_REGISTRY.values()]
    results = await asyncio.gather(*tasks)
    agent_results = [{"source":name,"score":getattr(res,"sentiment_score",None) or getattr(res,"score",0.0),"meta":{},"entry":getattr(res,"entry",0.0)} for name,res in zip(AGENT_REGISTRY.keys(),results)]
    signals_counter.inc()
    agents_gauge.set(len(AGENT_REGISTRY))
    fused = await adaptive_signal_fusion(symbol, agent_results)
    await ws_manager.broadcast(fused)
    await push_signal_card(fused)
    exec_res = await execution_router.execute_trade(fused, user_id="telegram")
    fused["execution"] = exec_res
    await push_trade_card(fused, exec_res)
    await log_trade(fused["symbol"],exec_res.get("side"),exec_res.get("size"),exec_res.get("entry"),exec_res.get("exit"),exec_res.get("pnl"),rationale="auto-trade fused signal",user="telegram")
    await push_trade_update(fused)
    if exec_res.get("success") and exec_res.get("exit"):
        await push_pnl_card(fused["symbol"])
    return fused

@app.post("/audit/{address}")
async def audit_token(address: str):
    return (await auditor.audit_token(address)).dict()

@app.post("/explain_signal")
async def explain_signal(request: Request):
    payload = await request.json()
    sym = payload.get("symbol")
    if not sym:
        raise HTTPException(status_code=400, detail="Missing 'symbol'.")
    result = await explainer.explain(sym, payload.get("context", {}))
    return result.dict() if hasattr(result, "dict") else result
