# File: main_service.py
# Elite pre-launch main service wiring for Nova Pulse Alpha

import os
import glob
import logging
import asyncio
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
from core.data_collection.social_sentiment import SocialSentiment
from core.data_collection.tech_indicator_agent import TechIndicatorAgent
from core.signal_generation.sl_watchdog import start_sl_watchdog
from core.utils.plugin_manager import PluginManager as CorePluginManager
from core.ws_endpoint import ws_manager, websocket_endpoint
from core.signal_generation.rl_trainer_loop import RLTrainerLoop
from core.execution.execute_trade import AutoTrader
from core.social.helius import register_helius_webhook
from core.utils.token_auditor import TokenAuditor
from core.utils.token_guardian import TokenGuardian
from core.utils.trade_logger import log_trade
from core.utils.dlq_manager import get_dlq_summary, replay_execution_dlq
from core.utils.signal_card import generate_signal_card
from core.utils.pnl_card import generate_pnl_card
from aiogram import Bot

# === Settings ===
class Settings(BaseSettings):
    WEBHOOK_URL: str
    TELEGRAM_CHAT_ID: int
    TELEGRAM_BOT_TOKEN: str
    SOL_PRIVATE_KEY: str
    SOL_ADDRESS: str
    REDIS_URL: str
    HELIUS_RPC: str
    HELIUS_API_KEY: str
    CMC_API_KEY: str
    OPENAI_API_KEY: str
    METRICS_PORT: int = 8001
    # For internal polling
    HTTP_PORT: int = 8000
    JAEGER_HOST: str = "localhost"
    JAEGER_PORT: int = 6831

    class Config:
        env_file = ".env"

# Initialize settings and config
settings = Settings()
config = Config()

# Telegram Bot (for pushers)
bot = Bot(token=settings.TELEGRAM_BOT_TOKEN)

# FastAPI app setup
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

# Prometheus metrics
start_http_server(settings.METRICS_PORT)
signals_counter = Counter("nova_signals_generated", "Signals generated")
agents_gauge = Gauge("nova_agents_active", "Active agents in mesh")

# === Core subsystems ===
wallet_manager = WalletManager(config)
trade_memory = TradeMemory(config)
rl_tuner = ReinforcementTuner(config)
execution_router = ExecutionRouter(
    wallet_manager=wallet_manager,
    rpc_nodes=[settings.HELIUS_RPC],
)
auditor = TokenAuditor(config.REDIS_URL)
guardian = TokenGuardian(config.HELIUS_API_KEY)
explainer = SignalExplainerAI(config.OPENAI_API_KEY)
plugin_manager = CorePluginManager()
auto_trader = AutoTrader(
    redis_url=settings.REDIS_URL,
    router=execution_router,
    wallet_manager=wallet_manager,
    telegram_token=settings.TELEGRAM_BOT_TOKEN,
    telegram_chat_id=settings.TELEGRAM_CHAT_ID,
)

# Register agents
AGENT_REGISTRY = {}
def register_agent(name: str, agent_obj):
    AGENT_REGISTRY[name] = agent_obj

register_agent(
    "alpha_signal",
    AlphaSignalAgent(
        config=config,
        trade_memory=trade_memory,
        rl_tuner=rl_tuner,
        orderbook_overlay=None,
    ),
)
register_agent("quantum_fractal", QuantumFractalAgent(config))
register_agent("social_sentiment", SocialSentiment(config))
register_agent("tech_indicators", TechIndicatorAgent(config))

async def adaptive_signal_fusion(symbol: str, agent_results: list[dict]) -> dict:
    weights = {res["source"]: rl_tuner.get_trust_score(res["source"], symbol) for res in agent_results}
    total = sum(weights.values()) or 1.0
    score = sum(res["score"] * weights[res["source"]] for res in agent_results) / total
    entropy_penalty = 1.0 - min(max(score, 0), 1)
    adjusted_score = round(score * (1 - entropy_penalty * 0.15), 4)
    return {
        "symbol": symbol,
        "score": adjusted_score,
        "breakdown": [{**res, "weight": weights[res["source"]]} for res in agent_results],
        "meta": {"fusion": "adaptive_trust_entropy", "agents": list(weights.keys())},
    }

async def load_plugins_from_dir(directory: str = "core/plugins"):
    mapping = {
        os.path.splitext(os.path.basename(f))[0]: f.replace("/", ".")[:-3]
        for f in glob.glob(f"{directory}/*.py")
    }
    await plugin_manager.initialize(mapping)
    logging.info("🔌 Loaded plugins: %s", list(mapping.keys()))

def setup_logging():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

def setup_tracing():
    exporter = JaegerExporter(
        agent_host_name=settings.JAEGER_HOST, agent_port=settings.JAEGER_PORT
    )
    provider = TracerProvider(resource=Resource.create({"service.name": "nova-pulse"}))
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    logging.info(
        "✅ Jaeger tracing initialized on %s:%s", settings.JAEGER_HOST, settings.JAEGER_PORT
    )

# Telegram pushers
async def push_signal_card(signal: dict):
    try:
        img = await generate_signal_card(signal)
        caption = (
            f"🚨 *Nova Pulse Signal*\n"
            f"Symbol: {signal.get('symbol')}\n"
            f"Confidence: {signal.get('score'):.2f}\n"
            f"Entry: {signal.get('entry', 'N/A')}\n"
            f"TP: {signal.get('tp', '...')} | SL: {signal.get('sl', '...')}\n"
            f"@NovaPulseAI"
        )
        await bot.send_photo(
            settings.TELEGRAM_CHAT_ID, img, caption, parse_mode="Markdown"
        )
    except Exception as e:
        logging.error("[push_signal_card] %s", e)

async def push_trade_card(signal: dict, execution: dict):
    try:
        img = await generate_signal_card({**signal, **execution, "executed": True})
        caption = (
            f"💸 *Trade Executed*\n"
            f"Symbol: {signal.get('symbol')} | Side: {execution.get('side', 'N/A')}\n"
            f"Size: {execution.get('size', 'N/A')} | Entry: {execution.get('entry', 'N/A')}\n"
            f"TP: {execution.get('tp', '...')} | SL: {execution.get('sl', '...')}\n"
            f"Status: {'✅ Success' if execution.get('success') else '❌ Fail'}\n"
            f"@NovaPulseAI"
        )
        await bot.send_photo(
            settings.TELEGRAM_CHAT_ID, img, caption, parse_mode="Markdown"
        )
    except Exception as e:
        logging.error("[push_trade_card] %s", e)

async def push_pnl_card(symbol: str):
    try:
        img = await generate_pnl_card(symbol)
        caption = (
            f"🏁 *Trade Closed*\nSymbol: {symbol}\nPnL snapshot below\n@NovaPulseAI"
        )
        await bot.send_photo(
            settings.TELEGRAM_CHAT_ID, img, caption, parse_mode="Markdown"
        )
    except Exception as e:
        logging.error("[push_pnl_card] %s", e)

# ─ Startup & shutdown ─
@app.on_event("startup")
async def startup_event():
    setup_logging()
    setup_tracing()
    logging.info("Prometheus metrics on port %s", settings.METRICS_PORT)

    # Connect agents and subsystems
    for agent in AGENT_REGISTRY.values():
        if hasattr(agent, "connect"):
            await agent.connect()
    await auditor.connect()
    await explainer.connect()
    await auto_trader.connect()
    await execution_router.connect()
    await load_plugins_from_dir()

    # RL trainer & SL watchdog
    asyncio.create_task(RLTrainerLoop(rl_tuner).start())
    asyncio.create_task(start_sl_watchdog(wallet_manager, trade_memory, execution_router))

    # Auto-trade loop
    async def trade_loop():
        while True:
            try:
                await auto_trader.execute_next_trade()
            except Exception as e:
                logging.error("Auto-trade loop error: %s", e)
            await asyncio.sleep(60)
    asyncio.create_task(trade_loop())

    # Automated signal loop
    symbols = os.getenv("WATCH_SYMBOLS", "SOL,ETH,BTC").split(",")
    interval = int(os.getenv("SIGNAL_INTERVAL", "60"))
    async def signal_loop():
        client = httpx.AsyncClient(timeout=10)
        url = f"http://localhost:{settings.HTTP_PORT}/generate_signal"
        while True:
            for sym in symbols:
                try:
                    r = await client.post(url, json={"symbol": sym})
                    if r.status_code != 200:
                        logging.warning("Signal loop failed %s → %s", sym, r.text)
                except Exception as e:
                    logging.error("Signal loop error for %s: %s", sym, e)
            await asyncio.sleep(interval)
    asyncio.create_task(signal_loop())

    # Helius webhook (if using)
    await register_helius_webhook(settings)

    logging.info("Nova Pulse LIVE: %s", list(AGENT_REGISTRY.keys()))

@app.on_event("shutdown")
async def shutdown_event():
    logging.info("Shutting down Nova Pulse...")
    for agent in AGENT_REGISTRY.values():
        if hasattr(agent, "disconnect"):
            await agent.disconnect()
    await auditor.disconnect()
    await explainer.disconnect()
    await auto_trader.disconnect()
    await execution_router.disconnect()
    logging.info("Clean shutdown complete.")

@app.exception_handler(Exception)
async def universal_exception_handler(request: Request, exc: Exception):
    logging.error("Unhandled error: %s", exc, exc_info=True)
    return JSONResponse(status_code=500, content={"detail": "Internal error occurred."})

@app.get("/healthz")
def health_check():
    return {"status": "ok", "agents": list(AGENT_REGISTRY.keys())}

@app.get("/dlq/status")
async def dlq_status():
    summary = await get_dlq_summary()
    return {"status": "ok", "summary": summary}

@app.post("/dlq/replay")
async def replay_dlq():
    results = await replay_execution_dlq(execution_router)
    return {"replay_attempted": len(results), "details": results}

@app.post("/generate_signal")
async def generate_signal(request: Request):
    payload = await request.json()
    symbol = payload.get("symbol")
    if not symbol:
        raise HTTPException(status_code=400, detail="Missing 'symbol' field.")

    # Gather agent signals
    tasks = [
        agent.analyze(symbol, **payload)
        if hasattr(agent, "analyze")
        else agent.score_sentiment(symbol)
        for agent in AGENT_REGISTRY.values()
    ]
    results = await asyncio.gather(*tasks)

    agent_results = []
    for name, res in zip(AGENT_REGISTRY.keys(), results):
        score = getattr(res, "confidence", None) or getattr(res, "score", 0.0)
        meta = {}
        if hasattr(res, "trust_score"):
            meta["trust"] = res.trust_score
        if hasattr(res, "volume"):
            meta["volume"] = res.volume
        agent_results.append(
            {"source": name, "score": score, "meta": meta, "entry": getattr(res, "entry", 0.0)}
        )

    signals_counter.inc()
    agents_gauge.set(len(AGENT_REGISTRY))

    # Fuse, broadcast, push
    fused = await adaptive_signal_fusion(symbol, agent_results)
    await ws_manager.broadcast(fused)
    await push_signal_card(fused)

    # Execute trade + push card
    exec_res = await execution_router.execute_trade(fused, user_id="telegram")
    fused["execution"] = exec_res
    await push_trade_card(fused, exec_res)

    # Log + update
    await log_trade(
        fused["symbol"],
        exec_res.get("side"),
        exec_res.get("size"),
        exec_res.get("entry"),
        exec_res.get("exit"),
        exec_res.get("pnl"),
        rationale="auto-trade fused signal",
        user="telegram",
    )
    from core.telegram_pusher import push_trade_update
    await push_trade_update(fused)

    # Closed PnL snapshot
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
        raise HTTPException(status_code=400, detail="Missing 'symbol' field.")
    result = await explainer.explain(sym, payload.get("context", {}))
    return result.dict() if hasattr(result, "dict") else result
