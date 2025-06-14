# File: main_service.py

import os
import glob
import logging
import asyncio
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

# Utils & Core imports aligned with repo structure
from utils.config import Config
from core.wallet_management.wallet_manager import WalletManager
from utils.trade_memory import TradeMemory
from core.signal_generation.reinforcement_learning import ReinforcementTuner
from core.execution.execution_router import ExecutionRouter
from core.signal_generation.signal_explainer_ai import SignalExplainerAI
from core.signal_generation.combined_signal_engine import CombinedSignalEngine
from core.agents.quantum_fractal_agent import QuantumFractalAgent
from core.data_collection.social_sentiment import SocialSentimentEngine
from core.signal_generation.sl_watchdog import start_sl_watchdog
from core.data_collection.tech_indicator_agent import TechIndicatorAgent
from core.plugin_manager import PluginManager
from core.ws_endpoint import ws_manager, websocket_endpoint
from core.feedback_router import build_feedback_router, router as feedback_router
from core.rl_trainer_loop import RLTrainerLoop
from core.pnl_api import router as pnl_router
from core.telegram_pusher import push_trade_update
from core.auto_trader import AutoTrader
from core.helius_hooks import helius_router, register_helius_webhook
from utils.token_auditor import TokenAuditor
from utils.token_guardian import TokenGuardian
from utils.trade_logger import log_trade
from utils.dlq_manager import get_all_dlq, get_dlq_summary, replay_execution_dlq

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
    JAEGER_HOST: str = "localhost"
    JAEGER_PORT: int = 6831

    class Config:
        env_file = ".env"

# Initialize settings and config
settings = Settings()
config = Config()

# FastAPI app
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
signals_counter = Counter('nova_signals_generated', 'Signals generated')
agents_gauge    = Gauge('nova_agents_active',     'Active agents in mesh')

# Initialize core components
tools_logger = logging.getLogger()
wallet_manager   = WalletManager(config)
trade_memory     = TradeMemory(config)
rl_tuner         = ReinforcementTuner(config)
execution_router = ExecutionRouter(
    wallet_manager=wallet_manager,
    rpc_nodes=[settings.HELIUS_RPC],
    config=config
)
auditor          = TokenAuditor(config.REDIS_URL)
guardian         = TokenGuardian(config.HELIUS_API_KEY)
explainer        = SignalExplainerAI(config.OPENAI_API_KEY)
plugin_manager   = PluginManager()
auto_trader      = AutoTrader(
    redis_url=settings.REDIS_URL,
    router=execution_router,
    wallet_manager=wallet_manager,
    telegram_token=settings.TELEGRAM_BOT_TOKEN,
    telegram_chat_id=settings.TELEGRAM_CHAT_ID
)

# Agent registry
AGENT_REGISTRY: dict[str, Any] = {}

def register_agent(name: str, agent_obj: Any):
    AGENT_REGISTRY[name] = agent_obj

register_agent(
    "alpha_signal",
    AlphaSignalAgent(
        redis_url=settings.REDIS_URL,
        openai_api_key=settings.OPENAI_API_KEY,
        trade_memory=trade_memory,
        rl_tuner=rl_tuner
    )
)
register_agent("quantum_fractal", QuantumFractalAgent(config))
register_agent("social_sentiment", SocialSentimentEngine(config))
register_agent("tech_indicators", TechIndicatorAgent(config))

async def adaptive_signal_fusion(symbol: str, agent_results: list[dict]) -> dict:
    weights = {
        res["source"]: getattr(rl_tuner, 'get_trust_score', lambda s, sym: 1.0)(res["source"], symbol)
        for res in agent_results
    }
    total = sum(weights.values()) or 1.0
    score = sum(res["score"] * weights[res["source"]] for res in agent_results) / total
    entropy_penalty = 1.0 - min(max(score, 0), 1)
    adjusted_score = round(score * (1 - entropy_penalty * 0.15), 4)
    return {
        "symbol": symbol,
        "score": adjusted_score,
        "breakdown": [{**res, "weight": weights[res["source"]]} for res in agent_results],
        "meta": {"fusion": "adaptive_trust_entropy", "agents": list(weights.keys())}
    }

async def load_plugins_from_dir(directory: str = "core/plugins"):
    plugin_map = {
        os.path.splitext(os.path.basename(f))[0]:
        f.replace('/', '.')[:-3]
        for f in glob.glob(f"{directory}/*.py")
    }
    await plugin_manager.initialize(plugin_map)
    logging.info("🔌 Loaded plugins: %s", list(plugin_map.keys()))

# Tracing and Logging

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )


def setup_tracing():
    exporter = JaegerExporter(
        agent_host_name=settings.JAEGER_HOST,
        agent_port=settings.JAEGER_PORT
    )
    provider = TracerProvider(resource=Resource.create({"service.name": "nova-pulse"}))
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    logging.info(f"✅ Jaeger tracing initialized on {settings.JAEGER_HOST}:{settings.JAEGER_PORT}")

# Startup & Shutdown
@app.on_event("startup")
async def startup_event():
    setup_logging()
    setup_tracing()
    logging.info(f"Prometheus metrics running on port {settings.METRICS_PORT}")

    # Connect agents
    for agent in AGENT_REGISTRY.values():
        if hasattr(agent, "connect"):
            await agent.connect()

    await auditor.connect()
    await explainer.connect()
    await auto_trader.connect()
    await execution_router.connect()

    await load_plugins_from_dir()
    asyncio.create_task(RLTrainerLoop(rl_tuner).start())
    asyncio.create_task(start_sl_watchdog(wallet_manager, trade_memory, execution_router))

    # Auto-trade loop
    async def trade_loop():
        while True:
            try:
                await auto_trader.execute_next_trade()
                await asyncio.sleep(60)
            except Exception as e:
                logging.error(f"Auto-trade loop error: {e}")
    asyncio.create_task(trade_loop())
    
    await register_helius_webhook(settings)
    logging.info("✅ Nova Pulse Alpha LIVE with agents: %s", list(AGENT_REGISTRY.keys()))

@app.on_event("shutdown")
async def shutdown_event():
    logging.info("😕 Nova Pulse shutting down...")
    for agent in AGENT_REGISTRY.values():
        if hasattr(agent, "disconnect"):
            await agent.disconnect()
    await auditor.disconnect()
    await explainer.disconnect()
    await auto_trader.disconnect()
    await execution_router.disconnect()
    logging.info("🧼 Clean shutdown complete.")

@app.exception_handler(Exception)
async def universal_exception_handler(request: Request, exc: Exception):
    logging.error(f"Unhandled error: {exc}", exc_info=True)
    return JSONResponse(status_code=500, content={"detail": "Internal error. Nova Pulse has been notified."})

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

    tasks = [
        agent.analyze(symbol, **payload)
        if hasattr(agent, "analyze") else agent.score_sentiment(symbol)
        for agent in AGENT_REGISTRY.values()
    ]
    results = await asyncio.gather(*tasks)

    agent_results = []
    for name, result in zip(AGENT_REGISTRY.keys(), results):
        score = getattr(result, "confidence", None) or getattr(result, "score", 0.0)
        meta = {}
        if hasattr(result, "trust_score"):
            meta["trust"] = result.trust_score
        if hasattr(result, "volume"):
            meta["volume"] = result.volume
        agent_results.append({"source": name, "score": score, "meta": meta, "entry": getattr(result, "entry", 0.0)})

    signals_counter.inc()
    agents_gauge.set(len(AGENT_REGISTRY))

    fused_signal = await adaptive_signal_fusion(symbol, agent_results)
    await ws_manager.broadcast(fused_signal)

    execution = await execution_router.execute_trade(fused_signal, user_id="telegram")
    fused_signal["execution"] = execution
    await log_trade(
        fused_signal["symbol"], execution.get("side"), execution.get("size"),
        execution.get("entry"), execution.get("exit"), execution.get("pnl"),
        rationale="fused signal auto-trade", user="telegram"
    )
    await push_trade_update(fused_signal)

    return fused_signal

@app.post("/audit/{address}")
async def audit_token(address: str):
    return (await auditor.audit_token(address)).dict()

@app.post("/explain_signal")
async def explain_signal(request: Request):
    payload = await request.json()
    symbol = payload.get("symbol")
    context = payload.get("context", {})
    if not symbol:
        raise HTTPException(status_code=400, detail="Missing 'symbol' field.")
    explanation = await explainer.explain(symbol, context)
    return explanation.dict() if hasattr(explanation, "dict") else explanation
