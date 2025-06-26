import os
import json
import re
from typing import Any, List


# ───── Legacy constants (still globally importable) ─────
DEXSCREENER_URL = os.getenv("DEXSCREENER_URL", "https://api.dexscreener.com/latest/dex/tokens/")
PRICE_CHANGE_THRESHOLD = float(os.getenv("PRICE_CHANGE_THRESHOLD", "0.02"))


class Config:
    """
    Singleton config loader with strict validation.
    Ensures required envs, JSON secret parsing, Base58, and URL checks.
    """
    _instance: "Config" = None
    _initialized: bool = False

    # Regex for Solana wallet validation (avoids 0/O/l/I confusion)
    _BASE58_REGEX = re.compile(r'^[A-HJ-NP-Za-km-z1-9]{32,44}$')

    # Valid token logo CDNs
    TRUSTED_LOGO_SOURCES: List[str] = [
        "https://assets.coingecko.com",
        "https://raw.githubusercontent.com",
        "https://s2.coinmarketcap.com/",
        "https://cdn.solana.fm/"
    ]

    def __new__(cls) -> "Config":
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True

        # Timeouts / network defaults
        self.HTTP_TIMEOUT = self._require_int("HTTP_TIMEOUT", default="15")
        self.JUPITER_SWAP_ENDPOINT = os.getenv("JUPITER_SWAP_ENDPOINT", "https://quote-api.jup.ag/v6/swap")

        # Telegram + Redis
        self.REDIS_URL = self._require("REDIS_URL")
        self.TELEGRAM_BOT_TOKEN = self._require("TELEGRAM_BOT_TOKEN")
        self.TELEGRAM_CHAT_ID = self._require_int("TELEGRAM_CHAT_ID")
        self.WEBHOOK_URL = self._require("WEBHOOK_URL")

        # Solana wallet + key path
        key_path = self._require("SOL_PRIVATE_KEY_PATH")
        self.SOL_PRIVATE_KEY = self._load_json_secret(key_path)
        self.SOL_ADDRESS = self._require("SOL_ADDRESS")
        self._validate_base58(self.SOL_ADDRESS, "SOL_ADDRESS")

        # APIs + keys
        self.HELIUS_RPC = self._require("HELIUS_RPC")
        self._validate_url(self.HELIUS_RPC, "HELIUS_RPC")
        self.HELIUS_API_KEY = self._require("HELIUS_API_KEY")
        self.CMC_API_KEY = self._require("CMC_API_KEY")
        self.GECKO_API_URL = self._require("GECKO_API_URL")
        self.OPENAI_API_KEY = self._require("OPENAI_API_KEY")
        self.TWITTER_BEARER_TOKEN = self._require("TWITTER_BEARER_TOKEN")
        self.MACRO_API_URL = self._require("MACRO_API_URL")
        self.MACRO_API_KEY = self._require("MACRO_API_KEY")
        self.ETHERSCAN_API_KEY = self._require("ETHERSCAN_API_KEY")

        # ML model paths
        self.TPSL_MODEL_PATH = os.getenv("TPSL_MODEL_PATH", "models/tp_sl_predictor.pkl")
        self.DEFAULT_TP_PCT = self._require_float("DEFAULT_TP_PCT", default="0.07")
        self.DEFAULT_SL_PCT = self._require_float("DEFAULT_SL_PCT", default="0.04")

        # Optional tracing
        self.JAEGER_HOST = os.getenv("JAEGER_HOST", "localhost")
        self.JAEGER_PORT = self._require_int("JAEGER_PORT", default="6831")

        # File paths
        self.TRADE_MEMORY_DB_PATH = os.getenv("TRADE_MEMORY_DB_PATH", "trade_memory.db")
        self.MASCOT_IMAGE_PATH = os.getenv("MASCOT_IMAGE_PATH", "assets/mascot/Nova1.png")
        self.LOGO_DIR = os.getenv("LOGO_DIR", "assets/templates/")  # 🔥 This fixes your crash.

    # ───── Validation + Secrets ─────

    def _require(self, key: str) -> str:
        val = os.getenv(key)
        if not val:
            raise EnvironmentError(f"[Config] Missing required environment variable: '{key}'")
        return val

    def _require_int(self, key: str, default: str = None) -> int:
        val = os.getenv(key, default)
        try:
            return int(val)
        except ValueError:
            raise ValueError(f"[Config] '{key}' must be an integer, got '{val}'")

    def _require_float(self, key: str, default: str = None) -> float:
        val = os.getenv(key, default)
        try:
            return float(val)
        except ValueError:
            raise ValueError(f"[Config] '{key}' must be a float, got '{val}'")

    def _load_json_secret(self, path: str) -> Any:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"[Config] Secret file not found: '{path}'")
        try:
            with open(path, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            raise ValueError(f"[Config] Secret file at '{path}' is not valid JSON")

    def _validate_url(self, url: str, key: str) -> None:
        if not url.startswith(("https://", "http://")):
            raise ValueError(f"[Config] Invalid URL in '{key}': '{url}'")

    def _validate_base58(self, addr: str, key: str) -> None:
        if not self._BASE58_REGEX.match(addr):
            raise ValueError(f"[Config] Invalid Base58 address in '{key}': '{addr}'")


# ───── Global config instance ─────
config = Config()
