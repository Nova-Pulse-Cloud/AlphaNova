# core/utils/config.py

import os
import json
import re
from typing import Any, Dict

class Config:
    """
    Singleton configuration loader with strict validation.
    Loads all required environment variables, validates formats,
    and reads the Solana private key from a mounted JSON secret.
    """
    _instance: "Config" = None
    _initialized: bool = False

    # Regex for base58‐encoded Solana addresses (32–44 chars, no 0/O/I/l)
    _BASE58_REGEX = re.compile(r'^[A-HJ-NP-Za-km-z1-9]{32,44}$')

    def __new__(cls) -> "Config":
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True

        # Core connectivity
        self.REDIS_URL         = self._require("REDIS_URL")
        self.TELEGRAM_BOT_TOKEN= self._require("TELEGRAM_BOT_TOKEN")
        self.TELEGRAM_CHAT_ID  = self._require_int("TELEGRAM_CHAT_ID")
        self.WEBHOOK_URL       = self._require("WEBHOOK_URL")

        # Solana wallet
        key_path = self._require("SOL_PRIVATE_KEY_PATH")
        self.SOL_PRIVATE_KEY   = self._load_json_secret(key_path)
        self.SOL_ADDRESS       = self._require("SOL_ADDRESS")
        self._validate_base58(self.SOL_ADDRESS, "SOL_ADDRESS")

        # RPC / API keys
        self.HELIUS_RPC            = self._require("HELIUS_RPC")
        self._validate_url(self.HELIUS_RPC, "HELIUS_RPC")
        self.HELIUS_API_KEY        = self._require("HELIUS_API_KEY")
        self.CMC_API_KEY           = self._require("CMC_API_KEY")
        self.GECKO_API_URL         = self._require("GECKO_API_URL")
        self.OPENAI_API_KEY        = self._require("OPENAI_API_KEY")
        self.TWITTER_BEARER_TOKEN  = self._require("TWITTER_BEARER_TOKEN")
        self.MACRO_API_URL         = self._require("MACRO_API_URL")
        self.MACRO_API_KEY         = self._require("MACRO_API_KEY")
        self.ETHERSCAN_API_KEY     = self._require("ETHERSCAN_API_KEY")

    def _require(self, key: str) -> str:
        """Fetch an env var or raise if missing."""
        val = os.getenv(key)
        if not val:
            raise EnvironmentError(f"[Config] Missing required environment variable: '{key}'")
        return val

    def _require_int(self, key: str) -> int:
        """Fetch an env var and cast to int, raising on error."""
        val = self._require(key)
        try:
            return int(val)
        except ValueError:
            raise ValueError(f"[Config] Environment variable '{key}' must be an integer, got '{val}'")

    def _load_json_secret(self, path: str) -> Dict[str, Any]:
        """Read a JSON file (e.g. Solana keypair), with precise error messages."""
        if not os.path.isfile(path):
            raise FileNotFoundError(f"[Config] Secret file not found: '{path}'")
        try:
            with open(path, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            raise ValueError(f"[Config] Secret file at '{path}' is not valid JSON")
        except Exception as e:
            raise RuntimeError(f"[Config] Failed to load secret from '{path}': {e}")

    def _validate_url(self, url: str, key: str) -> None:
        """Basic URL format check."""
        if not url.startswith(("https://", "http://")):
            raise ValueError(f"[Config] Invalid URL in '{key}': '{url}'")

    def _validate_base58(self, addr: str, key: str) -> None:
        """Ensure Solana addresses are valid base58 strings."""
        if not self._BASE58_REGEX.match(addr):
            raise ValueError(f"[Config] Invalid base58 address in '{key}': '{addr}'")

# Global singleton instance
config = Config()
