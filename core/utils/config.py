import os
import json

class Config:
    def __init__(self):
        # Core connectivity
        self.REDIS_URL             = self._get_env("REDIS_URL")
        self.TELEGRAM_BOT_TOKEN    = self._get_env("TELEGRAM_BOT_TOKEN")
        self.TELEGRAM_CHAT_ID      = int(self._get_env("TELEGRAM_CHAT_ID"))
        self.WEBHOOK_URL           = self._get_env("WEBHOOK_URL")

        # Solana wallet (mounted secret)
        key_path = self._get_env("SOL_PRIVATE_KEY_PATH")
        try:
            with open(key_path, 'r') as f:
                self.SOL_PRIVATE_KEY = json.load(f)
        except Exception as e:
            raise RuntimeError(f"Failed to load Solana key from {key_path}: {e}")
        self.SOL_ADDRESS           = self._get_env("SOL_ADDRESS")

        # RPC / API keys
        self.HELIUS_RPC            = self._get_env("HELIUS_RPC")
        self.HELIUS_API_KEY        = self._get_env("HELIUS_API_KEY")
        self.CMC_API_KEY           = self._get_env("CMC_API_KEY")
        self.GECKO_API_URL         = self._get_env("GECKO_API_URL")
        self.OPENAI_API_KEY        = self._get_env("OPENAI_API_KEY")
        self.TWITTER_BEARER_TOKEN  = self._get_env("TWITTER_BEARER_TOKEN")
        self.MACRO_API_URL         = self._get_env("MACRO_API_URL")
        self.MACRO_API_KEY         = self._get_env("MACRO_API_KEY")
        self.ETHERSCAN_API_KEY     = self._get_env("ETHERSCAN_API_KEY")

    def _get_env(self, key: str, default=None) -> str:
        """Fetch an env var or fail early."""
        value = os.getenv(key, default)
        if not value:
            raise ValueError(f"Missing required environment variable: {key}")
        return value
