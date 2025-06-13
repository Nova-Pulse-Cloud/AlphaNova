class Config:
    def __init__(self):
        self.REDIS_URL = self.get_env("REDIS_URL")
        self.TELEGRAM_BOT_TOKEN = self.get_env("TELEGRAM_BOT_TOKEN")
        self.TELEGRAM_CHAT_ID = self.get_env("TELEGRAM_CHAT_ID")
        self.WEBHOOK_URL = self.get_env("WEBHOOK_URL")
        self.SOL_PRIVATE_KEY = self.get_env("SOL_PRIVATE_KEY")
        self.SOL_ADDRESS = self.get_env("SOL_ADDRESS")
        self.HELIUS_RPC = self.get_env("HELIUS_RPC")
        self.HELIUS_API_KEY = self.get_env("HELIUS_API_KEY")
        self.CMC_API_KEY = self.get_env("CMC_API_KEY")
        self.GECKO_API_URL = self.get_env("GECKO_API_URL")
        self.OPENAI_API_KEY = self.get_env("OPENAI_API_KEY")
        self.TWITTER_BEARER_TOKEN = self.get_env("TWITTER_BEARER_TOKEN")

    def get_env(self, key: str, default=None) -> str:
        """Helper function to get environment variables with optional fallback."""
        value = os.getenv(key, default)
        if not value:
            raise ValueError(f"Missing required environment variable: {key}")
        return value
