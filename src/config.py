import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Config(BaseSettings):
    SUPABASE_URL: str = Field(..., env="SUPABASE_URL")
    SUPABASE_SERVICE_KEY: str = Field(..., env="SUPABASE_SERVICE_KEY")
    BYBIT_API_KEY: str = Field(..., env="BYBIT_API_KEY")
    BYBIT_API_SECRET: str = Field(..., env="BYBIT_API_SECRET")
    BYBIT_WS_URL: str = "wss://stream.bybit.com/v5/public/linear"
    BYBIT_REST_URL: str = "https://api.bybit.com"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

def load_config() -> Config:
    load_dotenv()
    return Config()