from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List


class Settings(BaseSettings):
    # Telegram Bot
    bot_token: str = Field(..., alias="BOT_TOKEN")

    # PostgreSQL
    postgres_host: str = Field("db", alias="POSTGRES_HOST")
    postgres_port: int = Field(5432, alias="POSTGRES_PORT")
    postgres_db: str = Field("tender_matcher", alias="POSTGRES_DB")
    postgres_user: str = Field("postgres", alias="POSTGRES_USER")
    postgres_password: str = Field(..., alias="POSTGRES_PASSWORD")

    # OpenAI (now optional — not used for matching, kept for backward compat)
    openai_api_key: str = Field("", alias="OPENAI_API_KEY")
    openai_model: str = Field("gpt-4o", alias="OPENAI_MODEL")
    openai_router_model: str = Field("gpt-4o-mini", alias="OPENAI_ROUTER_MODEL")

    # LLM parsing (uses OpenAI)
    llm_parsing_enabled: bool = Field(True, alias="LLM_PARSING_ENABLED")
    llm_char_matching_enabled: bool = Field(True, alias="LLM_CHAR_MATCHING_ENABLED")

    # Whitelist
    admin_ids: str = Field("", alias="ADMIN_IDS")

    # Matching settings
    # Lowered from 89% to 70%: fuzzy matching is more precise than AI-based normalization
    match_threshold: int = Field(70, alias="MATCH_THRESHOLD")
    allow_lower_values: bool = Field(False, alias="ALLOW_LOWER_VALUES")
    deduplicate_models: bool = Field(True, alias="DEDUPLICATE_MODELS")
    filter_by_spec_count: bool = Field(False, alias="FILTER_BY_SPEC_COUNT")

    # Logging
    log_level: str = Field("INFO", alias="LOG_LEVEL")

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def admin_ids_list(self) -> List[int]:
        if not self.admin_ids:
            return []
        result = []
        for x in self.admin_ids.split(","):
            x = x.strip()
            if not x:
                continue
            try:
                result.append(int(x))
            except ValueError:
                import logging
                logging.getLogger(__name__).warning(f"Invalid ADMIN_IDS entry (not an integer): {x!r}")
        return result

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }


settings = Settings()
