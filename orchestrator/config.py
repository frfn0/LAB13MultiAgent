"""Настройки оркестратора из переменных окружения."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Параметры оркестратора."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    nats_url: str = "nats://127.0.0.1:4222"

    # Сколько ждать ответа агента по умолчанию, секунды.
    task_timeout: float = 5.0

    # Повторных попыток не больше трёх - это задание 6.
    max_attempts: int = 3

    # Пауза перед повторной попыткой, секунды.
    retry_delay: float = 0.5

    log_level: str = "INFO"
    log_file: str = ""


@lru_cache
def get_settings() -> Settings:
    """Настройки создаются один раз за процесс."""
    return Settings()
