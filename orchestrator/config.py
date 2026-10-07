"""Настройки оркестратора из переменных окружения."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict

from orchestrator.retry import RetryPolicy


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
    # Считаются все попытки, включая первую: по умолчанию одна первая
    # и две повторные.
    max_attempts: int = 3

    # Пауза перед повторной попыткой, секунды.
    retry_delay: float = 0.5

    # Во сколько раз растёт пауза перед каждым следующим повтором.
    retry_backoff: float = 2.0

    # Верхняя граница паузы между повторами, секунды.
    retry_max_delay: float = 5.0

    # Доля случайного разброса паузы, от 0 до 1.
    retry_jitter: float = 0.1

    # Повторять ли задание, если агент ответил ошибкой, а не промолчал.
    retry_on_agent_error: bool = True

    # Где поднимать REST API из задания 8.
    api_host: str = "127.0.0.1"
    api_port: int = 8000

    log_level: str = "INFO"
    log_file: str = ""


@lru_cache
def get_settings() -> Settings:
    """Настройки создаются один раз за процесс."""
    return Settings()


@lru_cache
def get_retry_policy() -> RetryPolicy:
    """Политика повторов, собранная из настроек."""
    settings = get_settings()
    return RetryPolicy(
        max_attempts=settings.max_attempts,
        initial_delay=settings.retry_delay,
        backoff=settings.retry_backoff,
        max_delay=settings.retry_max_delay,
        jitter=settings.retry_jitter,
        retry_on_agent_error=settings.retry_on_agent_error,
    )
