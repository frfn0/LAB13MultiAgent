"""Схемы запросов и ответов API.

Схемы описаны явно, а не через dict: FastAPI по ним проверяет входные
данные, генерирует документацию и возвращает 422 на некорректный запрос.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class TicketCreate(BaseModel):
    """Запрос на обработку обращения клиента."""

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {"text": "Не могу оплатить заказ, карта не проходит"},
            ]
        },
    )

    text: str = Field(
        min_length=5,
        max_length=2000,
        description="Текст обращения клиента",
    )


class TicketResponse(BaseModel):
    """Результат обработки обращения."""

    model_config = ConfigDict(extra="allow")

    ticket_id: str = ""
    text: str = ""
    success: bool = True
    category: str = ""
    priority: int = 0
    tags: list[str] = Field(default_factory=list)
    answer: str = ""
    answer_type: str = ""
    escalated: bool = False
    assigned_to: str = ""
    sla_hours: int = 0
    article_title: str = ""
    confidence: float = 0.0
    duration_ms: float = 0.0
    status: str = "completed"
    steps: list[dict[str, Any]] = Field(default_factory=list)


class TicketAcceptedResponse(BaseModel):
    """Ответ на принятие обращения в асинхронную работу."""

    ticket_id: str
    status: str = "accepted"
    message: str = ""


class TicketListResponse(BaseModel):
    """Список обработанных обращений или метрики агентов."""

    model_config = ConfigDict(extra="allow")

    count: int = 0
    items: list[dict[str, Any]] = Field(default_factory=list)


class HealthResponse(BaseModel):
    """Состояние API, NATS и агентов."""

    status: str = "ok"
    nats_connected: bool = False
    nats_url: str = ""
    agents: int = 0
    agents_list: list[str] = Field(default_factory=list)
    tickets_processed: int = 0


class ErrorResponse(BaseModel):
    """Ошибка API."""

    error: str
    detail: str = ""
