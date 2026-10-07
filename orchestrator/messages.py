"""Контракт сообщений на стороне Python.

Должен совпадать с pkg/messages/messages.go на стороне Go: те же имена
полей в JSON и те же значения констант. При изменении правится оба файла.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

# Темы NATS. Значения совпадают с константами в Go.
SUBJECT_CLASSIFY = "ticket.classify"
SUBJECT_KNOWLEDGE = "ticket.knowledge"
SUBJECT_ANSWER = "ticket.answer"
SUBJECT_ESCALATE = "ticket.escalate"
SUBJECT_RESULT = "ticket.result"
SUBJECT_METRICS = "agent.metrics"

# Категории обращений.
CATEGORY_BILLING = "billing"
CATEGORY_TECHNICAL = "technical"
CATEGORY_ACCOUNT = "account"
CATEGORY_DELIVERY = "delivery"
CATEGORY_OTHER = "other"

# Типы заданий.
TASK_CLASSIFY = "classify"
TASK_KNOWLEDGE = "knowledge"
TASK_ANSWER = "answer"
TASK_ESCALATE = "escalate"

# Типы ответов агента генерации.
ANSWER_RESOLVED = "resolved"
ANSWER_DEFERRED = "deferred"


class Ticket(BaseModel):
    """Исходное обращение клиента."""

    model_config = ConfigDict(extra="forbid")

    id: str
    text: str
    author: str = "client"


class Task(BaseModel):
    """Задание, отправляемое агенту."""

    model_config = ConfigDict(extra="forbid")

    task_id: str
    type: str
    ticket: Ticket
    category: str = ""
    priority: int = 0
    tags: list[str] = Field(default_factory=list)
    article_id: int = 0
    article_title: str = ""
    solution: str = ""
    confidence: float = 0.0
    found: bool = False
    reason: str = ""
    attempts: int = 1


class Result(BaseModel):
    """Ответ агента."""

    model_config = ConfigDict(extra="allow")

    task_id: str
    agent: str = ""
    success: bool = False
    error: str = ""

    category: str = ""
    priority: int = 0
    tags: list[str] = Field(default_factory=list)

    article_id: int = 0
    article_title: str = ""
    solution: str = ""
    confidence: float = 0.0
    found: bool = False

    answer: str = ""
    answer_type: str = ""

    escalation_id: str = ""
    assigned_to: str = ""
    sla_hours: int = 0
    urgent: bool = False
