"""Общие фикстуры и моки NATS для тестов оркестратора.

Брокер в тестах не нужен: NatsConnection подменяется объектом с тем же
интерфейсом, а сообщения метрик собираются напрямую. Так тесты проверяют
логику оркестратора, а не работу NATS.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pytest
from nats.aio.msg import Msg

from orchestrator.client import (
    OrchestratorError,
    TaskFailedError,
    TaskTimeoutError,
)
from orchestrator.messages import Result, Task
from orchestrator.metrics import MetricsCollector


class FakeConnection:
    """Подмена NatsConnection: отвечает по заранее заданному сценарию.

    Сценарий - список ответов на вызовы `send_task`. Элемент списка может
    быть:
      * объектом Result - вернуть его;
      * исключением - выбросить его;
      * вызываемой функцией - вызвать её с номером попытки.

    После исчерпания сценария повторяется последний элемент, чтобы удобно
    проверять поведение при нескольких попытках подряд.
    """

    def __init__(
        self, scenario: list[Any] | None = None, connected: bool = True
    ) -> None:
        self.scenario = scenario or []
        self.attempts: list[str] = []
        self.subjects: list[str] = []
        self.timeouts: list[float | None] = []
        self._connected = connected
        self.subscribed = False

    @property
    def connected(self) -> bool:
        """Подключение считается живым, если его не отключили."""
        return self._connected

    @property
    def client(self) -> Any:
        """Сырое подключение в тестах не используется."""
        raise OrchestratorError("в тестах подключение NATS недоступно")

    async def connect(self) -> None:
        """Подключение уже есть."""
        self._connected = True

    async def close(self) -> None:
        """Отключение для тестов не делает ничего."""
        self._connected = False

    async def subscribe_metrics(self, collector: MetricsCollector) -> None:
        """Отмечает, что коллектор подписан на метрики."""
        self.subscribed = True

    async def send_task(
        self,
        subject: str,
        task: Task,
        timeout: float | None = None,
    ) -> Result:
        """Отвечает по сценарию, имитируя работу агента.

        Args:
            subject: тема задания.
            task: отправляемое задание.
            timeout: таймаут одной попытки.

        Returns:
            Result: ответ по сценарию.

        Raises:
            TaskTimeoutError: если сценарий требует таймаута.
            TaskFailedError: если сценарий требует ошибки агента.
            OrchestratorError: если в сценарии другой сбой.
        """
        self.attempts.append(task.task_id)
        self.subjects.append(subject)
        self.timeouts.append(timeout)

        if not self.scenario:
            return Result(task_id=task.task_id, agent="stub", success=True)

        index = min(len(self.attempts) - 1, len(self.scenario) - 1)
        outcome = self.scenario[index]

        if callable(outcome):
            outcome = outcome(len(self.attempts))

        if isinstance(outcome, BaseException):
            raise outcome

        return outcome


class ScriptedConnection(FakeConnection):
    """Подмена, которая отвечает вычисляемым результатом.

    Нужна там, где ответ зависит от задания: например, конвейер передаёт
    результат классификации следующему шагу.
    """

    def __init__(self, responder: Callable[[str, Task], Result]) -> None:
        super().__init__()
        self._responder = responder

    async def send_task(
        self,
        subject: str,
        task: Task,
        timeout: float | None = None,
    ) -> Result:
        """Строит ответ из задания."""
        self.attempts.append(task.task_id)
        self.subjects.append(subject)
        self.timeouts.append(timeout)

        outcome = self._responder(subject, task)

        # Ответ-подмена может и задать сбой: возвращать исключение вместо
        # того, чтобы поднять его, приводило к попытке разобрать объект
        # ошибки как результат агента.
        if isinstance(outcome, BaseException):
            raise outcome

        return outcome


def timeout_error(message: str = "агент молчит") -> TaskTimeoutError:
    """Готовая ошибка таймаута."""
    return TaskTimeoutError(message)


def agent_error(message: str = "сбой агента") -> TaskFailedError:
    """Готовая ошибка агента."""
    return TaskFailedError(message)


def metrics_message(
    agent: str,
    instance: str,
    received: int = 0,
    processed: int = 0,
    failed: int = 0,
    queue: str = "",
) -> Msg:
    """Собирает сообщение с метриками агента.

    Args:
        agent: имя агента.
        instance: идентификатор экземпляра.
        received: получено заданий.
        processed: обработано заданий.
        failed: заданий с ошибкой.
        queue: имя группы очереди.

    Returns:
        Сообщение NATS с JSON метрик.
    """
    payload = json.dumps(
        {
            "agent": agent,
            "queue": queue,
            "instance": instance,
            "received": received,
            "processed": processed,
            "failed": failed,
            "uptime_seconds": 10,
        }
    ).encode()

    return Msg(_client=None, subject="agent.metrics", reply="", data=payload)


@pytest.fixture
def collector() -> MetricsCollector:
    """Пустой коллектор метрик."""
    return MetricsCollector()


@pytest.fixture
def fake_connection() -> FakeConnection:
    """Подключение, которое всегда отвечает успехом."""
    return FakeConnection()


@pytest.fixture(autouse=True)
def fast_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Убирает паузы между попытками.

    Без этого набор тестов на повторы занимал бы десятки секунд: сама пауза
    здесь ничего не проверяет, проверяется число повторов.

    Заглушка обязана отдавать управление циклу событий, поэтому внутри
    вызывается настоящий sleep с нулевым ожиданием. Заглушка, просто
    возвращающая управление, ломала бы посторонние ожидания в тестах.
    """
    import asyncio

    import orchestrator.retry as retry_module

    real_sleep = asyncio.sleep

    async def no_wait(delay: float) -> None:
        await real_sleep(0)

    monkeypatch.setattr(retry_module.asyncio, "sleep", no_wait)
