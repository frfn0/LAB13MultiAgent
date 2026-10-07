"""Тесты обмена с NATS (задания 3 и 4).

Соединение с брокером подменяется: nats.connect переопределён на объект с
тем же интерфейсом. Так проверяется собственная логика клиента - прежде
всего раздача ответов ожидающим заданиям по идентификатору, - без
настоящего брокера.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from nats.aio.msg import Msg

from orchestrator import client as client_module
from orchestrator.client import (
    NatsConnection,
    OrchestratorError,
    TaskFailedError,
    TaskTimeoutError,
)
from orchestrator.messages import SUBJECT_RESULT, Result, Task, Ticket


class FakeSubscription:
    """Подписка вместо настоящей."""

    def __init__(self) -> None:
        self.unsubscribed = False

    async def unsubscribe(self) -> None:
        """Снимает подписку."""
        self.unsubscribed = True


class FakeNatsClient:
    """Подмена клиента NATS.

    Ответ агента на задание формируется в обработчике `respond`: тест
    задаёт, что именно ответит "агент".
    """

    def __init__(self) -> None:
        self.is_closed = False
        self.published: list[tuple[str, bytes]] = []
        self.subscriptions: list[tuple[str, Any]] = []
        self.subscription = FakeSubscription()
        self.drained = False
        self.flushed = 0
        # Функция, которую тест задаёт вместо агента.
        self.respond: Any = None

    async def subscribe(self, subject: str, cb: Any = None) -> FakeSubscription:
        """Запоминает подписку на тему результатов."""
        self.subscriptions.append((subject, cb))
        return self.subscription

    async def publish(self, subject: str, data: bytes) -> None:
        """Запоминает публикацию и имитирует ответ агента.

        Агент отвечает отдельным сообщением в тему ticket.result, поэтому
        ответ создаётся на публикацию задания, а не наоборот.
        """
        self.published.append((subject, data))

        if self.respond is None or subject == SUBJECT_RESULT:
            return

        payload = self.respond(subject, data)
        if payload is None:
            return

        _, callback = self.subscriptions[0]
        asyncio.create_task(
            callback(Msg(_client=None, subject=SUBJECT_RESULT, reply="", data=payload))
        )

    async def flush(self) -> None:
        """Подтверждает отправку."""
        self.flushed += 1

    async def drain(self) -> None:
        """Дренирование перед закрытием."""
        self.drained = True

    async def close(self) -> None:
        """Закрывает соединение."""
        self.is_closed = True


@pytest.fixture
def fake_nats(monkeypatch: pytest.MonkeyPatch) -> FakeNatsClient:
    """Подменяет nats.connect на подставной клиент."""
    client = FakeNatsClient()

    async def fake_connect(*_: Any, **__: Any) -> FakeNatsClient:
        return client

    monkeypatch.setattr(client_module.nats, "connect", fake_connect)
    return client


def make_task(task_id: str = "task-1") -> Task:
    """Задание для отправки."""
    return Task(
        task_id=task_id,
        type="classify",
        ticket=Ticket(id="ticket-1", text="не проходит оплата"),
    )


def result_payload(task_id: str = "task-1", success: bool = True) -> bytes:
    """JSON успешного ответа агента."""
    return json.dumps(
        Result(
            task_id=task_id,
            agent="classifier",
            success=success,
            category="billing",
        ).model_dump()
    ).encode()


def send_without_answer(connection: NatsConnection, timeout: float) -> Any:
    """Отправляет задание в фоне, не отвечая на него.

    Возвращает задачу, которую нужно дождаться: она завершится по таймауту.
    """

    async def run() -> None:
        with pytest.raises(TaskTimeoutError):
            await connection.send_task("ticket.classify", make_task(), timeout=timeout)

    return asyncio.create_task(run())


class TestConnect:
    """Проверки подключения."""

    async def test_subscribes_to_results(self, fake_nats: FakeNatsClient) -> None:
        """Оркестратор подписывается на тему ответов."""
        connection = NatsConnection("nats://test:4222")
        await connection.connect()

        assert connection.connected
        assert fake_nats.subscriptions[0][0] == SUBJECT_RESULT

        await connection.close()

    async def test_close_drains_and_unsubscribes(
        self, fake_nats: FakeNatsClient
    ) -> None:
        """Закрытие начинается со снятия подписки и дренирования."""
        connection = NatsConnection("nats://test:4222")
        await connection.connect()
        await connection.close()

        assert fake_nats.subscription.unsubscribed
        assert fake_nats.drained
        assert fake_nats.is_closed
        assert not connection.connected

    async def test_connect_is_idempotent(self, fake_nats: FakeNatsClient) -> None:
        """Повторное подключение не создаёт вторую подписку."""
        connection = NatsConnection("nats://test:4222")
        await connection.connect()
        await connection.connect()

        assert len(fake_nats.subscriptions) == 1

        await connection.close()

    async def test_client_property_requires_connection(self) -> None:
        """Сырое подключение недоступно до connect."""
        connection = NatsConnection("nats://test:4222")

        with pytest.raises(OrchestratorError, match="нет соединения"):
            _ = connection.client


class TestSendTask:
    """Проверки отправки задания и получения ответа."""

    async def test_returns_result(self, fake_nats: FakeNatsClient) -> None:
        """Успешный ответ агента доходит до вызывающего кода."""
        connection = NatsConnection("nats://test:4222")
        await connection.connect()
        fake_nats.respond = lambda subject, data: result_payload()

        result = await connection.send_task("ticket.classify", make_task(), timeout=2.0)

        assert result.success
        assert result.category == "billing"
        assert connection.pending_count == 0

        await connection.close()

    async def test_result_routed_by_task_id(self, fake_nats: FakeNatsClient) -> None:
        """Ответ попадает к тому заданию, чей идентификатор совпал.

        Подмена отвечает на чужое задание: наше остаётся без ответа и
        закончится таймаутом.
        """
        connection = NatsConnection("nats://test:4222")
        await connection.connect()
        fake_nats.respond = lambda subject, data: json.dumps(
            {"task_id": "чужое"}
        ).encode()

        with pytest.raises(TaskTimeoutError):
            await connection.send_task("ticket.classify", make_task(), timeout=0.2)

        await connection.close()

    async def test_timeout_releases_pending(self, fake_nats: FakeNatsClient) -> None:
        """После таймаута задание не остаётся в ожидании."""
        connection = NatsConnection("nats://test:4222")
        await connection.connect()

        with pytest.raises(TaskTimeoutError, match="за 0.2"):
            await connection.send_task("ticket.classify", make_task(), timeout=0.2)

        assert connection.pending_count == 0

        await connection.close()

    async def test_agent_error_raises(self, fake_nats: FakeNatsClient) -> None:
        """Ответ агента с ошибкой превращается в TaskFailedError."""
        connection = NatsConnection("nats://test:4222")
        await connection.connect()
        fake_nats.respond = lambda subject, data: json.dumps(
            {
                "task_id": "task-1",
                "agent": "classifier",
                "success": False,
                "error": "текст обращения пуст",
            }
        ).encode()

        with pytest.raises(TaskFailedError, match="текст обращения пуст"):
            await connection.send_task("ticket.classify", make_task(), timeout=1.0)

        assert connection.pending_count == 0

        await connection.close()

    async def test_broken_result_is_ignored(self, fake_nats: FakeNatsClient) -> None:
        """Битый ответ не считается результатом: задание уходит в таймаут."""
        connection = NatsConnection("nats://test:4222")
        await connection.connect()
        fake_nats.respond = lambda subject, data: "{не json".encode()

        with pytest.raises(TaskTimeoutError):
            await connection.send_task("ticket.classify", make_task(), timeout=0.2)

        await connection.close()

    async def test_result_without_task_id_is_ignored(
        self, fake_nats: FakeNatsClient
    ) -> None:
        """Ответ без идентификатора игнорируется."""
        connection = NatsConnection("nats://test:4222")
        await connection.connect()
        fake_nats.respond = lambda subject, data: json.dumps({"success": True}).encode()

        with pytest.raises(TaskTimeoutError):
            await connection.send_task("ticket.classify", make_task(), timeout=0.2)

        await connection.close()

    async def test_unknown_result_is_dropped(self, fake_nats: FakeNatsClient) -> None:
        """Ответ на неизвестное задание не роняет оркестратор."""
        connection = NatsConnection("nats://test:4222")
        await connection.connect()

        _, callback = fake_nats.subscriptions[0]
        await callback(
            Msg(
                _client=None,
                subject=SUBJECT_RESULT,
                reply="",
                data=result_payload("task-неизвестное"),
            )
        )

        assert connection.pending_count == 0

        await connection.close()

    async def test_send_without_connection_fails(self) -> None:
        """Без подключения отправка невозможна."""
        connection = NatsConnection("nats://test:4222")

        with pytest.raises(OrchestratorError, match="нет соединения"):
            await connection.send_task("ticket.classify", make_task())

    async def test_publish_failure_releases_pending(
        self, fake_nats: FakeNatsClient
    ) -> None:
        """Сбой отправки не оставляет задание в ожидании."""

        async def broken_publish(subject: str, data: bytes) -> None:
            raise OSError("соединение потеряно")

        connection = NatsConnection("nats://test:4222")
        await connection.connect()
        fake_nats.publish = broken_publish  # type: ignore[method-assign]

        with pytest.raises(OrchestratorError, match="не удалось отправить"):
            await connection.send_task("ticket.classify", make_task())

        assert connection.pending_count == 0

        await connection.close()


class TestWaitForIdle:
    """Проверки ожидания простоя."""

    async def test_returns_true_when_empty(self, fake_nats: FakeNatsClient) -> None:
        """Без ожидающих заданий простой достигнут."""
        connection = NatsConnection("nats://test:4222")
        await connection.connect()

        assert await connection.wait_for_idle(timeout=0.5) is True

        await connection.close()

    async def test_returns_false_while_task_pending(
        self, fake_nats: FakeNatsClient
    ) -> None:
        """Пока задание ждёт ответа, поток не считается простаивающим."""
        connection = NatsConnection("nats://test:4222")
        await connection.connect()

        pending = send_without_answer(connection, timeout=1.0)
        # Даём фоновой задаче зарегистрировать ожидание ответа.
        await asyncio.sleep(0.05)

        assert connection.pending_count == 1
        assert await connection.wait_for_idle(timeout=0.1) is False

        await pending
        assert await connection.wait_for_idle(timeout=0.1) is True

        await connection.close()
