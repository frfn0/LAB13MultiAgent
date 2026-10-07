"""Тесты REST API (задание 8).

Приложение проверяется через TestClient с подменённым состоянием: брокер и
агенты не нужны, проверяется именно слой HTTP - коды ответа, проверка входа
и поведение при сбое агента.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from api.app import create_app
from api.state import AppState
from orchestrator.client import TaskFailedError
from orchestrator.messages import Result, Task

from tests.conftest import FakeConnection, ScriptedConnection, timeout_error


class ReadyConnection(FakeConnection):
    """Подключение, у которого всегда есть счётчики агентов."""

    def __init__(self) -> None:
        super().__init__([Result(task_id="stub", agent="classifier", success=True)])


def build_state(connection: Any) -> AppState:
    """Состояние приложения на подменённом подключении."""
    return AppState(
        nats_url="nats://test:4222",
        connection=connection,
        task_timeout=1.0,
    )


def working_connection() -> ScriptedConnection:
    """Подключение, отвечающее как исправные агенты."""

    def responder(subject: str, task: Task) -> Result:
        if subject.endswith("classify"):
            return Result(
                task_id=task.task_id,
                agent="classifier",
                instance="classifier-1",
                success=True,
                category="billing",
                priority=4,
                tags=["money"],
            )
        if subject.endswith("knowledge"):
            return Result(
                task_id=task.task_id,
                agent="knowledge",
                success=True,
                found=True,
                article_id=1,
                article_title="Не проходит оплата банковской картой",
                solution="Проверьте карту.",
                confidence=0.8,
            )
        if subject.endswith("answer"):
            return Result(
                task_id=task.task_id,
                agent="responder",
                success=True,
                answer="Проверьте срок действия карты.",
                answer_type="resolved",
            )
        return Result(task_id=task.task_id, agent="escalation", success=True)

    return ScriptedConnection(responder)


@pytest.fixture
def client() -> TestClient:
    """Клиент API с исправными агентами."""
    state = build_state(working_connection())
    return TestClient(create_app(state))


@pytest.fixture
def disconnected_client() -> TestClient:
    """Клиент API без соединения с NATS."""
    state = build_state(FakeConnection(connected=False))
    return TestClient(create_app(state))


class TestHealth:
    """Проверки служебного маршрута."""

    def test_reports_ready(self, client: TestClient) -> None:
        """При готовом соединении статус ok."""
        response = client.get("/health")

        assert response.status_code == 200
        assert response.json()["nats_connected"] is True

    def test_reports_disconnected(self, disconnected_client: TestClient) -> None:
        """Без NATS статус degraded, а не ошибка."""
        response = disconnected_client.get("/health")

        assert response.status_code == 200
        assert response.json()["status"] == "degraded"
        assert response.json()["nats_connected"] is False


class TestCreateTicket:
    """Проверки синхронной обработки обращения."""

    def test_returns_answer(self, client: TestClient) -> None:
        """Обращение возвращается с ответом клиенту."""
        response = client.post("/api/tickets", json={"text": "Не могу оплатить заказ"})

        assert response.status_code == 200

        body = response.json()
        assert body["success"] is True
        assert body["category"] == "billing"
        assert body["answer"] == "Проверьте срок действия карты."
        assert body["status"] == "completed"

    def test_response_matches_documentation(self, client: TestClient) -> None:
        """Тело ответа содержит поля, объявленные в схеме.

        Без проверки в ответе можно было бы не заметить, что поле
        assigned_to приходит как null там, где в схеме пустая строка.
        """
        body = client.post(
            "/api/tickets", json={"text": "Не могу оплатить заказ"}
        ).json()

        assert body["assigned_to"] == ""
        assert body["escalated"] is False
        assert body["sla_hours"] == 0
        assert isinstance(body["duration_ms"], float)

    def test_sets_timing_header(self, client: TestClient) -> None:
        """Middleware возвращает время обработки в заголовке."""
        response = client.post("/api/tickets", json={"text": "Не могу оплатить заказ"})

        assert "X-Process-Time-Ms" in response.headers

    def test_timeout_gives_504(self) -> None:
        """Молчащий агент приводит к 504."""
        state = build_state(
            ScriptedConnection(lambda subject, task: timeout_error("агент молчит"))
        )
        client = TestClient(create_app(state))

        response = client.post("/api/tickets", json={"text": "Не могу оплатить заказ"})

        assert response.status_code == 504
        assert response.json()["success"] is False
        assert "агент молчит" in response.json()["error"]

    def test_agent_error_gives_502(self) -> None:
        """Ошибка агента приводит к 502."""

        def failing(_: str, __: Task) -> Result:
            raise TaskFailedError("агент ответил ошибкой")

        client = TestClient(create_app(build_state(ScriptedConnection(failing))))

        response = client.post("/api/tickets", json={"text": "Не могу оплатить заказ"})

        assert response.status_code == 502

    def test_without_connection_gives_503(
        self, disconnected_client: TestClient
    ) -> None:
        """Без NATS запрос не падает с 500, а отвечает 503."""
        response = disconnected_client.post(
            "/api/tickets", json={"text": "Не могу оплатить заказ"}
        )

        assert response.status_code == 503


class TestValidation:
    """Проверки входных данных."""

    @pytest.mark.parametrize(
        "payload",
        [
            {"text": "ой"},
            {"text": ""},
            {},
            {"text": "нормальный текст", "лишнее": 1},
        ],
    )
    def test_rejects_bad_payload(
        self, client: TestClient, payload: dict[str, Any]
    ) -> None:
        """Некорректный запрос отклоняется с 422."""
        assert client.post("/api/tickets", json=payload).status_code == 422

    def test_rejects_too_long_text(self, client: TestClient) -> None:
        """Слишком длинное обращение отклоняется."""
        response = client.post("/api/tickets", json={"text": "а" * 2001})

        assert response.status_code == 422


class TestAsyncTicket:
    """Проверки асинхронного принятия обращения."""

    def test_returns_202_and_id(self, client: TestClient) -> None:
        """Обращение принимается в работу и получает номер."""
        response = client.post(
            "/api/tickets/async", json={"text": "Не могу оплатить заказ"}
        )

        assert response.status_code == 202

        body = response.json()
        assert body["status"] == "accepted"
        assert body["ticket_id"].startswith("ticket-")

    def test_result_can_be_fetched(self, client: TestClient) -> None:
        """Ответ асинхронно принятого обращения можно забрать."""
        ticket_id = client.post(
            "/api/tickets/async", json={"text": "Не могу оплатить заказ"}
        ).json()["ticket_id"]

        response = client.get(f"/api/tickets/{ticket_id}")

        assert response.status_code == 200
        assert response.json()["ticket_id"] == ticket_id
        assert response.json()["status"] in {"processing", "completed"}


class TestTicketsList:
    """Проверки списка обращений."""

    def test_lists_processed_tickets(self, client: TestClient) -> None:
        """Обработанные обращения попадают в список."""
        client.post("/api/tickets", json={"text": "Не могу оплатить заказ"})

        response = client.get("/api/tickets")

        assert response.status_code == 200
        assert response.json()["count"] >= 1

    def test_respects_limit(self, client: TestClient) -> None:
        """Параметр limit ограничивает выдачу."""
        for _ in range(3):
            client.post("/api/tickets", json={"text": "Не могу оплатить заказ"})

        response = client.get("/api/tickets", params={"limit": 2})

        assert response.json()["count"] == 2

    @pytest.mark.parametrize("limit", [0, 101, -1])
    def test_rejects_wrong_limit(self, client: TestClient, limit: int) -> None:
        """Недопустимый limit отклоняется."""
        assert client.get("/api/tickets", params={"limit": limit}).status_code == 422

    def test_unknown_ticket_gives_404(self, client: TestClient) -> None:
        """Неизвестный номер обращения даёт 404."""
        response = client.get("/api/tickets/ticket-нет")

        assert response.status_code == 404
        assert "не найдено" in response.json()["error"]


class TestAgents:
    """Проверки маршрута метрик."""

    def test_returns_counters(self, client: TestClient) -> None:
        """Ответ содержит метрики агентов и счётчики оркестратора."""
        client.post("/api/tickets", json={"text": "Не могу оплатить заказ"})

        response = client.get("/api/agents")

        assert response.status_code == 200

        body = response.json()
        assert body["orchestrator"]["tasks_sent"] == 3
        assert body["orchestrator"]["retries"] == 0


class TestDocumentation:
    """Проверки документации."""

    def test_openapi_is_generated(self, client: TestClient) -> None:
        """Схемы формируют OpenAPI, значит документация не устареет."""
        response = client.get("/openapi.json")

        assert response.status_code == 200

        paths = response.json()["paths"]
        assert "/api/tickets" in paths
        assert "/api/tickets/async" in paths
        assert "/health" in paths
