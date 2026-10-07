"""Состояние API: подключение к NATS, метрики и результаты обращений.

Логика вынесена из FastAPI, чтобы её можно было проверять в тестах без
самого HTTP-слоя: состояние работает с тем же контрактом, что и
оркестратор из заданий 3-7.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from api.schemas import HealthResponse, TicketResponse
from orchestrator.client import (
    NatsConnection,
    OrchestratorError,
    TaskFailedError,
    TaskTimeoutError,
)
from orchestrator.config import get_retry_policy, get_settings
from orchestrator.metrics import MetricsCollector
from orchestrator.pipeline import new_ticket, run_pipeline

logger = logging.getLogger(__name__)

# Сколько последних обращений держим в памяти. Больше держать незачем:
# хранилище нужно, чтобы забрать ответ асинхронно принятого обращения.
MAX_TICKETS = 200


class StateError(OrchestratorError):
    """Приложение не готово принимать запросы."""


class AppState:
    """Подключение к NATS, счётчики агентов и результаты обращений."""

    def __init__(
        self,
        nats_url: str | None = None,
        connection: NatsConnection | None = None,
        metrics: MetricsCollector | None = None,
        task_timeout: float | None = None,
    ) -> None:
        settings = get_settings()
        self._url = nats_url or settings.nats_url
        self._connection = connection or NatsConnection(self._url)
        self._metrics = metrics or MetricsCollector()
        self._timeout = (
            task_timeout if task_timeout is not None else settings.task_timeout
        )
        self._policy = get_retry_policy()
        self._tickets: dict[str, dict[str, Any]] = {}
        self._order: list[str] = []
        self._processed = 0

    # Подключение -----------------------------------------------------------

    async def connect(self) -> None:
        """Подключается к NATS и подписывается на метрики агентов."""
        try:
            await self._connection.connect()
            await self._metrics.subscribe(self._connection.client)
        except Exception as exc:  # noqa: BLE001 - приложение должно сказать причину
            raise StateError(
                f"не удалось подключиться к NATS {self._url}: {exc}"
            ) from exc

        logger.info("API готов принимать обращения")

    async def close(self) -> None:
        """Закрывает соединение с NATS."""
        await self._connection.close()

    def require_ready(self) -> None:
        """Проверяет, что приложение готово принимать обращения.

        Raises:
            StateError: если соединения нет.
        """
        if not self._connection.connected:
            raise StateError("нет соединения с NATS")

    # Обработка обращений ---------------------------------------------------

    async def process(self, text: str) -> tuple[dict[str, Any], int]:
        """Проводит обращение через агентов и возвращает отчёт и код ответа.

        Args:
            text: текст обращения клиента.

        Returns:
            Пара: тело ответа и HTTP-код. Код 504 означает таймаут агента,
            502 - ошибку агента, 200 - успех.

        Raises:
            StateError: если нет соединения с NATS.
        """
        self.require_ready()

        begin = time.perf_counter()
        try:
            report = await run_pipeline(
                self._connection,
                text,
                self._timeout,
                self._metrics,
                self._policy,
            )
        except TaskTimeoutError as exc:
            logger.error("таймаут агента: %s", exc)
            return self._failure(text, str(exc)), 504
        except TaskFailedError as exc:
            logger.error("агент ответил ошибкой: %s", exc)
            return self._failure(text, str(exc)), 502

        duration_ms = (time.perf_counter() - begin) * 1000
        report["duration_ms"] = round(duration_ms, 2)
        report["status"] = "completed"
        # Ответ прогоняется через схему, иначе в теле запроса не хватает
        # полей, объявленных в документации: например, assigned_to остаётся
        # пустым без эскалации, а не становится пустой строкой.
        report = TicketResponse.model_validate(report).model_dump()
        self._remember(report)

        logger.info(
            "обращение %s обработано за %.1f мс, ответ: %s",
            report.get("ticket_id"),
            duration_ms,
            report.get("answer_type"),
        )
        return report, 200

    async def process_async(self, text: str) -> dict[str, Any]:
        """Принимает обращение в работу и обрабатывает его в фоне.

        Args:
            text: текст обращения клиента.

        Returns:
            Минимальный отчёт с идентификатором обращения и статусом
            "processing". Полный результат появится по этому
            идентификатору через `ticket`.

        Raises:
            StateError: если нет соединения с NATS.
        """
        self.require_ready()

        ticket = new_ticket(text)
        pending: dict[str, Any] = {
            "ticket_id": ticket.id,
            "text": text,
            "success": False,
            "status": "processing",
            "answer": "",
            "steps": [],
        }
        self._remember(pending)

        asyncio.create_task(self._process_in_background(ticket.id, text))
        return pending

    async def _process_in_background(self, ticket_id: str, text: str) -> None:
        """Обрабатывает обращение в фоне и обновляет его в хранилище."""
        try:
            report, _code = await self.process(text)
        except Exception as exc:  # noqa: BLE001 - фоновая задача не должна падать
            logger.exception("фоновая обработка %s не удалась", ticket_id)
            report = self._failure(text, str(exc))
            report["ticket_id"] = ticket_id

        report["ticket_id"] = ticket_id
        self._remember(report)

    # Хранилище результатов -------------------------------------------------

    def _remember(self, report: dict[str, Any]) -> None:
        """Сохраняет отчёт, вытесняя самые старые при переполнении."""
        ticket_id = str(report.get("ticket_id") or "")
        if not ticket_id:
            return

        if ticket_id not in self._tickets:
            self._order.append(ticket_id)

        self._tickets[ticket_id] = report
        self._processed += 1

        while len(self._order) > MAX_TICKETS:
            oldest = self._order.pop(0)
            self._tickets.pop(oldest, None)

    def ticket(self, ticket_id: str) -> dict[str, Any] | None:
        """Возвращает отчёт по обращению или None."""
        return self._tickets.get(ticket_id)

    async def tickets(self, limit: int = 20) -> dict[str, Any]:
        """Возвращает последние обращения, свежие первыми.

        Args:
            limit: сколько обращений вернуть.

        Returns:
            Словарь с количеством и списком обращений.
        """
        identifiers = list(reversed(self._order))[:limit]
        items = [self._tickets[item] for item in identifiers if item in self._tickets]
        return {"count": len(items), "items": items}

    # Служебное -------------------------------------------------------------

    async def health(self) -> Any:
        """Состояние API, NATS и агентов."""
        agents = [snapshot["agent"] for snapshot in self._metrics.agents()]
        return HealthResponse(
            status="ok" if self._connection.connected else "degraded",
            nats_connected=self._connection.connected,
            nats_url=self._url,
            agents=len(agents),
            agents_list=sorted(set(agents)),
            tickets_processed=self._processed,
        )

    async def agents(self) -> dict[str, Any]:
        """Метрики агентов и счётчики оркестратора."""
        totals = self._metrics.totals()
        return {
            "count": len(totals["agents"]),
            "items": self._metrics.agents(),
            "orchestrator": totals["orchestrator"],
        }

    def _failure(self, text: str, error: str) -> dict[str, Any]:
        """Тело ответа при сбое конвейера.

        Тело проходит через ту же схему, что и успешный ответ: клиенту не
        нужно знать, чем заполнены поля, которых не было в успешном ответе.
        """
        logger.error("обращение не обработано: %s", error)
        report = {
            "ticket_id": "",
            "text": text,
            "success": False,
            "status": "failed",
            "error": error,
        }
        return TicketResponse.model_validate(report).model_dump()
