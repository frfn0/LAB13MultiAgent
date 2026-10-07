"""REST API для запуска задач.

Поток запроса: HTTP -> оркестратор -> агенты -> ответ.

Приложение само подключается к NATS при старте, подписывается на метрики
агентов и держит в памяти результаты последних обращений, чтобы клиент мог
забрать ответ позже.
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from api.schemas import (
    ErrorResponse,
    HealthResponse,
    TicketAcceptedResponse,
    TicketCreate,
    TicketListResponse,
    TicketResponse,
)
from api.state import AppState, StateError

logger = logging.getLogger(__name__)

TITLE = "Лабораторная работа №13: API запуска задач"
DESCRIPTION = (
    "Принимает обращения клиентов, проводит их через агентов "
    "классификации, поиска по базе знаний и генерации ответа, "
    "возвращает ответ клиенту."
)


def create_app(state: AppState | None = None) -> FastAPI:
    """Создаёт приложение API.

    Args:
        state: состояние приложения. Если не передано, оно создаётся при
            старте и подключается к NATS. В тестах передают состояние
            с подменёнными зависимостями.

    Returns:
        Готовое приложение FastAPI.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Подключает и отключает NATS вместе с приложением."""
        own_state = state is None
        current = state or AppState()

        app.state.state = current

        if own_state:
            await current.connect()

        try:
            yield
        finally:
            if own_state:
                await current.close()

    app = FastAPI(
        title=TITLE,
        description=DESCRIPTION,
        version="1.0.0",
        lifespan=lifespan,
    )

    current = state

    def active_state() -> AppState:
        """Состояние приложения на момент запроса."""
        # При обычном запуске состояние появляется в lifespan, при тестах
        # его передали заранее.
        return current or app.state.state

    @app.exception_handler(StateError)
    async def state_error_handler(_: Request, exc: StateError) -> JSONResponse:
        """NATS недоступен - отвечаем 503, а не падаем с 500."""
        logger.error("состояние приложения недоступно: %s", exc)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=ErrorResponse(
                error=str(exc), detail="состояние недоступно"
            ).model_dump(),
        )

    @app.middleware("http")
    async def timing_middleware(request: Request, call_next: Any) -> Any:
        """Логирует каждый запрос и время его обработки.

        Время ответа возвращается и в заголовке X-Process-Time-Ms: клиенту
        полезно знать, сколько занял ответ, не открывая тело ответа.
        """
        begin = time.perf_counter()
        response = await call_next(request)
        duration_ms = (time.perf_counter() - begin) * 1000

        logger.info(
            "%s %s -> %s за %.1f мс",
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
        )
        response.headers["X-Process-Time-Ms"] = f"{duration_ms:.1f}"
        return response

    @app.get("/health", response_model=HealthResponse, tags=["служебное"])
    async def health() -> HealthResponse:
        """Проверяет, что API работает и агенты на месте."""
        return await active_state().health()

    @app.get(
        "/api/agents",
        response_model=TicketListResponse,
        tags=["служебное"],
    )
    async def agents() -> Any:
        """Отдаёт метрики агентов из темы agent.metrics."""
        return await active_state().agents()

    @app.post(
        "/api/tickets",
        response_model=TicketResponse,
        status_code=status.HTTP_200_OK,
        responses={
            status.HTTP_504_GATEWAY_TIMEOUT: {
                "model": ErrorResponse,
                "description": "Агент не ответил за отведённое время",
            },
            status.HTTP_502_BAD_GATEWAY: {
                "model": ErrorResponse,
                "description": "Агент ответил ошибкой",
            },
        },
        tags=["обращения"],
    )
    async def create_ticket(payload: TicketCreate) -> Any:
        """Проводит обращение через всех агентов и возвращает ответ."""
        report, status_code = await active_state().process(payload.text)
        return JSONResponse(
            status_code=status_code,
            content=report,
        )

    @app.post(
        "/api/tickets/async",
        response_model=TicketAcceptedResponse,
        status_code=status.HTTP_202_ACCEPTED,
        tags=["обращения"],
    )
    async def create_ticket_async(payload: TicketCreate) -> TicketAcceptedResponse:
        """Принимает обращение в работу и возвращает его идентификатор.

        Ответ клиенту можно забрать позже запросом
        `GET /api/tickets/{ticket_id}`.
        """
        ticket = await active_state().process_async(payload.text)
        return TicketAcceptedResponse(
            ticket_id=ticket["ticket_id"],
            status="accepted",
            message="Обращение принято в работу, ответ можно забрать по /api/tickets/{ticket_id}",
        )

    @app.get(
        "/api/tickets",
        response_model=TicketListResponse,
        tags=["обращения"],
    )
    async def list_tickets(limit: int = 20) -> Any:
        """Отдаёт последние обработанные обращения."""
        if not 1 <= limit <= 100:
            return JSONResponse(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                content=ErrorResponse(
                    error="limit должен быть от 1 до 100",
                    detail="неверный параметр",
                ).model_dump(),
            )
        return await active_state().tickets(limit)

    @app.get(
        "/api/tickets/{ticket_id}",
        response_model=TicketResponse,
        responses={status.HTTP_404_NOT_FOUND: {"model": ErrorResponse}},
        tags=["обращения"],
    )
    async def get_ticket(ticket_id: str) -> Any:
        """Отдаёт результат одного обращения."""
        ticket = active_state().ticket(ticket_id)
        if ticket is None:
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content=ErrorResponse(
                    error=f"обращение {ticket_id} не найдено",
                    detail="не найдено",
                ).model_dump(),
            )
        return ticket

    return app


app = create_app()
