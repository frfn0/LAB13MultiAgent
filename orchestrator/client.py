"""Подключение к NATS и обмен заданиями с агентами.

Агенты публикуют результаты в общую тему ticket.result, поэтому
оркестратор подписывается на неё один раз и раздаёт ответы ожидающим
заданиям по идентификатору задания.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable
from typing import Any

import nats
from nats.aio.msg import Msg

from orchestrator.messages import SUBJECT_RESULT, Result, Task

logger = logging.getLogger(__name__)


class OrchestratorError(Exception):
    """Базовая ошибка оркестратора."""


class TaskTimeoutError(OrchestratorError):
    """Агент не ответил за отведённое время."""


class TaskFailedError(OrchestratorError):
    """Агент ответил, но сообщил об ошибке."""


class NatsConnection:
    """Подключение к NATS с подпиской на тему результатов."""

    def __init__(self, url: str) -> None:
        self._url = url
        self._nc: nats.NATS | None = None
        self._subscription: Any = None
        # Ожидающие задания: task_id -> future с результатом.
        self._pending: dict[str, asyncio.Future[Result]] = {}
        self._lock = asyncio.Lock()

    @property
    def connected(self) -> bool:
        """Установлено ли соединение с NATS."""
        return self._nc is not None and not self._nc.is_closed

    @property
    def client(self) -> nats.NATS:
        """Сырое подключение к NATS.

        Нужно подписчикам на дополнительные темы, например на метрики
        агентов. Обычный код пользуется send_task.

        Raises:
            OrchestratorError: если соединения ещё нет.
        """
        if self._nc is None:
            raise OrchestratorError("нет соединения с NATS")
        return self._nc

    async def connect(self) -> None:
        """Подключается к NATS и подписывается на результаты."""
        if self._nc is not None:
            return

        self._nc = await nats.connect(
            self._url,
            name="orchestrator",
            max_reconnect_attempts=-1,
            reconnect_time_wait=1,
            connect_timeout=5,
        )

        self._subscription = await self._nc.subscribe(
            SUBJECT_RESULT, cb=self._on_result
        )
        await self._nc.flush()

        logger.info("оркестратор подключён к %s", self._url)

    async def close(self) -> None:
        """Закрывает подписку и соединение."""
        if self._subscription is not None:
            try:
                await self._subscription.unsubscribe()
            except Exception as exc:  # noqa: BLE001 - отключение не должно падать
                logger.warning("не удалось снять подписку: %s", exc)
            self._subscription = None

        if self._nc is not None:
            try:
                await self._nc.drain()
            except Exception as exc:  # noqa: BLE001 - отключение не должно падать
                logger.warning("дренирование не завершилось: %s", exc)
            try:
                await self._nc.close()
            except Exception as exc:  # noqa: BLE001 - отключение не должно падать
                logger.warning("не удалось закрыть соединение: %s", exc)
            self._nc = None

        logger.info("оркестратор отключён")

    async def _on_result(self, msg: Msg) -> None:
        """Разбирает ответ агента и отдаёт его ожидающему заданию."""
        try:
            raw = json.loads(msg.data)
        except json.JSONDecodeError as exc:
            logger.error("ответ агента не является корректным JSON: %s", exc)
            return

        task_id = str(raw.get("task_id", ""))
        if not task_id:
            logger.error("в ответе агента нет task_id: %s", raw)
            return

        async with self._lock:
            future = self._pending.pop(task_id, None)

        if future is None or future.done():
            logger.warning("ответ без ожидающего задания: task_id=%s", task_id)
            return

        result = Result.model_validate(raw)
        logger.debug(
            "получен ответ task_id=%s agent=%s success=%s",
            task_id,
            result.agent,
            result.success,
        )
        future.set_result(result)

    async def send_task(
        self,
        subject: str,
        task: Task,
        timeout: float | None = None,
    ) -> Result:
        """Отправляет задание агенту и ждёт результат.

        Args:
            subject: тема с заданием.
            task: задание.
            timeout: сколько ждать ответа, секунды. None - берётся из настроек.

        Returns:
            Result: ответ агента.

        Raises:
            TaskTimeoutError: если за отведённое время ответа нет.
            TaskFailedError: если агент ответил ошибкой.
            OrchestratorError: если нет соединения с NATS.
        """
        if not self.connected:
            raise OrchestratorError("нет соединения с NATS")

        loop = asyncio.get_running_loop()
        future: asyncio.Future[Result] = loop.create_future()

        async with self._lock:
            self._pending[task.task_id] = future

        payload = json.dumps(task.model_dump()).encode()

        try:
            await self._nc.publish(subject, payload)  # type: ignore[union-attr]
            await self._nc.flush()  # type: ignore[union-attr]
        except Exception as exc:  # noqa: BLE001 - NATS бросает разные исключения
            async with self._lock:
                self._pending.pop(task.task_id, None)
            raise OrchestratorError(f"не удалось отправить задание: {exc}") from exc

        logger.debug("задание отправлено task_id=%s subject=%s", task.task_id, subject)

        try:
            result = await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError as exc:
            async with self._lock:
                self._pending.pop(task.task_id, None)
            raise TaskTimeoutError(
                f"агент не ответил на задание {task.task_id} за {timeout} сек"
            ) from exc

        if not result.success:
            raise TaskFailedError(
                f"агент {result.agent} вернул ошибку для {task.task_id}: {result.error}"
            )

        return result

    @property
    def pending_count(self) -> int:
        """Сколько заданий сейчас ожидают ответа."""
        return len(self._pending)

    async def wait_for_idle(self, timeout: float = 1.0) -> bool:
        """Ждёт, пока не останется заданий в ожидании.

        Нужно для проверок: позволяет убедиться, что все ответы получены.
        """
        deadline = asyncio.get_running_loop().time() + timeout
        while self.pending_count:
            if asyncio.get_running_loop().time() >= deadline:
                return False
            await asyncio.sleep(0.05)
        return True


def run(coro: Awaitable[Any]) -> Any:
    """Синхронная обёртка над asyncio.run для точки входа."""
    return asyncio.run(coro)
