"""Повторная отправка задания при сбое агента.

Методичка требует повторять задание при сбое агента не более трёх раз.
Здесь это сделано так:

- всего попыток не больше `max_attempts` (по умолчанию 3, включая первую);
- перед повтором делается пауза, растущая геометрически
  (`initial_delay`, `backoff`, ограничение `max_delay`) и слегка
  разбросанная, чтобы повторы нескольких обращений не совпали по времени;
- повтор делается и при таймауте, и при ошибке агента: «сбой агента» —
  это и молчание, и явная ошибка в ответе;
- при исчерпании попыток наружу поднимается последняя ошибка, чтобы
  конвейер не потерял её причину.

Отдельно стоит сказать, что повтор на любую ошибку агента — это осознанное
упрощение: некорректное задание повторять бессмысленно. В продакшене стоит
разделять постоянные ошибки (плохой JSON, нет идентификатора тикета) и
временные (перегрузка, недоступность), и повторять только вторые.
"""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass

from orchestrator.client import (
    NatsConnection,
    OrchestratorError,
    TaskFailedError,
    TaskTimeoutError,
)
from orchestrator.messages import Result, Task
from orchestrator.metrics import MetricsCollector

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RetryPolicy:
    """Параметры повторной отправки задания.

    Attributes:
        max_attempts: сколько попыток отправки всего, включая первую.
            Методичка требует «не более 3 раз», поэтому по умолчанию 3:
            одна первая попытка и две повторные.
        initial_delay: пауза перед первым повтором, секунды.
        backoff: во сколько раз увеличивается пауза перед каждым
            следующим повтором.
        max_delay: верхняя граница паузы, секунды.
        jitter: доля случайного разброса паузы, от 0 до 1.
        retry_on_agent_error: повторять ли задание, если агент ответил
            ошибкой, а не промолчал.
    """

    max_attempts: int = 3
    initial_delay: float = 0.5
    backoff: float = 2.0
    max_delay: float = 5.0
    jitter: float = 0.1
    retry_on_agent_error: bool = True

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts должен быть не меньше 1")
        if self.initial_delay < 0:
            raise ValueError("initial_delay не может быть отрицательным")
        if self.backoff < 1:
            raise ValueError("backoff должен быть не меньше 1")
        if self.max_delay < 0:
            raise ValueError("max_delay не может быть отрицательным")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter должен быть от 0 до 1")

    def delay_for(self, attempt: int) -> float:
        """Пауза перед повтором, когда попытка `attempt` не удалась.

        Args:
            attempt: номер неудачной попытки, начиная с 1.

        Returns:
            Длительность паузы в секундах с небольшим разбросом.

        Raises:
            ValueError: если `attempt` меньше 1.
        """
        if attempt < 1:
            raise ValueError("attempt должен быть не меньше 1")

        base = min(self.initial_delay * (self.backoff ** (attempt - 1)), self.max_delay)
        if self.jitter == 0:
            return base

        spread = base * self.jitter
        return max(0.0, base + random.uniform(-spread, spread))


def is_retryable(exc: Exception, policy: RetryPolicy) -> bool:
    """Стоит ли повторять задание после этой ошибки.

    Таймаут и обрыв связи — всегда временные. Ошибка агента повторяется
    только если это разрешено политикой.

    Args:
        exc: ошибка, полученная при отправке задания.
        policy: параметры повтора.

    Returns:
        True, если задание имеет смысл отправить ещё раз.
    """
    if isinstance(exc, TaskFailedError):
        return policy.retry_on_agent_error
    return isinstance(exc, OrchestratorError)


async def send_with_retry(
    connection: NatsConnection,
    subject: str,
    task: Task,
    timeout: float,
    policy: RetryPolicy | None = None,
    metrics: MetricsCollector | None = None,
) -> Result:
    """Отправляет задание агенту и повторяет отправку при сбое.

    Args:
        connection: подключение к NATS.
        subject: тема с заданием.
        task: задание. При повторе отправляется то же самое задание с тем же
            `task_id`: агент узнаёт повтор по идентификатору.
        timeout: сколько ждать ответа на одну попытку, секунды.
        policy: параметры повтора. По умолчанию — стандартные.
        metrics: счётчики, в которые попадают попытки и повторы.

    Returns:
        Result: успешный ответ агента.

    Raises:
        TaskTimeoutError: агент не ответил ни разу за отведённое время.
        TaskFailedError: агент ответил ошибкой на всех попытках.
        OrchestratorError: не удалось отправить задание или нет соединения.
    """
    policy = policy or RetryPolicy()
    last_error: Exception | None = None

    for attempt in range(1, policy.max_attempts + 1):
        if metrics is not None:
            # Считаем именно попытки: одно задание может занять три попытки.
            metrics.count_sent()

        try:
            result = await connection.send_task(subject, task, timeout=timeout)
        except (OrchestratorError, asyncio.TimeoutError) as exc:
            last_error = exc
            if metrics is not None and isinstance(exc, TaskTimeoutError):
                # Считаем каждый таймаут, а не только последний: иначе
                # повторы, которые в итоге увенчались успехом, были бы
                # не видны в мониторинге.
                metrics.count_timeout()
            logger.warning(
                "попытка %d/%d не удалась: %s",
                attempt,
                policy.max_attempts,
                exc,
            )

            if attempt >= policy.max_attempts or not is_retryable(exc, policy):
                break

            delay = policy.delay_for(attempt)
            logger.info(
                "повтор через %.2f с (попытка %d из %d)",
                delay,
                attempt + 1,
                policy.max_attempts,
            )
            if metrics is not None:
                metrics.count_retry()
            await asyncio.sleep(delay)
            continue

        if attempt > 1:
            logger.info("задание выполнено с попытки %d", attempt)
        return result

    if last_error is not None:
        raise last_error

    # Сюда попасть не должно: цикл выполняется хотя бы один раз.
    raise OrchestratorError("задание не отправлено ни разу")
