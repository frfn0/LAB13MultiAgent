"""Тесты повторной отправки заданий (задание 6)."""

from __future__ import annotations

import pytest

from orchestrator.client import TaskFailedError, TaskTimeoutError
from orchestrator.messages import Task, Ticket
from orchestrator.metrics import MetricsCollector
from orchestrator.retry import RetryPolicy, is_retryable, send_with_retry

from tests.conftest import FakeConnection, agent_error, timeout_error


def make_task() -> Task:
    """Задание для отправки."""
    return Task(
        task_id="task-1",
        type="classify",
        ticket=Ticket(id="ticket-1", text="не проходит оплата"),
    )


def success(task_id: str = "task-1") -> object:
    """Успешный ответ агента."""
    from orchestrator.messages import Result

    return Result(task_id=task_id, agent="classifier", success=True, category="billing")


class TestRetryPolicy:
    """Проверки параметров повторов."""

    def test_limits_three_attempts_by_default(self) -> None:
        """Методичка требует не более трёх попыток."""
        assert RetryPolicy().max_attempts == 3

    def test_rejects_zero_attempts(self) -> None:
        """Ноль попыток не имеет смысла: задание не уйдёт вовсе."""
        with pytest.raises(ValueError, match="max_attempts"):
            RetryPolicy(max_attempts=0)

    def test_rejects_backoff_below_one(self) -> None:
        """Пауза не должна уменьшаться от попытки к попытке."""
        with pytest.raises(ValueError, match="backoff"):
            RetryPolicy(backoff=0.5)

    def test_rejects_jitter_out_of_range(self) -> None:
        """Доля разброса задаётся от 0 до 1."""
        with pytest.raises(ValueError, match="jitter"):
            RetryPolicy(jitter=1.5)

    def test_delay_grows_between_attempts(self) -> None:
        """Пауза растёт геометрически."""
        policy = RetryPolicy(initial_delay=0.5, backoff=2.0, jitter=0)

        assert policy.delay_for(1) == pytest.approx(0.5)
        assert policy.delay_for(2) == pytest.approx(1.0)
        assert policy.delay_for(3) == pytest.approx(2.0)

    def test_delay_capped_by_max_delay(self) -> None:
        """Пауза не растёт бесконечно."""
        policy = RetryPolicy(initial_delay=1.0, backoff=10.0, max_delay=3.0, jitter=0)

        assert policy.delay_for(4) == 3.0

    def test_delay_within_jitter_bounds(self) -> None:
        """Разброс не выходит за границы доли."""
        policy = RetryPolicy(initial_delay=1.0, backoff=1.0, jitter=0.2)

        for _ in range(50):
            delay = policy.delay_for(1)
            assert 0.8 <= delay <= 1.2

    def test_delay_rejects_attempt_below_one(self) -> None:
        """Нумерация попыток начинается с единицы."""
        with pytest.raises(ValueError, match="attempt"):
            RetryPolicy().delay_for(0)


class TestIsRetryable:
    """Проверки того, что стоит повторять."""

    def test_timeout_is_retryable(self) -> None:
        """Молчание агента - временная проблема."""
        assert is_retryable(timeout_error(), RetryPolicy())

    def test_transport_error_is_retryable(self) -> None:
        """Обрыв связи - временная проблема."""
        from orchestrator.client import OrchestratorError

        assert is_retryable(OrchestratorError("нет соединения"), RetryPolicy())

    def test_agent_error_depends_on_policy(self) -> None:
        """Ошибку агента можно как повторять, так и нет."""
        error = agent_error()

        assert is_retryable(error, RetryPolicy(retry_on_agent_error=True))
        assert not is_retryable(error, RetryPolicy(retry_on_agent_error=False))


class TestSendWithRetry:
    """Проверки самой повторной отправки."""

    async def test_success_on_first_attempt(self) -> None:
        """Успешный ответ не приводит к повторам."""
        connection = FakeConnection([success()])
        metrics = MetricsCollector()

        result = await send_with_retry(
            connection, "ticket.classify", make_task(), 5.0, None, metrics
        )

        assert result.success
        assert len(connection.attempts) == 1
        assert metrics.retries == 0
        assert metrics.tasks_sent == 1

    async def test_retry_after_timeout(self) -> None:
        """Таймаут первой попытки приводит к повтору."""
        connection = FakeConnection([timeout_error(), success()])
        metrics = MetricsCollector()

        result = await send_with_retry(
            connection, "ticket.classify", make_task(), 5.0, None, metrics
        )

        assert result.success
        assert len(connection.attempts) == 2
        assert metrics.retries == 1
        # Счётчик отправок считает попытки, а не задания.
        assert metrics.tasks_sent == 2

    async def test_retry_after_agent_error(self) -> None:
        """Ошибка агента тоже приводит к повтору."""
        connection = FakeConnection([agent_error(), success()])

        result = await send_with_retry(connection, "ticket.classify", make_task(), 5.0)

        assert result.success
        assert len(connection.attempts) == 2

    async def test_no_retry_when_policy_forbids(self) -> None:
        """При запрете повторов ошибка агента пробрасывается сразу."""
        connection = FakeConnection([agent_error()])
        policy = RetryPolicy(retry_on_agent_error=False)

        with pytest.raises(TaskFailedError):
            await send_with_retry(
                connection, "ticket.classify", make_task(), 5.0, policy
            )

        assert len(connection.attempts) == 1

    async def test_attempts_limited_by_policy(self) -> None:
        """После трёх неудач задание сдаётся."""
        connection = FakeConnection([timeout_error()])
        metrics = MetricsCollector()

        with pytest.raises(TaskTimeoutError):
            await send_with_retry(
                connection, "ticket.classify", make_task(), 5.0, None, metrics
            )

        assert len(connection.attempts) == 3
        assert metrics.retries == 2
        assert metrics.timeouts == 3

    async def test_same_task_id_on_every_attempt(self) -> None:
        """Повтор отправляет то же задание: агент узнаёт повтор по id."""
        connection = FakeConnection([timeout_error(), timeout_error(), success()])

        await send_with_retry(connection, "ticket.classify", make_task(), 5.0)

        assert connection.attempts == ["task-1", "task-1", "task-1"]

    async def test_timeout_passed_to_every_attempt(self) -> None:
        """Таймаут применяется к каждой попытке отдельно."""
        connection = FakeConnection([timeout_error(), success()])

        await send_with_retry(connection, "ticket.classify", make_task(), 1.5)

        assert connection.timeouts == [1.5, 1.5]

    async def test_subject_unchanged_between_attempts(self) -> None:
        """Повтор идёт в ту же тему."""
        connection = FakeConnection([timeout_error(), success()])

        await send_with_retry(connection, "ticket.answer", make_task(), 5.0)

        assert connection.subjects == ["ticket.answer", "ticket.answer"]

    async def test_last_error_is_raised(self) -> None:
        """Наружу поднимается именно та ошибка, на которой остановились."""
        connection = FakeConnection(
            [timeout_error("первый сбой"), agent_error("второй сбой")]
        )

        with pytest.raises(TaskFailedError, match="второй сбой"):
            await send_with_retry(connection, "ticket.classify", make_task(), 5.0)

    async def test_works_without_metrics(self) -> None:
        """Счётчики необязательны: демонстрации их не всегда передают."""
        connection = FakeConnection([timeout_error(), success()])

        result = await send_with_retry(connection, "ticket.classify", make_task(), 5.0)

        assert result.success

    async def test_default_policy_is_three_attempts(self) -> None:
        """Без явной политики действует политика по умолчанию."""
        connection = FakeConnection([timeout_error()])

        with pytest.raises(TaskTimeoutError):
            await send_with_retry(connection, "ticket.classify", make_task(), 5.0)

        assert len(connection.attempts) == 3
