"""Тесты конвейера обработки обращения (задания 3 и 4)."""

from __future__ import annotations

import pytest

from orchestrator.client import TaskTimeoutError
from orchestrator.messages import (
    CATEGORY_BILLING,
    CATEGORY_OTHER,
    SUBJECT_ANSWER,
    SUBJECT_CLASSIFY,
    SUBJECT_ESCALATE,
    SUBJECT_KNOWLEDGE,
    Result,
    Task,
)
from orchestrator.metrics import MetricsCollector
from orchestrator.pipeline import new_ticket, run_pipeline, run_pipeline_safe

from tests.conftest import ScriptedConnection, timeout_error


def build_connection(
    category: str = CATEGORY_BILLING,
    found: bool = True,
    priority: int = 4,
    escalate: bool = False,
) -> ScriptedConnection:
    """Подключение, отвечающее как агенты при заданных входных данных."""

    def responder(subject: str, task: Task) -> Result:
        if subject == SUBJECT_CLASSIFY:
            return Result(
                task_id=task.task_id,
                agent="classifier",
                instance="classifier-1",
                success=True,
                category=category,
                priority=priority,
                tags=["money"],
            )

        if subject == SUBJECT_KNOWLEDGE:
            if not found:
                return Result(
                    task_id=task.task_id,
                    agent="knowledge",
                    success=True,
                    found=False,
                    confidence=0.1,
                )
            return Result(
                task_id=task.task_id,
                agent="knowledge",
                success=True,
                found=True,
                article_id=1,
                article_title="Не проходит оплата банковской картой",
                solution="Проверьте срок действия карты.",
                confidence=0.8,
            )

        if subject == SUBJECT_ANSWER:
            return Result(
                task_id=task.task_id,
                agent="responder",
                instance="responder-1",
                success=True,
                answer="Проверьте срок действия карты.",
                answer_type="resolved",
            )

        if subject == SUBJECT_ESCALATE:
            return Result(
                task_id=task.task_id,
                agent="escalation",
                success=True,
                escalation_id="ESC-1",
                assigned_to="старший-специалист",
                sla_hours=24,
                urgent=True,
                answer="Обращение передано специалисту.",
                answer_type="deferred",
            )

        raise AssertionError(f"неожиданная тема {subject}")

    return ScriptedConnection(responder)


class TestNewTicket:
    """Проверки создания обращения."""

    def test_ids_are_unique(self) -> None:
        """Два обращения не должны получить один номер."""
        first = new_ticket("первое")
        second = new_ticket("первое")

        assert first.id != second.id

    def test_id_has_prefix(self) -> None:
        """Номер обращения начинается с ticket-."""
        assert new_ticket("текст").id.startswith("ticket-")


class TestRunPipeline:
    """Проверки успешного пути конвейера."""

    async def test_three_steps(self) -> None:
        """Типовое обращение проходит три шага."""
        connection = build_connection()
        metrics = MetricsCollector()

        report = await run_pipeline(connection, "не проходит оплата", 5.0, metrics)

        assert report["success"] is True
        assert report["category"] == CATEGORY_BILLING
        assert report["article_title"] == "Не проходит оплата банковской картой"
        assert report["answer_type"] == "resolved"
        assert report["escalated"] is False

        # Шаг эскалации попадает в отчёт всегда, даже когда пропущен:
        # по отчёту видно весь маршрут обращения.
        steps = [step["step"] for step in report["steps"]]
        assert steps == ["classify", "knowledge", "answer", "escalate"]
        assert report["steps"][-1].get("skipped") is True

        performed = [
            step["step"] for step in report["steps"] if not step.get("skipped")
        ]
        assert performed == ["classify", "knowledge", "answer"]

    async def test_topics_in_order(self) -> None:
        """Шаги идут в правильном порядке."""
        connection = build_connection()

        await run_pipeline(connection, "не проходит оплата", 5.0)

        assert connection.subjects == [
            SUBJECT_CLASSIFY,
            SUBJECT_KNOWLEDGE,
            SUBJECT_ANSWER,
        ]

    async def test_counts_every_attempt(self) -> None:
        """Счётчик отправок считает попытки: здесь их три."""
        connection = build_connection()
        metrics = MetricsCollector()

        await run_pipeline(connection, "не проходит оплата", 5.0, metrics)

        assert metrics.tasks_sent == 3

    async def test_escalation_when_article_not_found(self) -> None:
        """Без статьи обращение уходит человеку."""
        connection = build_connection(found=False, escalate=True)

        report = await run_pipeline(connection, "хочу обсудить тариф", 5.0)

        assert report["escalated"] is True
        assert report["assigned_to"] == "старший-специалист"
        assert report["sla_hours"] == 24
        assert SUBJECT_ESCALATE in connection.subjects

    async def test_escalation_replaces_client_answer(self) -> None:
        """При эскалации клиенту не показывают автоматический ответ.

        Обращение ушло человеку, и говорить «решено» было бы неправдой.
        """
        connection = build_connection(found=False, escalate=True)

        report = await run_pipeline(connection, "хочу обсудить тариф", 5.0)

        assert report["answer"] == "Обращение передано специалисту."
        assert report["answer_type"] == "deferred"

    async def test_escalation_for_unknown_category(self) -> None:
        """Нераспознанная категория тоже эскалируется."""
        connection = build_connection(
            category=CATEGORY_OTHER, found=False, priority=1, escalate=True
        )

        report = await run_pipeline(connection, "здрасте, вопрос", 5.0)

        assert report["escalated"] is True

    async def test_escalation_skipped_when_not_needed(self) -> None:
        """При найденной статье шаг эскалации пропускается."""
        connection = build_connection()

        report = await run_pipeline(connection, "не проходит оплата", 5.0)

        assert SUBJECT_ESCALATE not in connection.subjects
        assert any(step.get("skipped") for step in report["steps"])


class TestRunPipelineSafe:
    """Проверки того, что сбои не поднимаются наружу."""

    async def test_timeout_is_reported(self) -> None:
        """Таймаут попадает в отчёт, а не наружу."""

        def responder(_: str, __: Task) -> Result:
            raise timeout_error("агент не ответил")

        connection = ScriptedConnection(responder)
        metrics = MetricsCollector()

        report = await run_pipeline_safe(connection, "текст обращения", 1.0, metrics)

        assert report["success"] is False
        assert "агент не ответил" in report["error"]
        assert metrics.tasks_failed == 1

    async def test_timeout_counted(self) -> None:
        """Каждый таймаут попадает в счётчик."""
        attempts = {"count": 0}

        def responder(_: str, __: Task) -> Result:
            attempts["count"] += 1
            raise TaskTimeoutError(f"попытка {attempts['count']}")

        connection = ScriptedConnection(responder)
        metrics = MetricsCollector()

        await run_pipeline_safe(connection, "текст обращения", 1.0, metrics)

        # Три попытки по одному таймауту, но провален только один шаг.
        assert metrics.timeouts == 3
        assert metrics.tasks_failed == 1
        assert metrics.retries == 2

    async def test_ticket_id_survives_failure(self) -> None:
        """В отчёте остаётся текст обращения."""

        def responder(_: str, __: Task) -> Result:
            raise timeout_error()

        connection = ScriptedConnection(responder)

        report = await run_pipeline_safe(connection, "моё обращение", 1.0)

        assert report["text"] == "моё обращение"
        assert report["ticket_id"] is None


class TestMetricsOptional:
    """Конвейер должен работать и без счётчиков."""

    async def test_runs_without_metrics(self) -> None:
        """Демонстрации передают счётчики, но конвейер от них не зависит."""
        connection = build_connection()

        report = await run_pipeline(connection, "не проходит оплата", 5.0, None)

        assert report["success"] is True


@pytest.mark.parametrize(
    "text",
    ["не проходит оплата", "курьер не пришёл", "не проходит оплата картой"],
)
async def test_pipeline_is_deterministic(text: str) -> None:
    """Одинаковый вход даёт одинаковый результат.

    Агент генерации ответов детерминированный: это проверяется здесь на
    уровне конвейера, где собираются все шаги.
    """
    first = await run_pipeline(build_connection(), text, 5.0)
    second = await run_pipeline(build_connection(), text, 5.0)

    assert first["answer"] == second["answer"]
    assert first["category"] == second["category"]
    assert first["escalated"] == second["escalated"]
