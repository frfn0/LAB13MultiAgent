"""Тесты контракта сообщений на стороне Python.

Контракт должен совпадать с pkg/messages/messages.go на стороне Go: те же
имена полей и те же значения констант. Расхождение здесь означает, что
агенты и оркестратор перестанут понимать друг друга.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from orchestrator import messages


class TestSubjects:
    """Проверки тем NATS."""

    def test_subjects_match_go_contract(self) -> None:
        """Темы зафиксированы в agents/AGENTS.md."""
        assert messages.SUBJECT_CLASSIFY == "ticket.classify"
        assert messages.SUBJECT_KNOWLEDGE == "ticket.knowledge"
        assert messages.SUBJECT_ANSWER == "ticket.answer"
        assert messages.SUBJECT_ESCALATE == "ticket.escalate"
        assert messages.SUBJECT_RESULT == "ticket.result"
        assert messages.SUBJECT_METRICS == "agent.metrics"

    def test_topics_are_distinct(self) -> None:
        """У каждого шага своя тема, иначе агенты мешают друг другу."""
        subjects = [
            messages.SUBJECT_CLASSIFY,
            messages.SUBJECT_KNOWLEDGE,
            messages.SUBJECT_ANSWER,
            messages.SUBJECT_ESCALATE,
            messages.SUBJECT_RESULT,
            messages.SUBJECT_METRICS,
        ]

        assert len(set(subjects)) == len(subjects)


class TestTask:
    """Проверки модели задания."""

    def test_minimal_task(self) -> None:
        """Задание создаётся только из обязательных полей."""
        task = messages.Task(
            task_id="task-1",
            type=messages.TASK_CLASSIFY,
            ticket=messages.Ticket(id="ticket-1", text="текст"),
        )

        assert task.priority == 0
        assert task.tags == []
        assert task.found is False
        assert task.attempts == 1

    def test_rejects_unknown_field(self) -> None:
        """Лишнее поле в задании означает ошибку в контракте."""
        with pytest.raises(ValidationError):
            messages.Task(
                task_id="task-1",
                type=messages.TASK_CLASSIFY,
                ticket=messages.Ticket(id="ticket-1", text="текст"),
                unexpected="поле",
            )

    def test_allows_empty_ticket_id(self) -> None:
        """Модель не проверяет номер обращения, это делает агент.

        Проверка живёт на стороне Go в handleMessage: задание без
        номера тикета отклоняется и учитывается в счётчике ошибок.
        Дублировать её здесь означало бы держать правило в двух местах.
        """
        ticket = messages.Ticket(id="", text="текст")

        assert ticket.id == ""


class TestResult:
    """Проверки модели ответа агента."""

    def test_defaults(self) -> None:
        """Поля ответа по умолчанию пустые."""
        result = messages.Result(task_id="task-1")

        assert result.agent == ""
        assert result.success is False
        assert result.instance == ""
        assert result.confidence == 0.0

    def test_keeps_instance(self) -> None:
        """Экземпляр агента попадает в ответ."""
        result = messages.Result(task_id="task-1", instance="classifier-2")

        assert result.instance == "classifier-2"

    def test_allows_agent_specific_fields(self) -> None:
        """Ответ агента может нести поля своих шагов.

        Например, эскалация добавляет escalation_id, а классификатор -
        категорию. Строгий extra="forbid" здесь сломал бы добавление
        полей в уже работающие агенты.
        """
        result = messages.Result(task_id="task-1", escalation_id="ESC-1", sla_hours=24)

        assert result.escalation_id == "ESC-1"
        assert result.sla_hours == 24

    def test_rejects_wrong_types(self) -> None:
        """Поле неверного типа отклоняется."""
        with pytest.raises(ValidationError):
            messages.Result(task_id="task-1", sla_hours="сутки")


class TestConstants:
    """Проверки констант."""

    def test_categories(self) -> None:
        """Набор категорий совпадает с AGENTS.md."""
        assert messages.CATEGORY_BILLING == "billing"
        assert messages.CATEGORY_TECHNICAL == "technical"
        assert messages.CATEGORY_ACCOUNT == "account"
        assert messages.CATEGORY_DELIVERY == "delivery"
        assert messages.CATEGORY_OTHER == "other"

    def test_answer_types(self) -> None:
        """Типы ответов агента генерации."""
        assert messages.ANSWER_RESOLVED == "resolved"
        assert messages.ANSWER_DEFERRED == "deferred"

    def test_task_types(self) -> None:
        """Типы заданий."""
        assert messages.TASK_CLASSIFY == "classify"
        assert messages.TASK_KNOWLEDGE == "knowledge"
        assert messages.TASK_ANSWER == "answer"
        assert messages.TASK_ESCALATE == "escalate"
