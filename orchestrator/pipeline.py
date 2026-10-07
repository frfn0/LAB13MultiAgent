"""Конвейер обработки обращения: классификация -> база знаний -> ответ.

Оркестратор не хранит логику агентов. Он только передаёт данные от одного
шага к следующему и решает, что делать с неполным результатом: если статья
не найдена, вместо готового ответа уходит эскалация.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from orchestrator.client import NatsConnection, TaskTimeoutError
from orchestrator.messages import (
    SUBJECT_ANSWER,
    SUBJECT_CLASSIFY,
    SUBJECT_ESCALATE,
    SUBJECT_KNOWLEDGE,
    TASK_ANSWER,
    TASK_CLASSIFY,
    TASK_ESCALATE,
    TASK_KNOWLEDGE,
    Result,
    Task,
    Ticket,
)

logger = logging.getLogger(__name__)


def new_ticket(text: str, author: str = "client") -> Ticket:
    """Создаёт обращение с уникальным идентификатором."""
    return Ticket(id=f"ticket-{uuid.uuid4().hex[:8]}", text=text, author=author)


def _new_task(kind: str, ticket: Ticket, **fields: Any) -> Task:
    """Создаёт задание для очередного шага конвейера."""
    return Task(
        task_id=f"task-{uuid.uuid4().hex[:8]}",
        type=kind,
        ticket=ticket,
        attempts=1,
        **fields,
    )


async def classify(
    connection: NatsConnection, ticket: Ticket, timeout: float
) -> Result:
    """Шаг 1: классификация обращения."""
    task = _new_task(TASK_CLASSIFY, ticket)
    return await connection.send_task(SUBJECT_CLASSIFY, task, timeout=timeout)


async def find_article(
    connection: NatsConnection,
    ticket: Ticket,
    category: str,
    priority: int,
    tags: list[str],
    timeout: float,
) -> Result:
    """Шаг 2: поиск статьи базы знаний."""
    task = _new_task(
        TASK_KNOWLEDGE, ticket, category=category, priority=priority, tags=tags
    )
    return await connection.send_task(SUBJECT_KNOWLEDGE, task, timeout=timeout)


async def make_answer(
    connection: NatsConnection,
    ticket: Ticket,
    category: str,
    priority: int,
    article: Result,
    timeout: float,
) -> Result:
    """Шаг 3: формирование ответа клиенту."""
    task = _new_task(
        TASK_ANSWER,
        ticket,
        category=category,
        priority=priority,
        found=article.found,
        article_id=article.article_id,
        article_title=article.article_title,
        solution=article.solution,
        confidence=article.confidence,
    )
    return await connection.send_task(SUBJECT_ANSWER, task, timeout=timeout)


async def escalate(
    connection: NatsConnection,
    ticket: Ticket,
    category: str,
    priority: int,
    reason: str,
    timeout: float,
) -> Result:
    """Шаг 4: эскалация обращения."""
    task = _new_task(
        TASK_ESCALATE,
        ticket,
        category=category,
        priority=priority,
        reason=reason,
    )
    return await connection.send_task(SUBJECT_ESCALATE, task, timeout=timeout)


async def run_pipeline(
    connection: NatsConnection,
    text: str,
    timeout: float = 5.0,
) -> dict[str, Any]:
    """Проводит обращение через весь конвейер.

    Returns:
        Словарь с результатом каждого шага и итоговым ответом клиенту.
    """
    ticket = new_ticket(text)
    report: dict[str, Any] = {
        "ticket_id": ticket.id,
        "text": text,
        "steps": [],
    }

    # Шаг 1: классификация
    classified = await classify(connection, ticket, timeout)
    report["category"] = classified.category
    report["priority"] = classified.priority
    report["tags"] = classified.tags
    report["steps"].append(
        {
            "step": "classify",
            "agent": classified.agent,
            "category": classified.category,
            "priority": classified.priority,
        }
    )
    logger.info(
        "классификация: %s приоритет %s", classified.category, classified.priority
    )

    # Шаг 2: поиск по базе знаний
    article = await find_article(
        connection,
        ticket,
        classified.category,
        classified.priority,
        classified.tags,
        timeout,
    )
    report["article_found"] = article.found
    report["article_title"] = article.article_title
    report["confidence"] = article.confidence
    report["steps"].append(
        {
            "step": "knowledge",
            "agent": article.agent,
            "found": article.found,
            "article_id": article.article_id,
            "article_title": article.article_title,
            "confidence": article.confidence,
        }
    )
    logger.info(
        "база знаний: найдено=%s уверенность=%s", article.found, article.confidence
    )

    # Шаг 3: ответ клиенту
    answer = await make_answer(
        connection,
        ticket,
        classified.category,
        classified.priority,
        article,
        timeout,
    )
    report["answer"] = answer.answer
    report["answer_type"] = answer.answer_type
    report["steps"].append(
        {
            "step": "answer",
            "agent": answer.agent,
            "answer_type": answer.answer_type,
        }
    )
    logger.info("ответ: тип %s", answer.answer_type)

    # Эскалация нужна там, где автоматически закрыть обращение не вышло:
    # статья не найдена или категория не распознана.
    if not article.found or classified.category == "other":
        reason = (
            "категория обращения не распознана"
            if classified.category == "other"
            else "не найдено решение в базе знаний"
        )
        escalation = await escalate(
            connection,
            ticket,
            classified.category,
            classified.priority,
            reason,
            timeout,
        )
        report["escalated"] = True
        report["escalation_id"] = escalation.escalation_id
        report["assigned_to"] = escalation.assigned_to
        report["sla_hours"] = escalation.sla_hours
        report["urgent"] = escalation.urgent

        # Клиенту показываем ответ об эскалации, а не автоматический ответ
        # агента: обращение ушло человеку, и говорить «решено» было бы
        # неправдой.
        if escalation.answer:
            report["answer"] = escalation.answer
            report["answer_type"] = escalation.answer_type
        report["steps"].append(
            {
                "step": "escalate",
                "agent": escalation.agent,
                "escalation_id": escalation.escalation_id,
                "assigned_to": escalation.assigned_to,
                "sla_hours": escalation.sla_hours,
                "urgent": escalation.urgent,
            }
        )
        logger.info(
            "эскалация: %s срок %d ч.", escalation.assigned_to, escalation.sla_hours
        )
    else:
        report["escalated"] = False
        report["steps"].append({"step": "escalate", "skipped": True})

    report["success"] = True
    return report


async def run_pipeline_safe(
    connection: NatsConnection,
    text: str,
    timeout: float = 5.0,
) -> dict[str, Any]:
    """Обёртка конвейера: сбои не поднимаются наружу, а попадают в отчёт."""
    try:
        return await run_pipeline(connection, text, timeout)
    except TaskTimeoutError as exc:
        logger.error("шаг конвейера не уложился в таймаут: %s", exc)
        return {"ticket_id": None, "text": text, "success": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001 - отчёт должен быть всегда
        logger.exception("конвейер завершился с ошибкой")
        return {"ticket_id": None, "text": text, "success": False, "error": str(exc)}
