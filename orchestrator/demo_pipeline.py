"""Демонстрация сквозного конвейера обработки обращения.

Запуск:
    python -m orchestrator.demo_pipeline
"""

from __future__ import annotations

import asyncio
import logging
import sys

from orchestrator.client import NatsConnection
from orchestrator.config import get_settings
from orchestrator.pipeline import run_pipeline_safe

DEMO_TICKETS = [
    "Не могу оплатить заказ, карта не проходит",
    "Приложение не работает, ошибка 500",
    "Не могу войти, забыл пароль",
    "Где моя посылка? Трек не обновляется",
    "Хочу обсудить сотрудничество с вашей компанией",
]


def setup_logging(level: str) -> None:
    """Логи оркестратора в консоль."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-5s %(name)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
    # Логи клиента NATS слишком подробны для демонстрации.
    logging.getLogger("nats").setLevel(logging.WARNING)


async def main() -> int:
    """Прогоняет обращения через конвейер и печатает отчёт."""
    settings = get_settings()
    setup_logging(settings.log_level)

    connection = NatsConnection(settings.nats_url)
    await connection.connect()

    print(f"Подключение к NATS: {settings.nats_url}")
    print()

    failures = 0
    for text in DEMO_TICKETS:
        print("=" * 92)
        print(f"Обращение: {text}")
        print("=" * 92)

        report = await run_pipeline_safe(connection, text, settings.task_timeout)

        if not report.get("success"):
            failures += 1
            print(f"  ОШИБКА: {report.get('error')}")
            print()
            continue

        print(f"  тикет          {report['ticket_id']}")
        print(f"  категория      {report['category']} (приоритет {report['priority']})")
        print(f"  теги           {', '.join(report['tags'])}")

        if report["article_found"]:
            print(
                f"  статья         №{report['article_title']} "
                f"(уверенность {report['confidence']})"
            )
        else:
            print("  статья         не найдена")

        print(f"  ответ          {report['answer']}")

        if report["escalated"]:
            print(
                f"  эскалация      {report['escalation_id']} -> "
                f"{report['assigned_to']}, SLA {report['sla_hours']} ч., "
                f"срочно: {report['urgent']}"
            )
        else:
            print("  эскалация      не требуется")

        print("  шаги:")
        for step in report["steps"]:
            if step.get("skipped"):
                print(f"    - {step['step']}: пропущен")
            else:
                print(f"    - {step['step']}: агент {step.get('agent')}")
        print()

    await connection.close()

    print(f"Обращений: {len(DEMO_TICKETS)}, сбоев: {failures}")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
