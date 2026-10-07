"""Точка входа оркестратора.

Пока реализовано задание 3 методички: оркестратор умеет отправлять задание
агенту, ждать результат и обрабатывать таймаут. Конвейер из нескольких
агентов собирается в следующих заданиях, когда появятся остальные агенты.

Запуск:
    python -m orchestrator.main
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
import uuid

from orchestrator.client import (
    NatsConnection,
    TaskFailedError,
    TaskTimeoutError,
)
from orchestrator.config import get_settings
from orchestrator.messages import (
    SUBJECT_CLASSIFY,
    TASK_CLASSIFY,
    Task,
    Ticket,
)

DEMO_TICKETS = [
    "Не могу оплатить заказ, карта не проходит",
    "Приложение не работает, ошибка 500",
    "Не могу войти, забыл пароль",
    "Где моя посылка? Трек не обновляется",
    "Здравствуйте, хочу спросить про вас",
]


def setup_logging(level: str, log_file: str = "") -> None:
    """Настраивает вывод логов в консоль и, при желании, в файл."""
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-5s %(name)s %(message)s",
        handlers=handlers,
        force=True,
    )


def new_task(text: str) -> Task:
    """Создаёт задание классификации для нового обращения."""
    ticket_id = f"ticket-{uuid.uuid4().hex[:8]}"
    return Task(
        task_id=f"task-{uuid.uuid4().hex[:8]}",
        type=TASK_CLASSIFY,
        ticket=Ticket(id=ticket_id, text=text, author="client"),
        attempts=1,
    )


async def demo_pipeline() -> int:
    """Отправляет несколько обращений классификатору и печатает ответы."""
    settings = get_settings()
    setup_logging(settings.log_level, settings.log_file)

    connection = NatsConnection(settings.nats_url)
    await connection.connect()

    print(f"Подключение к NATS: {settings.nats_url}")
    print(f"Таймаут ответа агента: {settings.task_timeout} сек")
    print()

    print("Отправка обращений классификатору")
    print("-" * 78)
    print(f"{'обращение':44} {'категория':11} {'приоритет':9}")
    print("-" * 78)

    failures = 0
    for text in DEMO_TICKETS:
        task = new_task(text)
        started = time.perf_counter()
        try:
            result = await connection.send_task(
                SUBJECT_CLASSIFY, task, timeout=settings.task_timeout
            )
            elapsed = time.perf_counter() - started
            print(
                f"{text[:42]:44} {result.category:11} {result.priority:<9} "
                f"{elapsed * 1000:.1f} мс"
            )
        except TaskTimeoutError as exc:
            failures += 1
            print(f"{text[:42]:44} {'ТАЙМАУТ':11} {'-':9} {exc}")
        except TaskFailedError as exc:
            failures += 1
            print(f"{text[:42]:44} {'ОШИБКА':11} {'-':9} {exc}")

    print()

    print("Проверка таймаута: задание в тему без агента")
    print("-" * 78)
    task = new_task("Обращение, на которое никто не ответит")
    task.task_id = "task-no-agent"
    started = time.perf_counter()
    try:
        await connection.send_task("ticket.no_such_agent", task, timeout=1.0)
        print("  неожиданно получен ответ")
        failures += 1
    except TaskTimeoutError as exc:
        elapsed = time.perf_counter() - started
        print(f"  отловлен таймаут за {elapsed:.2f} сек: {exc}")
    print()

    print("Все ожидающие задания сняты:", await connection.wait_for_idle(2.0))
    await connection.close()

    print()
    print(f"Обращений: {len(DEMO_TICKETS)}, сбоев: {failures + 1} (таймаут ожидаем)")
    return 0 if failures == 0 else 1


def main() -> int:
    """Точка входа."""
    try:
        return asyncio.run(demo_pipeline())
    except KeyboardInterrupt:
        print("\nОстановлено пользователем")
        return 0


if __name__ == "__main__":
    sys.exit(main())
