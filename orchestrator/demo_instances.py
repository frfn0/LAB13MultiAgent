"""Демонстрация нескольких экземпляров одного агента.

Задание 7: запустить 2-3 экземпляра одного и того же агента и показать,
что задания распределяются между ними.

Экземпляры работают в одной группе очереди NATS: все подписаны на одну
тему с одинаковым именем группы, поэтому брокер сам раздаёт задания и ни
один агент не получает задание дважды.

Сценарий из трёх фаз:

  1. три экземпляра, 30 заданий - короткая пачка, разброс заметен;
  2. три экземпляра, 300 заданий - распределение выравнивается;
  3. один экземпляр остановлен, 200 заданий - их забирают два других.

Проверяется не «всем поровну», а по смыслу: каждый работающий экземпляр
получает свою долю заданий, никто не забирает всё, а остановленный
экземпляр не выполняет ничего. Короткая пачка (фаза 1) показывается, но
не проверяется: на 30 заданиях брокер раздаёт их заметно неравномерно, и
это нормально, а не сбой.

Запуск:

    docker compose up -d
    python -m orchestrator.demo_instances
"""

from __future__ import annotations

import asyncio
import logging
import sys
from math import ceil

from orchestrator.client import NatsConnection
from orchestrator.config import get_retry_policy, get_settings
from orchestrator.demo_pipeline import DEMO_TICKETS
from orchestrator.harness import AgentPool, build_agents
from orchestrator.messages import Result, Ticket
from orchestrator.metrics import MetricsCollector
from orchestrator.pipeline import classify, new_ticket

logger = logging.getLogger(__name__)

AGENTS = ("classifier", "knowledge", "responder", "escalation")
INSTANCES = ("classifier-1", "classifier-2", "classifier-3")
STOPPED = INSTANCES[2]

SMALL_BATCH = 30
LARGE_BATCH = 300
AFTER_STOP_BATCH = 200
# Задания отправляются частями: одна большая пачка перегружает клиента
# оркестратора, да и распределение выходит менее ровным.
CHUNK = 50

# Допустимое отклонение доли экземпляра от идеальной. Брокер раздаёт
# задания по кругу, но с элементом случайности: на 300 заданиях разброс
# получается около 10% от идеальной доли.
MIN_SHARE = 0.8
MAX_SHARE = 1.25


async def send_batch(
    connection: NatsConnection,
    metrics: MetricsCollector,
    count: int,
) -> list[Result]:
    """Отправляет порцию заданий классификации.

    Задания уходят одновременно, а не по одному: при последовательной
    отправке один и тот же экземпляр успевает ответить раньше, чем задание
    уйдёт дальше, и балансировка была бы не видна.

    Args:
        connection: подключение к NATS.
        metrics: счётчики попыток.
        count: сколько заданий отправить.

    Returns:
        Ответы агентов в порядке отправки.
    """
    policy = get_retry_policy()
    results: list[Result] = []

    for start in range(0, count, CHUNK):
        tickets: list[Ticket] = [
            new_ticket(DEMO_TICKETS[i % len(DEMO_TICKETS)])
            for i in range(start, min(start + CHUNK, count))
        ]
        results.extend(
            await asyncio.gather(
                *(
                    classify(connection, ticket, 5.0, metrics, policy)
                    for ticket in tickets
                )
            )
        )

    return results


def processed_by_instance(metrics: MetricsCollector) -> dict[str, int]:
    """Сколько заданий обработал каждый экземпляр классификатора."""
    return {
        snapshot["instance"]: snapshot["processed"]
        for snapshot in metrics.agents()
        if snapshot["agent"] == "classifier"
    }


def print_distribution(
    results: list[Result],
    metrics: MetricsCollector,
    active: tuple[str, ...],
    before: dict[str, int],
    title: str,
    enforced: bool,
) -> list[str]:
    """Печатает распределение заданий и возвращает список замечаний.

    Args:
        results: ответы агентов фазы.
        metrics: счётчики агентов.
        active: экземпляры, которые сейчас работают.
        before: счётчики экземпляров до начала фазы.
        title: заголовок таблицы.
        enforced: проверять ли распределение.

    Returns:
        Список замечаний. Пустой список означает, что всё сошлось.
    """
    total = len(results)
    current = processed_by_instance(metrics)
    in_phase = {name: current.get(name, 0) - before.get(name, 0) for name in current}

    print(f"  {title}")
    print("  " + "-" * 78)
    print(f"    {'экземпляр':18} {'в фазе':8} {'доля':7} {'по метрикам':13}")
    print("  " + "-" * 78)

    for name in sorted(current):
        count = in_phase.get(name, 0)
        share = f"{count / total * 100:.0f}%" if total else "-"
        state = "" if name in active else "  (остановлен)"
        print(f"    {name:18} {count:<8} {share:7} {current[name]:<13}{state}")

    # Разброс считаем только по работающим экземплярам: у остановленного
    # ноль заданий, и с ним в разнице получился бы разброс в размер всей
    # нагрузки.
    active_counts = [in_phase.get(name, 0) for name in active]
    spread = max(active_counts, default=0) - min(active_counts, default=0)
    ideal = total / len(active)
    print(f"    {'всего в фазе':18} {total:<8}")
    print(
        f"    разброс между работающими экземплярами {spread} "
        f"({spread / ideal * 100:.0f}% от идеальной доли {ideal:.0f})"
    )
    print()

    problems: list[str] = []
    if not enforced:
        print("  фаза показана для сравнения, распределение не проверяется")
        print()
        return problems

    if total == 0:
        return ["ни одного задания не выполнено"]

    floor = ceil(ideal * MIN_SHARE)
    ceiling = ideal * MAX_SHARE

    for name in active:
        count = in_phase.get(name, 0)
        if count < floor:
            problems.append(
                f"{name} выполнил {count} заданий, ожидалось не меньше {floor}"
            )
        if count > ceiling:
            problems.append(
                f"{name} выполнил {count} заданий, больше допустимых {ceiling:.0f}"
            )

    for name in sorted(in_phase):
        if name not in active and in_phase[name] > 0:
            problems.append(
                f"остановленный экземпляр {name} выполнил {in_phase[name]} заданий"
            )

    for problem in problems:
        print(f"  ОШИБКА: {problem}")

    return problems


async def main() -> int:
    """Запускает три экземпляра классификатора и проверяет распределение."""
    settings = get_settings()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
    logging.getLogger("nats").setLevel(logging.WARNING)

    print("Сборка агентов")
    build_agents(AGENTS)

    connection = NatsConnection(settings.nats_url)
    try:
        await connection.connect()
    except Exception as exc:  # noqa: BLE001 - нужен понятный совет
        print(f"Не удалось подключиться к NATS: {exc}")
        print("Сначала запустите брокер: docker compose up -d")
        return 1

    print()
    print("Запуск остальных агентов и трёх экземпляров классификатора")
    pool = AgentPool()
    for name in ("knowledge", "responder", "escalation"):
        pool.start(name, f"{name}-task7")
    for instance in INSTANCES:
        pool.start("classifier", instance)
    await asyncio.sleep(1.5)

    metrics = MetricsCollector()
    await metrics.subscribe(connection.client)

    failures = 0
    try:
        before = processed_by_instance(metrics)

        print()
        print("=" * 78)
        print(f"Фаза 1. Три экземпляра, {SMALL_BATCH} заданий: короткая пачка")
        print("=" * 78)
        results = await send_batch(connection, metrics, SMALL_BATCH)
        await asyncio.sleep(2.5)
        failures += len(
            print_distribution(
                results,
                metrics,
                INSTANCES,
                before,
                "Кто какие задания выполнил",
                enforced=False,
            )
        )

        before = processed_by_instance(metrics)
        print()
        print("=" * 78)
        print(
            f"Фаза 2. Три экземпляра, {LARGE_BATCH} заданий: "
            "распределение должно выравниваться"
        )
        print("=" * 78)
        results = await send_batch(connection, metrics, LARGE_BATCH)
        await asyncio.sleep(2.5)
        failures += len(
            print_distribution(
                results,
                metrics,
                INSTANCES,
                before,
                "Кто какие задания выполнил",
                enforced=True,
            )
        )

        before = processed_by_instance(metrics)
        print()
        print("=" * 78)
        print(f"Фаза 3. Экземпляр {STOPPED} остановлен, {AFTER_STOP_BATCH} заданий")
        print("=" * 78)
        pool.stop(f"classifier-{STOPPED}")
        await asyncio.sleep(0.5)

        results = await send_batch(connection, metrics, AFTER_STOP_BATCH)
        await asyncio.sleep(2.5)
        active = INSTANCES[:2]
        failures += len(
            print_distribution(
                results,
                metrics,
                active,
                before,
                "Кто какие задания выполнил после остановки экземпляра",
                enforced=True,
            )
        )

        current = processed_by_instance(metrics)
        print(
            f"  экземпляр {STOPPED} остановился на "
            f"{current.get(STOPPED, 0)} заданиях и больше не растёт"
        )
        print()
    finally:
        pool.stop_all()
        await connection.close()

    print("=" * 78)
    print(f"Проверок не пройдено: {failures}")
    print("=" * 78)
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
