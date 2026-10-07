"""Демонстрация повторной отправки задания и таймаутов.

Сценарий состоит из трёх фаз. Агенты запускаются и останавливаются
из этого скрипта, а сбои включаются переменными окружения FAULT_*:

  1. все агенты здоровы - повторов быть не должно;
  2. агент-ответчик отвечает ошибкой на первые две попытки
     (FAULT_FAIL_ATTEMPTS=2) - повторы должны спасти обращение;
  3. агент-ответчик принимает задание, но не отвечает (FAULT_DROP=1) -
     все попытки должны закончиться таймаутом, а обращение - провалом.

Запуск:

    docker compose up -d
    python -m orchestrator.demo_retry
"""

from __future__ import annotations

import asyncio
import logging
import sys
from dataclasses import dataclass, field

from orchestrator.client import NatsConnection
from orchestrator.config import get_retry_policy, get_settings
from orchestrator.demo_pipeline import DEMO_TICKETS
from orchestrator.harness import LOG_DIR, AgentPool, build_agents
from orchestrator.metrics import MetricsCollector
from orchestrator.pipeline import run_pipeline_safe

logger = logging.getLogger(__name__)

AGENTS = ("classifier", "knowledge", "responder", "escalation")
HEALTHY_AGENTS = ("classifier", "knowledge", "escalation")
RESPONDER_INSTANCE = "responder-task6"
RESPONDER_KEY = f"responder-{RESPONDER_INSTANCE}"
TICKET = DEMO_TICKETS[0]


@dataclass
class Phase:
    """Одна фаза проверки."""

    title: str
    responder_env: dict[str, str] = field(default_factory=dict)
    timeout: float = 5.0
    expect_retries: int = 0
    expect_success: bool = True


def print_totals(metrics: MetricsCollector) -> dict[str, int]:
    """Печатает счётчики оркестратора и возвращает их."""
    totals = metrics.totals()["orchestrator"]
    print("  счётчики оркестратора")
    print("  " + "-" * 72)
    print(f"    попыток отправки   {totals['tasks_sent']}")
    print(f"    повторов           {totals['retries']}")
    print(f"    таймаутов          {totals['timeouts']}")
    print(f"    проваленных шагов  {totals['tasks_failed']}")
    print()
    return totals


async def run_phase(
    connection: NatsConnection,
    phase: Phase,
    pool: AgentPool,
) -> tuple[dict, dict[str, int]]:
    """Проводит одну фазу и печатает результат.

    Args:
        connection: подключение к NATS.
        phase: описание фазы.
        pool: процессы агентов. Ответчик перезапускается на каждой фазе,
            иначе здоровый экземпляр заберёт задания себе, а сбой в нужном
            экземпляре не проявится.

    Returns:
        Отчёт по обращению и счётчики оркестратора.
    """
    print()
    print("=" * 78)
    print(phase.title)
    print("=" * 78)

    pool.stop(RESPONDER_KEY)
    pool.start("responder", RESPONDER_INSTANCE, phase.responder_env)
    await asyncio.sleep(1.0)

    metrics = MetricsCollector()
    policy = get_retry_policy()
    print(
        f"  политика повторов: попыток {policy.max_attempts}, "
        f"пауза {policy.initial_delay} с, рост x{policy.backoff}"
    )
    print(f"  обращение: {TICKET}")
    print()

    report = await run_pipeline_safe(connection, TICKET, phase.timeout, metrics, policy)
    totals = print_totals(metrics)

    print("  итог фазы")
    print("  " + "-" * 72)
    print(f"    успешно          {report.get('success')}")
    if report.get("error"):
        print(f"    ошибка           {report['error']}")
    else:
        print(f"    категория        {report.get('category')}")
        print(f"    ответ            {(report.get('answer') or '')[:60]}...")
    print()

    return report, totals


async def main() -> int:
    """Прогоняет три фазы и проверяет ожидания."""
    settings = get_settings()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
    logging.getLogger("nats").setLevel(logging.WARNING)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

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
    print("Запуск постоянных агентов")
    pool = AgentPool()
    for name in HEALTHY_AGENTS:
        pool.start(name, f"{name}-task6")
    await asyncio.sleep(1.5)

    phases = [
        Phase(
            title="Фаза 1. Все агенты здоровы: повторов быть не должно",
            responder_env={},
            expect_retries=0,
            expect_success=True,
        ),
        Phase(
            title=(
                "Фаза 2. Агент-ответчик отвечает ошибкой на первые две попытки: "
                "повторы должны спасти обращение"
            ),
            responder_env={"FAULT_FAIL_ATTEMPTS": "2"},
            expect_retries=2,
            expect_success=True,
        ),
        Phase(
            title=(
                "Фаза 3. Агент-ответчик не отвечает вовсе: "
                "все попытки закончатся таймаутом"
            ),
            responder_env={"FAULT_DROP": "1"},
            timeout=1.5,
            expect_retries=2,
            expect_success=False,
        ),
    ]

    failures = 0
    try:
        for phase in phases:
            report, totals = await run_phase(connection, phase, pool)

            if totals["retries"] != phase.expect_retries:
                failures += 1
                print(
                    f"  ОШИБКА: повторов {totals['retries']}, "
                    f"ожидалось {phase.expect_retries}"
                )
            if bool(report.get("success")) != phase.expect_success:
                failures += 1
                print(
                    f"  ОШИБКА: успех обращения {report.get('success')}, "
                    f"ожидалось {phase.expect_success}"
                )
    finally:
        pool.stop_all()
        await connection.close()

    print()
    print("=" * 78)
    print(f"Проверок не пройдено: {failures}")
    print("=" * 78)
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
