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
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

from orchestrator.client import NatsConnection
from orchestrator.config import get_retry_policy, get_settings
from orchestrator.demo_pipeline import DEMO_TICKETS
from orchestrator.metrics import MetricsCollector
from orchestrator.pipeline import run_pipeline_safe

logger = logging.getLogger(__name__)

BIN_DIR = Path(os.environ.get("AGENT_BIN_DIR", ".task6-bin"))
LOG_DIR = Path("logs")

HEALTHY_AGENTS = ("classifier", "knowledge", "escalation")
TICKET = DEMO_TICKETS[0]


@dataclass
class Phase:
    """Одна фаза проверки."""

    title: str
    responder_env: dict[str, str] = field(default_factory=dict)
    timeout: float = 5.0
    expect_retries: int = 0
    expect_success: bool = True


def build_agents() -> None:
    """Собирает агентов в отдельный каталог."""
    BIN_DIR.mkdir(parents=True, exist_ok=True)
    for name in (*HEALTHY_AGENTS, "responder"):
        suffix = ".exe" if os.name == "nt" else ""
        target = BIN_DIR / f"{name}{suffix}"
        print(f"  сборка {name} -> {target}")
        subprocess.run(
            ["go", "build", "-o", str(target), f"./agents/{name}"],
            check=True,
        )


def agent_env(name: str, extra: dict[str, str]) -> dict[str, str]:
    """Окружение для запуска агента."""
    env = dict(os.environ)
    env["INSTANCE_ID"] = f"{name}-task6"
    env["LOG_FILE"] = str(LOG_DIR / f"{name}-task6.log")
    env["LOG_LEVEL"] = "INFO"
    env["METRICS_INTERVAL"] = "2s"
    for key in ("FAULT_FAIL_ATTEMPTS", "FAULT_DELAY_MS", "FAULT_DROP"):
        env.pop(key, None)
    env.update(extra)
    return env


def start_agent(name: str, extra: dict[str, str]) -> subprocess.Popen[bytes]:
    """Запускает агента в фоне."""
    suffix = ".exe" if os.name == "nt" else ""
    process = subprocess.Popen(
        [str(BIN_DIR / f"{name}{suffix}")],
        env=agent_env(name, extra),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    print(f"  запущен {name} pid={process.pid} {extra if extra else '(без сбоев)'}")
    return process


def stop_agent(process: subprocess.Popen[bytes]) -> None:
    """Останавливает агента и дожидается завершения."""
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


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
    responder: subprocess.Popen[bytes] | None,
) -> tuple[dict, dict[str, int], subprocess.Popen[bytes]]:
    """Проводит одну фазу и печатает результат.

    Returns:
        Отчёт по обращению, счётчики оркестратора и процесс агента-ответчика,
        остановленный на следующей фазе.
    """
    print()
    print("=" * 78)
    print(phase.title)
    print("=" * 78)

    if responder is not None:
        stop_agent(responder)
    started = start_agent("responder", phase.responder_env)
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

    return report, totals, started


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
    build_agents()

    connection = NatsConnection(settings.nats_url)
    try:
        await connection.connect()
    except Exception as exc:  # noqa: BLE001 - нужен понятный совет
        print(f"Не удалось подключиться к NATS: {exc}")
        print("Сначала запустите брокер: docker compose up -d")
        return 1

    print()
    print("Запуск постоянных агентов")
    # Агент-ответчик здесь не запускается: в каждой фазе он стартует заново
    # со своими настройками сбоя. Если держать ещё один здоровый экземпляр,
    # он встанет в ту же группу очереди и заберёт задания себе, а сбой в
    # нужном экземпляре так и не проявится.
    processes = [start_agent(name, {}) for name in HEALTHY_AGENTS]
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
    responder: subprocess.Popen[bytes] | None = None
    try:
        for phase in phases:
            report, totals, responder = await run_phase(connection, phase, responder)

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
        for process in processes:
            stop_agent(process)
        if responder is not None:
            stop_agent(responder)
        await connection.close()

    print()
    print("=" * 78)
    print(f"Проверок не пройдено: {failures}")
    print("=" * 78)
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
