"""Демонстрация логирования и мониторинга.

Показывает, как собираются метрики агентов и что попадает в файлы логов.

Запуск:
    METRICS_INTERVAL=2s python -m orchestrator.demo_metrics
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

from orchestrator.client import NatsConnection
from orchestrator.config import get_settings
from orchestrator.demo_pipeline import DEMO_TICKETS
from orchestrator.metrics import MetricsCollector
from orchestrator.pipeline import run_pipeline_safe

LOG_DIR = Path("logs")


def metrics_interval() -> float:
    """Интервал публикации метрик агентами в секундах."""
    raw = os.environ.get("METRICS_INTERVAL", "2s").strip().lower()
    if raw.endswith("s"):
        raw = raw[:-1]
    try:
        return float(raw)
    except ValueError:
        return 2.0


def setup_logging(level: str, log_file: str) -> None:
    """Логи оркестратора одновременно в консоль и в файл."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        target = Path(log_file)
        target.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(target, encoding="utf-8"))

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-5s %(name)s %(message)s",
        handlers=handlers,
        force=True,
    )
    logging.getLogger("nats").setLevel(logging.WARNING)

    logging.info("логирование настроено: консоль и %s", log_file or "(без файла)")


def print_agent_tables(collector: MetricsCollector) -> None:
    """Печатает сводку по агентам и по экземплярам."""
    print("Сводка по агентам")
    print("-" * 78)
    print(
        f"{'агент':14} {'экземпляров':11} {'получено':10} {'обработано':11} {'ошибок':8}"
    )
    print("-" * 78)
    for name, values in sorted(collector.by_agent().items()):
        print(
            f"{name:14} {values['instances']:<11} {values['received']:<10} "
            f"{values['processed']:<11} {values['failed']:<8}"
        )
    print()

    print("Снимки по экземплярам")
    print("-" * 78)
    for snapshot in collector.agents():
        print(
            f"  {snapshot['agent']:12} instance={snapshot['instance']:<22} "
            f"queue={snapshot['queue']:<16} обработано={snapshot['processed']}"
        )
    print()


async def main() -> int:
    """Прогоняет обращения и печатает сводку метрик."""
    settings = get_settings()
    setup_logging(settings.log_level, settings.log_file)

    interval = metrics_interval()
    collector = MetricsCollector()
    connection = NatsConnection(settings.nats_url)
    await connection.connect()
    await collector.subscribe(connection.client)

    print(f"Подключение к NATS: {settings.nats_url}")
    print(f"Интервал публикации метрик агентами: {interval:g} с")
    print(f"Обрабатываем обращений: {len(DEMO_TICKETS)}")
    print()

    # Даём агентам прислать первый снимок.
    await asyncio.sleep(interval + 0.5)
    print("Метрики до обработки обращений:")
    print("-" * 78)
    print(f"  {collector.summary_line()}")
    print()

    failures = 0
    for text in DEMO_TICKETS:
        report = await run_pipeline_safe(
            connection, text, settings.task_timeout, collector
        )
        if not report.get("success"):
            failures += 1
            logging.error("обращение не обработано: %s - %s", text, report.get("error"))

    totals = collector.totals()["orchestrator"]
    print("Счётчики оркестратора")
    print("-" * 78)
    print(f"  заданий отправлено   {totals['tasks_sent']}")
    print(f"  заданий с ошибкой    {totals['tasks_failed']}")
    print(f"  таймаутов            {totals['timeouts']}")
    print()

    logging.info("итоговая сводка: %s", collector.summary_line())

    # Ждём очередной публикации метрик агентов, иначе снимки останутся
    # такими же, какими были до обработки: агенты шлют их по таймеру,
    # а не в ответ на каждое задание.
    print(f"Ждём следующий снимок метрик ({interval + 0.5:g} с)")
    print()
    await asyncio.sleep(interval + 0.5)

    print("Метрики агентов после обработки обращений")
    print("=" * 78)
    print()
    print_agent_tables(collector)

    logging.info("итоговая сводка: %s", collector.summary_line())

    await connection.close()

    log_file = LOG_DIR / "orchestrator.log"
    if log_file.exists():
        size = log_file.stat().st_size
        print(f"Файл лога оркестратора: {log_file} ({size} байт)")
        print("Последние строки:")
        for line in log_file.read_text(encoding="utf-8").splitlines()[-5:]:
            print(f"  {line}")
        print()

    print(f"Обращений: {len(DEMO_TICKETS)}, сбоев: {failures}")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
