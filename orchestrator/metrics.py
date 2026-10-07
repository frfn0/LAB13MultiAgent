"""Сбор метрик агентов.

Агенты периодически публикуют свои счётчики в тему agent.metrics.
Оркестратор подписывается на неё, хранит последний снимок по каждому
экземпляру агента и умеет отдавать сводку.

Снимок ведётся по паре «агент + экземпляр»: в задании 7 запускается
несколько экземпляров одного агента, их счётчики нельзя складывать по
имени - каждый экземпляр обрабатывает свою долю заданий. При этом
экземпляры разных агентов не должны затирать друг друга, даже если
получили одинаковый instance.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

import nats
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription

from orchestrator.messages import SUBJECT_METRICS

logger = logging.getLogger(__name__)


@dataclass
class AgentStats:
    """Последний снимок счётчиков одного экземпляра агента."""

    agent: str
    queue: str
    instance: str
    received: int = 0
    processed: int = 0
    failed: int = 0
    uptime_seconds: int = 0

    @property
    def key(self) -> str:
        """Ключ снимка в словаре.

        Включает имя агента: экземпляры разных агентов на одном хосте
        по умолчанию получают одинаковый instance ("<host>:0"), и если
        ключом был бы только instance, метрики агентов затирали бы
        друг друга.
        """
        return f"{self.agent}:{self.instance}" if self.instance else self.agent

    def as_dict(self) -> dict[str, Any]:
        """Снимок для ответа API."""
        return {
            "agent": self.agent,
            "queue": self.queue,
            "instance": self.instance,
            "received": self.received,
            "processed": self.processed,
            "failed": self.failed,
            "uptime_seconds": self.uptime_seconds,
        }


@dataclass
class MetricsCollector:
    """Хранит снимки метрик агентов и ведёт счётчики оркестратора."""

    stats: dict[str, AgentStats] = field(default_factory=dict)

    # Счётчики самого оркестратора.
    tasks_sent: int = 0
    tasks_failed: int = 0
    timeouts: int = 0

    async def on_metrics(self, msg: Msg) -> None:
        """Обрабатывает публикацию метрик агента."""
        try:
            raw = json.loads(msg.data)
        except json.JSONDecodeError as exc:
            logger.error("метрики агента не являются корректным JSON: %s", exc)
            return

        snapshot = AgentStats(
            agent=str(raw.get("agent", "")),
            queue=str(raw.get("queue", "")),
            instance=str(raw.get("instance", "")),
            received=int(raw.get("received", 0)),
            processed=int(raw.get("processed", 0)),
            failed=int(raw.get("failed", 0)),
            uptime_seconds=int(raw.get("uptime_seconds", 0)),
        )

        if not snapshot.agent:
            logger.error("в метриках агента нет имени: %s", raw)
            return

        previous = self.stats.get(snapshot.key)
        if previous is not None:
            # Счётчики не должны уменьшаться: агент мог перезапуститься
            # с нуля, и тогда первый снимок после перезапуска показывал бы
            # нули вместо накопленного.
            snapshot.received = max(snapshot.received, previous.received)
            snapshot.processed = max(snapshot.processed, previous.processed)
            snapshot.failed = max(snapshot.failed, previous.failed)

        self.stats[snapshot.key] = snapshot
        logger.debug(
            "метрики агента %s: получено %d, обработано %d, ошибок %d",
            snapshot.agent,
            snapshot.received,
            snapshot.processed,
            snapshot.failed,
        )

    async def subscribe(self, client: nats.NATS) -> Subscription:
        """Подписывает коллектор на тему метрик агентов."""
        subscription = await client.subscribe(SUBJECT_METRICS, cb=self.on_metrics)
        await client.flush()
        logger.info("оркестратор подписан на метрики агентов (%s)", SUBJECT_METRICS)
        return subscription

    def agents(self) -> list[dict[str, Any]]:
        """Снимки всех известных экземпляров агентов."""
        return [snapshot.as_dict() for snapshot in self.stats.values()]

    def by_agent(self) -> dict[str, dict[str, int]]:
        """Суммарные счётчики по именам агентов."""
        totals: dict[str, dict[str, int]] = {}

        for snapshot in self.stats.values():
            bucket = totals.setdefault(
                snapshot.agent,
                {"instances": 0, "received": 0, "processed": 0, "failed": 0},
            )
            bucket["instances"] += 1
            bucket["received"] += snapshot.received
            bucket["processed"] += snapshot.processed
            bucket["failed"] += snapshot.failed

        return totals

    def totals(self) -> dict[str, Any]:
        """Полная сводка: оркестратор и агенты."""
        return {
            "orchestrator": {
                "tasks_sent": self.tasks_sent,
                "tasks_failed": self.tasks_failed,
                "timeouts": self.timeouts,
            },
            "agents": self.by_agent(),
        }

    def count_sent(self) -> None:
        """Задание отправлено агенту."""
        self.tasks_sent += 1

    def count_failed(self) -> None:
        """Задание завершилось ошибкой."""
        self.tasks_failed += 1

    def count_timeout(self) -> None:
        """Задание не уложилось в таймаут."""
        self.timeouts += 1

    def summary_line(self) -> str:
        """Одна строка со сводкой - для вывода в лог оркестратора."""
        totals = self.by_agent()
        if not totals:
            return "метрики агентов ещё не получены"

        parts = []
        for name, values in sorted(totals.items()):
            piece = f"{name}: обработано {values['processed']}"
            if values["received"]:
                piece += f" из {values['received']}"
            if values["instances"] > 1:
                piece += f", экземпляров {values['instances']}"
            if values["failed"]:
                piece += f", ошибок {values['failed']}"
            parts.append(piece)

        return "; ".join(parts)
