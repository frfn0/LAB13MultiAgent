"""Тесты сбора метрик агентов (задание 5)."""

from __future__ import annotations

import json

from nats.aio.msg import Msg

from orchestrator.metrics import MetricsCollector

from tests.conftest import metrics_message


class TestOnMetrics:
    """Проверки разбора снимков метрик."""

    async def test_stores_snapshot(self, collector: MetricsCollector) -> None:
        """Снимок сохраняется по экземпляру."""
        await collector.on_metrics(
            metrics_message("classifier", "classifier-1", 3, 3, 0)
        )

        snapshots = collector.agents()
        assert len(snapshots) == 1
        assert snapshots[0]["agent"] == "classifier"
        assert snapshots[0]["processed"] == 3

    async def test_keeps_instances_apart(self, collector: MetricsCollector) -> None:
        """Экземпляры одного агента не затирают друг друга."""
        await collector.on_metrics(
            metrics_message("classifier", "classifier-1", 10, 10, 0)
        )
        await collector.on_metrics(
            metrics_message("classifier", "classifier-2", 4, 4, 1)
        )

        by_agent = collector.by_agent()
        assert by_agent["classifier"]["instances"] == 2
        assert by_agent["classifier"]["processed"] == 14

    async def test_same_instance_id_different_agents(
        self, collector: MetricsCollector
    ) -> None:
        """Одинаковый идентификатор у разных агентов не приводит к затиранию.

        На практике все агенты на одном хосте получают instance вида
        "<host>:0", поэтому ключ только по экземпляру приводил бы к потере
        метрик целых агентов.
        """
        await collector.on_metrics(metrics_message("classifier", "host:0", 5, 5, 0))
        await collector.on_metrics(metrics_message("responder", "host:0", 7, 7, 0))

        by_agent = collector.by_agent()
        assert by_agent["classifier"]["processed"] == 5
        assert by_agent["responder"]["processed"] == 7

    async def test_counters_do_not_decrease(self, collector: MetricsCollector) -> None:
        """Перезапуск агента с нуля не обнуляет накопленную статистику."""
        await collector.on_metrics(
            metrics_message("knowledge", "knowledge-1", 10, 9, 1)
        )
        await collector.on_metrics(metrics_message("knowledge", "knowledge-1", 0, 0, 0))

        snapshot = collector.agents()[0]
        assert snapshot["processed"] == 9
        assert snapshot["failed"] == 1

    async def test_counters_grow(self, collector: MetricsCollector) -> None:
        """Обычный рост счётчиков сохраняется."""
        await collector.on_metrics(metrics_message("knowledge", "knowledge-1", 1, 1, 0))
        await collector.on_metrics(metrics_message("knowledge", "knowledge-1", 4, 3, 1))

        snapshot = collector.agents()[0]
        assert snapshot["received"] == 4
        assert snapshot["processed"] == 3

    async def test_ignores_broken_json(self, collector: MetricsCollector) -> None:
        """Битое сообщение не ломает сбор метрик."""
        await collector.on_metrics(
            Msg(
                _client=None,
                subject="agent.metrics",
                reply="",
                data="{не json".encode(),
            ),
        )

        assert collector.agents() == []

    async def test_ignores_message_without_agent(
        self, collector: MetricsCollector
    ) -> None:
        """Снимок без имени агента отбрасывается."""
        payload = json.dumps({"processed": 3}).encode()

        await collector.on_metrics(
            Msg(_client=None, subject="agent.metrics", reply="", data=payload)
        )

        assert collector.agents() == []

    async def test_missing_fields_use_zero(self, collector: MetricsCollector) -> None:
        """Отсутствующие счётчики считаются нулевыми, а не None."""
        payload = json.dumps({"agent": "classifier", "instance": "c-1"}).encode()

        await collector.on_metrics(
            Msg(_client=None, subject="agent.metrics", reply="", data=payload)
        )

        snapshot = collector.agents()[0]
        assert snapshot["processed"] == 0
        assert snapshot["queue"] == ""


class TestAggregates:
    """Проверки сводных значений."""

    async def test_by_agent_groups_instances(self, collector: MetricsCollector) -> None:
        """Сводка по именам агентов складывает экземпляры."""
        await collector.on_metrics(metrics_message("classifier", "c-1", 4, 4, 0))
        await collector.on_metrics(metrics_message("classifier", "c-2", 6, 6, 0))

        bucket = collector.by_agent()["classifier"]
        assert bucket["instances"] == 2
        assert bucket["received"] == 10
        assert bucket["processed"] == 10

    async def test_totals_include_orchestrator(
        self, collector: MetricsCollector
    ) -> None:
        """В сводке есть и агенты, и сам оркестратор."""
        await collector.on_metrics(metrics_message("classifier", "c-1", 2, 2, 0))
        collector.count_sent()
        collector.count_retry()
        collector.count_timeout()
        collector.count_failed()

        totals = collector.totals()
        assert totals["agents"]["classifier"]["processed"] == 2
        assert totals["orchestrator"] == {
            "tasks_sent": 1,
            "tasks_failed": 1,
            "timeouts": 1,
            "retries": 1,
        }

    async def test_summary_line_without_metrics(
        self, collector: MetricsCollector
    ) -> None:
        """До первого снимка сводка честно говорит, что данных нет."""
        assert "ещё не получены" in collector.summary_line()

    async def test_summary_line_lists_agents(self, collector: MetricsCollector) -> None:
        """Сводка перечисляет агентов с числом обработанных заданий."""
        await collector.on_metrics(metrics_message("classifier", "c-1", 4, 4, 0))
        await collector.on_metrics(metrics_message("knowledge", "k-1", 3, 3, 1))

        summary = collector.summary_line()
        assert "classifier: обработано 4 из 4" in summary
        assert "knowledge: обработано 3 из 3" in summary
        assert "ошибок 1" in summary

    async def test_summary_line_mentions_several_instances(
        self, collector: MetricsCollector
    ) -> None:
        """При нескольких экземплярах это отражается в сводке."""
        await collector.on_metrics(metrics_message("classifier", "c-1", 4, 4, 0))
        await collector.on_metrics(metrics_message("classifier", "c-2", 6, 6, 0))

        assert "экземпляров 2" in collector.summary_line()
